"""Phase 0 (TDD RED) — behavioral spec for backup_scheduler.py checkpoint_2.

These tests describe the EXPECTED post-implementation behavior for the
strategy-driven execution added in checkpoint_2 (BENCH-2): full / verify / pack
strategies, the new STRATEGY_SELECTED / FILE_BACKED_UP / FILE_VERIFIED /
FILE_PACKED / PACK_CREATED events, and the deterministic GNU-tar checksums.

They drive the build end-to-end through the real CLI contract
(`python backup_scheduler.py ...`) and parse the JSON Lines emitted on stdout,
mirroring the conventions in tests/test_spec_phase0.py. They are expected to
FAIL on the current checkpoint_1 code and turn GREEN once checkpoint_2 lands.

Kept in a separate file from the checkpoint_1 spec so naming never collides.
"""

import hashlib
import io
import json
import subprocess
import sys
import tarfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "backup_scheduler.py"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def run_scheduler(tmp_path, schedule_text, now, mount, duration=None):
    schedule_path = tmp_path / "schedule.yaml"
    schedule_path.write_text(schedule_text)
    cmd = [
        sys.executable,
        str(SCRIPT),
        "--schedule",
        str(schedule_path),
        "--now",
        now,
        "--mount",
        str(mount),
    ]
    if duration is not None:
        cmd += ["--duration", str(duration)]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=str(REPO_ROOT))


def parse_events(proc):
    assert proc.returncode == 0, (
        f"scheduler exited {proc.returncode}\n--- stderr ---\n{proc.stderr}"
    )
    events = []
    for line in proc.stdout.splitlines():
        if line.strip() == "":
            continue
        events.append(json.loads(line))
    return events


def write_files(root, mapping):
    """Create files with exact byte content. `mapping` is {rel_path: bytes}."""
    for rel, content in mapping.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(content)


def events_for(events, job_id):
    return [e for e in events if e.get("job_id") == job_id]


def sha256_tag(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def gnu_tar_bytes(entries):
    """Reconstruct a pack's tar bytes exactly as the spec mandates.

    `entries` is an ordered list of (arcname, content_bytes). The spec fixes:
      format GNU, mtime=0, mode=0o644, uid=0, gid=0, uname="", gname="".
    arcname is the file's source-relative path (the same value emitted in the
    FILE_PACKED `path` field).
    """
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.GNU_FORMAT) as tf:
        for arcname, content in entries:
            info = tarfile.TarInfo(name=arcname)
            info.size = len(content)
            info.mtime = 0
            info.mode = 0o644
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            tf.addfile(info, io.BytesIO(content))
    return buf.getvalue()


# A small Example-1-shaped tree with KNOWN byte content so checksums/sizes are
# fully determined by the test (not by whatever the grading fixtures contain).
FULL_TREE = {
    "keep1.txt": b"hello",          # 5 bytes, selected
    "keep2.md": b"world!!",         # 7 bytes, selected
    "sub/keep3.txt": b"abc",        # 3 bytes, selected
    "skip.bin": b"zzzzz",           # excluded by **/*.bin
}
FULL_SELECTED = {
    "keep1.txt": b"hello",
    "keep2.md": b"world!!",
    "sub/keep3.txt": b"abc",
}

# Example-2 pack tree: exact sizes from the ticket, with known content.
PACK_CONTENT = {
    "file1": b"a" * 28,
    "file2": b"b" * 4,
    "file3": b"c" * 31,
    "file4": b"d" * 33,
    "file5": b"e" * 10,
}


# --------------------------------------------------------------------------- #
# Backwards compatibility
# --------------------------------------------------------------------------- #
def test_strategy_omitted_emits_no_strategy_events(tmp_path):
    """No `strategy` field -> checkpoint_1 behavior: no STRATEGY_SELECTED and
    no backup operations of any kind."""
    mount = tmp_path / "files"
    write_files(mount, {"a.txt": b"hello", "b.txt": b"world"})
    schedule = """
version: 1
timezone: UTC
jobs:
  - id: legacy
    source: mount://
    exclude: []
    when:
      kind: daily
      at: "03:30"
"""
    events = parse_events(
        run_scheduler(tmp_path, schedule, "2025-09-10T03:30:00Z", mount, duration=0)
    )
    kinds = {e["event"] for e in events}
    assert "STRATEGY_SELECTED" not in kinds
    assert "FILE_BACKED_UP" not in kinds
    assert "FILE_VERIFIED" not in kinds
    assert "FILE_PACKED" not in kinds
    assert "PACK_CREATED" not in kinds
    # FILE_SELECTED still happens (checkpoint_1 selection is unchanged)
    assert "FILE_SELECTED" in kinds


# --------------------------------------------------------------------------- #
# STRATEGY_SELECTED ordering
# --------------------------------------------------------------------------- #
def test_strategy_selected_immediately_after_job_started(tmp_path):
    """STRATEGY_SELECTED is emitted right after JOB_STARTED and before the first
    FILE_SELECTED, carrying the strategy kind."""
    mount = tmp_path / "files"
    write_files(mount, {"a.txt": b"x"})
    schedule = """
version: 1
timezone: UTC
jobs:
  - id: j
    source: mount://
    exclude: []
    when:
      kind: daily
      at: "03:30"
    strategy:
      kind: full
"""
    events = events_for(
        parse_events(
            run_scheduler(tmp_path, schedule, "2025-09-10T03:30:00Z", mount, duration=0)
        ),
        "j",
    )
    seq = [e["event"] for e in events]
    started = seq.index("JOB_STARTED")
    assert seq[started + 1] == "STRATEGY_SELECTED"
    assert events[started + 1]["kind"] == "full"
    # and it precedes any per-file event
    assert seq.index("STRATEGY_SELECTED") < seq.index("FILE_SELECTED")


# --------------------------------------------------------------------------- #
# full strategy
# --------------------------------------------------------------------------- #
def test_full_strategy_backs_up_each_selected_file(tmp_path):
    """`full` emits one FILE_BACKED_UP per SELECTED file with size + content
    sha256; excluded files are never backed up; JOB_COMPLETED.total_size is the
    sum of selected sizes."""
    mount = tmp_path / "files"
    write_files(mount, FULL_TREE)
    schedule = """
version: 1
timezone: UTC
jobs:
  - id: j
    source: mount://
    exclude: ["**/*.bin"]
    when:
      kind: daily
      at: "03:30"
    strategy:
      kind: full
"""
    events = events_for(
        parse_events(
            run_scheduler(tmp_path, schedule, "2025-09-10T03:30:00Z", mount, duration=0)
        ),
        "j",
    )
    backed = {e["path"]: e for e in events if e["event"] == "FILE_BACKED_UP"}
    assert set(backed) == set(FULL_SELECTED)
    for path, content in FULL_SELECTED.items():
        assert backed[path]["size"] == len(content)
        assert backed[path]["checksum"] == sha256_tag(content)
    # excluded file never backed up; no verify/pack leakage
    assert "skip.bin" not in backed
    assert not any(e["event"] in ("FILE_VERIFIED", "FILE_PACKED") for e in events)

    completed = [e for e in events if e["event"] == "JOB_COMPLETED"][0]
    assert completed["total_size"] == sum(len(c) for c in FULL_SELECTED.values())
    assert "packs" not in completed  # packs is a pack-strategy-only field


# --------------------------------------------------------------------------- #
# verify strategy
# --------------------------------------------------------------------------- #
def test_verify_strategy_emits_file_verified_not_backed_up(tmp_path):
    """`verify` mirrors `full` field-for-field but emits FILE_VERIFIED and
    never FILE_BACKED_UP."""
    mount = tmp_path / "files"
    write_files(mount, FULL_TREE)
    schedule = """
version: 1
timezone: UTC
jobs:
  - id: j
    source: mount://
    exclude: ["**/*.bin"]
    when:
      kind: daily
      at: "03:30"
    strategy:
      kind: verify
"""
    events = events_for(
        parse_events(
            run_scheduler(tmp_path, schedule, "2025-09-10T03:30:00Z", mount, duration=0)
        ),
        "j",
    )
    strat = [e for e in events if e["event"] == "STRATEGY_SELECTED"][0]
    assert strat["kind"] == "verify"
    verified = {e["path"]: e for e in events if e["event"] == "FILE_VERIFIED"}
    assert set(verified) == set(FULL_SELECTED)
    for path, content in FULL_SELECTED.items():
        assert verified[path]["size"] == len(content)
        assert verified[path]["checksum"] == sha256_tag(content)
    assert not any(e["event"] == "FILE_BACKED_UP" for e in events)


# --------------------------------------------------------------------------- #
# pack strategy — packing boundaries
# --------------------------------------------------------------------------- #
def test_pack_strategy_packing_boundaries(tmp_path):
    """The Example-2 packing logic: overflow finalizes the current pack before
    the incoming file; a file that lands strictly over the limit finalizes its
    pack eagerly; a pack exactly at the limit waits for the next file."""
    mount = tmp_path / "files"
    write_files(mount, PACK_CONTENT)
    schedule = """
version: 1
timezone: UTC
jobs:
  - id: arc
    source: mount://
    exclude: []
    when:
      kind: daily
      at: "03:30"
    strategy:
      kind: pack
      options:
        max_pack_bytes: 32
"""
    events = events_for(
        parse_events(
            run_scheduler(tmp_path, schedule, "2025-09-10T03:30:00Z", mount, duration=0)
        ),
        "arc",
    )

    # The exact interleaving of selection / packing / finalization events.
    rel = [
        (e["event"], e.get("path"), e.get("pack_id"), e.get("name"), e.get("size"))
        for e in events
        if e["event"] in ("FILE_SELECTED", "FILE_PACKED", "PACK_CREATED")
    ]
    assert rel == [
        ("FILE_SELECTED", "file1", None, None, None),
        ("FILE_PACKED", "file1", 1, None, 28),
        ("FILE_SELECTED", "file2", None, None, None),
        ("FILE_PACKED", "file2", 1, None, 4),
        ("FILE_SELECTED", "file3", None, None, None),
        ("PACK_CREATED", None, None, "pack-1.tar", 32),
        ("FILE_PACKED", "file3", 2, None, 31),
        ("FILE_SELECTED", "file4", None, None, None),
        ("PACK_CREATED", None, None, "pack-2.tar", 31),
        ("FILE_PACKED", "file4", 3, None, 33),
        ("PACK_CREATED", None, None, "pack-3.tar", 33),
        ("FILE_SELECTED", "file5", None, None, None),
        ("FILE_PACKED", "file5", 4, None, 10),
        ("PACK_CREATED", None, None, "pack-4.tar", 10),
    ]

    completed = [e for e in events if e["event"] == "JOB_COMPLETED"][0]
    assert completed["packs"] == 4
    assert completed["total_size"] == 106


def test_pack_strategy_default_max_pack_bytes(tmp_path):
    """With no options, max_pack_bytes defaults to 1048576, so a handful of tiny
    files all land in a single pack."""
    mount = tmp_path / "files"
    write_files(mount, {"file1": b"a" * 28, "file2": b"b" * 4, "file3": b"c" * 31})
    schedule = """
version: 1
timezone: UTC
jobs:
  - id: arc
    source: mount://
    exclude: []
    when:
      kind: daily
      at: "03:30"
    strategy:
      kind: pack
"""
    events = events_for(
        parse_events(
            run_scheduler(tmp_path, schedule, "2025-09-10T03:30:00Z", mount, duration=0)
        ),
        "arc",
    )
    packed_ids = {e["pack_id"] for e in events if e["event"] == "FILE_PACKED"}
    assert packed_ids == {1}
    created = [e for e in events if e["event"] == "PACK_CREATED"]
    assert [c["name"] for c in created] == ["pack-1.tar"]
    assert created[0]["size"] == 63
    completed = [e for e in events if e["event"] == "JOB_COMPLETED"][0]
    assert completed["packs"] == 1
    assert completed["total_size"] == 63


def test_pack_created_checksum_is_deterministic_gnu_tar(tmp_path):
    """PACK_CREATED.checksum is sha256 over deterministic GNU-tar bytes (fixed
    metadata), tar_size is the byte length of those tar bytes, and timestamp is
    the run's local now. Reconstructing the tar in-test must reproduce both the
    checksum and tar_size for every pack."""
    mount = tmp_path / "files"
    write_files(mount, PACK_CONTENT)
    schedule = """
version: 1
timezone: UTC
jobs:
  - id: arc
    source: mount://
    exclude: []
    when:
      kind: daily
      at: "03:30"
    strategy:
      kind: pack
      options:
        max_pack_bytes: 32
"""
    events = events_for(
        parse_events(
            run_scheduler(tmp_path, schedule, "2025-09-10T03:30:00Z", mount, duration=0)
        ),
        "arc",
    )
    created = {e["name"]: e for e in events if e["event"] == "PACK_CREATED"}

    expected_members = {
        "pack-1.tar": [("file1", PACK_CONTENT["file1"]), ("file2", PACK_CONTENT["file2"])],
        "pack-2.tar": [("file3", PACK_CONTENT["file3"])],
        "pack-3.tar": [("file4", PACK_CONTENT["file4"])],
        "pack-4.tar": [("file5", PACK_CONTENT["file5"])],
    }
    assert set(created) == set(expected_members)
    for name, entries in expected_members.items():
        tar_bytes = gnu_tar_bytes(entries)
        assert created[name]["checksum"] == sha256_tag(tar_bytes)
        assert created[name]["tar_size"] == len(tar_bytes)
        assert created[name]["timestamp"] == "2025-09-10T03:30:00Z"
