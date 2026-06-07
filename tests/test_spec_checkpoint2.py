"""Phase 0 red tests for BENCH-14 (file_backup checkpoint_2 — Core Tests).

These tests describe the expected post-implementation behavior of the
strategy-driven execution added in checkpoint 2. They fail on the current
(checkpoint-1) implementation, which ignores the ``strategy`` field.

The tar checksums asserted for the ``pack`` strategy are NOT hard-coded magic
numbers: they are re-derived in-test from the spec's documented tar rules
(``tarfile.GNU_FORMAT`` + normalized entry metadata: mtime=0, mode=0o644,
uid=0, gid=0, uname="", gname="", arcname = the file's job-relative path). A
correct implementation that follows those rules produces identical bytes, so
``canonical_tar_bytes`` is an executable encoding of the spec — not a copy of
any reference output.

Priority order within this file: edge/boundary cases, error/rejection cases,
cross-feature interaction cases, then happy-path.
"""

import hashlib
import io
import json
import subprocess
import sys
import tarfile
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "backup_scheduler.py"

NOW = "2025-09-10T03:30:00Z"  # makes a daily job at 03:30 UTC due with duration 0


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def write_tree(root: Path, files: dict):
    """Create files with explicit byte content. ``files`` maps rel POSIX path -> bytes."""
    root.mkdir(parents=True, exist_ok=True)
    for rel, content in files.items():
        p = root / Path(rel)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(content)


def run_scheduler(tmp_path, schedule_yaml, now=NOW, *, duration=0, mount=None):
    sched = tmp_path / "schedule.yaml"
    sched.write_text(textwrap.dedent(schedule_yaml))
    if mount is None:
        mount = tmp_path / "mount"
        mount.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable, str(SCRIPT),
        "--schedule", str(sched),
        "--now", now,
        "--mount", str(mount),
        "--duration", str(duration),
    ]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=str(REPO_ROOT))


def events_of(proc):
    assert proc.returncode == 0, (
        f"non-zero exit {proc.returncode}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    )
    out = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if line:
            out.append(json.loads(line))
    return out


def named(events, name):
    return [e for e in events if e["event"] == name]


def sha256_prefixed(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


def canonical_tar_bytes(entries) -> bytes:
    """Build the canonical pack tar per the spec.

    ``entries`` is an ordered list of ``(arcname, content_bytes)`` in pack order.
    The archive uses GNU format with fully normalized entry metadata so the
    resulting bytes (and therefore the SHA-256) are deterministic.
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


def strategy_schedule(kind, *, exclude="[]", options=None, job_id="job", source="mount://"):
    """Render a single-job schedule whose job carries a ``strategy`` block."""
    lines = [
        "version: 1",
        "timezone: UTC",
        "jobs:",
        f"  - id: {job_id}",
        f"    source: {source}",
        '    when: {kind: daily, at: "03:30"}',
        f"    exclude: {exclude}",
        "    strategy:",
        f"      kind: {kind}",
    ]
    if options is not None:
        lines.append("      options:")
        for key, value in options.items():
            lines.append(f"        {key}: {value}")
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------- #
# 1. Edge / boundary cases
# --------------------------------------------------------------------------- #
def test_full_empty_file_has_zero_size_and_empty_checksum(tmp_path):
    """Boundary: an empty file backs up with size 0 and the SHA-256 of no bytes."""
    write_tree(tmp_path / "mount", {"empty.txt": b""})
    events = events_of(run_scheduler(tmp_path, strategy_schedule("full")))
    backed = named(events, "FILE_BACKED_UP")
    assert len(backed) == 1
    assert backed[0]["path"] == "empty.txt"
    assert backed[0]["size"] == 0
    assert backed[0]["checksum"] == sha256_prefixed(b"")


def test_pack_exact_fit_does_not_finalize_early(tmp_path):
    """Boundary: two files summing exactly to the limit share one pack."""
    contents = {"a": b"\x01" * 20, "b": b"\x02" * 12}  # 20 + 12 == 32 == limit
    write_tree(tmp_path / "mount", contents)
    events = events_of(
        run_scheduler(tmp_path, strategy_schedule("pack", options={"max_pack_bytes": 32}))
    )
    packed = [(e["path"], e["pack_id"]) for e in named(events, "FILE_PACKED")]
    assert packed == [("a", 1), ("b", 1)]
    created = named(events, "PACK_CREATED")
    assert len(created) == 1
    assert created[0]["size"] == 32


def test_pack_single_file_over_limit_is_packed_alone(tmp_path):
    """Boundary: a file larger than the limit is still packed (in its own pack)."""
    contents = {"big": b"\x01" * 100}  # 100 > limit 32
    write_tree(tmp_path / "mount", contents)
    events = events_of(
        run_scheduler(tmp_path, strategy_schedule("pack", options={"max_pack_bytes": 32}))
    )
    packed = [(e["path"], e["pack_id"]) for e in named(events, "FILE_PACKED")]
    assert packed == [("big", 1)]
    created = named(events, "PACK_CREATED")
    assert len(created) == 1
    assert created[0]["name"] == "pack-1.tar"
    assert created[0]["size"] == 100
    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["packs"] == 1
    assert jc["total_size"] == 100


def test_pack_default_max_pack_bytes_groups_small_files(tmp_path):
    """Boundary: with ``options`` omitted, the default 1 MiB limit packs small files together."""
    contents = {"a": b"x" * 10, "b": b"y" * 10, "c": b"z" * 10}
    write_tree(tmp_path / "mount", contents)
    events = events_of(run_scheduler(tmp_path, strategy_schedule("pack")))  # no options
    packed = [(e["path"], e["pack_id"]) for e in named(events, "FILE_PACKED")]
    assert packed == [("a", 1), ("b", 1), ("c", 1)]
    created = named(events, "PACK_CREATED")
    assert len(created) == 1
    assert created[0]["size"] == 30
    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["packs"] == 1
    assert jc["total_size"] == 30


def test_pack_with_no_selected_files_creates_no_pack(tmp_path):
    """Edge: a pack job whose files are all excluded creates zero packs."""
    contents = {"skip1.bin": b"a", "skip2.bin": b"bb"}
    write_tree(tmp_path / "mount", contents)
    events = events_of(
        run_scheduler(
            tmp_path,
            strategy_schedule("pack", exclude='["**/*.bin"]', options={"max_pack_bytes": 32}),
        )
    )
    assert named(events, "PACK_CREATED") == []
    assert named(events, "FILE_PACKED") == []
    # STRATEGY_SELECTED is keyed on strategy presence, not on having any files.
    assert named(events, "STRATEGY_SELECTED")[0]["kind"] == "pack"
    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["packs"] == 0
    assert jc["total_size"] == 0


# --------------------------------------------------------------------------- #
# 2. Error / rejection-adjacent cases
#    (The spec defines no hard error conditions for strategies; the closest
#     "rejection" semantics are that excluded files are not processed at all.)
# --------------------------------------------------------------------------- #
def test_full_does_not_back_up_excluded_files(tmp_path):
    write_tree(tmp_path / "mount", {"keep.txt": b"k", "skip.bin": b"xxxx"})
    events = events_of(
        run_scheduler(tmp_path, strategy_schedule("full", exclude='["**/*.bin"]'))
    )
    backed = {e["path"] for e in named(events, "FILE_BACKED_UP")}
    assert backed == {"keep.txt"}
    assert any(e["path"] == "skip.bin" for e in named(events, "FILE_EXCLUDED"))
    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["total_size"] == 1  # only keep.txt counts


def test_pack_excludes_files_from_packing(tmp_path):
    """Excluded files never enter a pack; pack boundaries span selected files only."""
    contents = {"keep1": b"\x01" * 20, "skip.bin": b"\x02" * 20, "keep2": b"\x03" * 20}
    write_tree(tmp_path / "mount", contents)
    events = events_of(
        run_scheduler(
            tmp_path,
            strategy_schedule("pack", exclude='["*.bin"]', options={"max_pack_bytes": 32}),
        )
    )
    packed = [(e["path"], e["pack_id"]) for e in named(events, "FILE_PACKED")]
    # selected order is lexicographic: keep1, keep2 (skip.bin excluded).
    # keep1(20) fills pack 1; keep2(20) would overflow -> pack 2.
    assert packed == [("keep1", 1), ("keep2", 2)]
    assert [c["name"] for c in named(events, "PACK_CREATED")] == ["pack-1.tar", "pack-2.tar"]
    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["selected"] == 2
    assert jc["excluded"] == 1
    assert jc["packs"] == 2
    assert jc["total_size"] == 40


# --------------------------------------------------------------------------- #
# 3. Cross-feature interaction cases
# --------------------------------------------------------------------------- #
def test_no_strategy_matches_checkpoint1_behavior(tmp_path):
    """Regression guard: a job without a strategy behaves exactly like checkpoint 1.

    No STRATEGY_SELECTED, no backup/verify/pack events, and JOB_COMPLETED carries
    neither ``packs`` nor ``total_size``.
    """
    write_tree(tmp_path / "mount", {"a.txt": b"hello", "b.bin": b"data"})
    schedule = """
    version: 1
    timezone: UTC
    jobs:
      - id: job
        source: mount://
        when: {kind: daily, at: "03:30"}
        exclude: ["**/*.bin"]
    """
    events = events_of(run_scheduler(tmp_path, schedule))
    assert named(events, "STRATEGY_SELECTED") == []
    assert named(events, "FILE_BACKED_UP") == []
    assert named(events, "FILE_VERIFIED") == []
    assert named(events, "FILE_PACKED") == []
    assert named(events, "PACK_CREATED") == []
    jc = named(events, "JOB_COMPLETED")[0]
    assert "packs" not in jc
    assert "total_size" not in jc
    assert jc["selected"] == 1
    assert jc["excluded"] == 1


def test_strategy_selected_only_for_jobs_with_strategy(tmp_path):
    """With mixed jobs, only the strategy-bearing job emits STRATEGY_SELECTED/backup events."""
    write_tree(tmp_path / "mount", {"a.txt": b"x"})
    schedule = """
    version: 1
    timezone: UTC
    jobs:
      - id: plain
        source: mount://
        when: {kind: daily, at: "03:30"}
        exclude: []
      - id: strat
        source: mount://
        when: {kind: daily, at: "03:30"}
        exclude: []
        strategy:
          kind: full
    """
    events = events_of(run_scheduler(tmp_path, schedule))
    assert [e["job_id"] for e in named(events, "STRATEGY_SELECTED")] == ["strat"]
    backed = named(events, "FILE_BACKED_UP")
    assert [e["job_id"] for e in backed] == ["strat"]


def test_strategy_selected_emitted_after_job_started_before_files(tmp_path):
    """STRATEGY_SELECTED sits between JOB_STARTED and the first per-file event."""
    write_tree(tmp_path / "mount", {"a.txt": b"hi"})
    events = events_of(run_scheduler(tmp_path, strategy_schedule("full")))
    order = [e["event"] for e in events]
    assert "STRATEGY_SELECTED" in order
    i_started = order.index("JOB_STARTED")
    i_strategy = order.index("STRATEGY_SELECTED")
    i_first_file = order.index("FILE_SELECTED")
    assert i_started < i_strategy < i_first_file
    ss = named(events, "STRATEGY_SELECTED")[0]
    assert ss["job_id"] == "job"
    assert ss["kind"] == "full"


def test_pack_finalizes_before_packing_overflowing_file(tmp_path):
    """The pivotal ordering rule: the current pack is finalized BEFORE the file that overflows it."""
    contents = {"file1": b"\x01" * 28, "file2": b"\x02" * 4, "file3": b"\x03" * 31}
    write_tree(tmp_path / "mount", contents)
    events = events_of(
        run_scheduler(tmp_path, strategy_schedule("pack", options={"max_pack_bytes": 32}))
    )
    seq = [
        (e["event"], e.get("path"), e.get("name"))
        for e in events
        if e["event"] in ("FILE_SELECTED", "FILE_PACKED", "PACK_CREATED")
    ]
    assert seq == [
        ("FILE_SELECTED", "file1", None),
        ("FILE_PACKED", "file1", None),
        ("FILE_SELECTED", "file2", None),
        ("FILE_PACKED", "file2", None),
        ("FILE_SELECTED", "file3", None),
        ("PACK_CREATED", None, "pack-1.tar"),  # finalize file1+file2 BEFORE packing file3
        ("FILE_PACKED", "file3", None),
        ("PACK_CREATED", None, "pack-2.tar"),  # trailing pack flushed at end of job
    ]


def test_strategy_events_are_compact_json(tmp_path):
    """The new event lines obey the compact-JSON rule (no spaces after separators)."""
    contents = {"a": b"\x01" * 10, "b": b"\x02" * 10}
    write_tree(tmp_path / "mount", contents)
    proc = run_scheduler(tmp_path, strategy_schedule("pack", options={"max_pack_bytes": 32}))
    assert proc.returncode == 0, proc.stderr
    saw_pack_event = False
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        assert ", " not in line, f"space after comma: {line!r}"
        assert '": ' not in line, f"space after colon: {line!r}"
        obj = json.loads(line)
        if obj["event"] in ("STRATEGY_SELECTED", "FILE_PACKED", "PACK_CREATED"):
            saw_pack_event = True
    assert saw_pack_event, "expected at least one new checkpoint-2 event in the stream"


# --------------------------------------------------------------------------- #
# 4. Happy-path / structural cases
# --------------------------------------------------------------------------- #
def test_full_backs_up_each_selected_file_with_size_and_checksum(tmp_path):
    files = {"empty.txt": b"", "hello.txt": b"hello", "data.md": b"abcdef"}
    write_tree(tmp_path / "mount", files)
    events = events_of(run_scheduler(tmp_path, strategy_schedule("full")))
    by_path = {e["path"]: e for e in named(events, "FILE_BACKED_UP")}
    assert set(by_path) == set(files)
    for rel, content in files.items():
        assert by_path[rel]["size"] == len(content)
        assert by_path[rel]["checksum"] == sha256_prefixed(content)
    assert named(events, "FILE_VERIFIED") == []
    assert named(events, "FILE_PACKED") == []
    ss = named(events, "STRATEGY_SELECTED")[0]
    assert ss["kind"] == "full"


def test_full_backed_up_immediately_follows_each_selected(tmp_path):
    write_tree(tmp_path / "mount", {"a.txt": b"A", "b.txt": b"BB"})
    events = events_of(run_scheduler(tmp_path, strategy_schedule("full")))
    seq = [
        (e["event"], e["path"])
        for e in events
        if e["event"] in ("FILE_SELECTED", "FILE_BACKED_UP")
    ]
    assert seq == [
        ("FILE_SELECTED", "a.txt"),
        ("FILE_BACKED_UP", "a.txt"),
        ("FILE_SELECTED", "b.txt"),
        ("FILE_BACKED_UP", "b.txt"),
    ]


def test_full_job_completed_reports_total_size_and_no_packs(tmp_path):
    write_tree(tmp_path / "mount", {"a.txt": b"abc", "b.txt": b"de"})  # 3 + 2 == 5
    events = events_of(run_scheduler(tmp_path, strategy_schedule("full")))
    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["total_size"] == 5
    assert "packs" not in jc
    assert jc["selected"] == 2
    assert jc["excluded"] == 0


def test_verify_emits_file_verified_with_size_and_checksum(tmp_path):
    files = {"empty.txt": b"", "x.txt": b"hello"}
    write_tree(tmp_path / "mount", files)
    events = events_of(run_scheduler(tmp_path, strategy_schedule("verify")))
    by_path = {e["path"]: e for e in named(events, "FILE_VERIFIED")}
    assert set(by_path) == set(files)
    for rel, content in files.items():
        assert by_path[rel]["size"] == len(content)
        assert by_path[rel]["checksum"] == sha256_prefixed(content)
    assert named(events, "FILE_BACKED_UP") == []
    assert named(events, "FILE_PACKED") == []
    assert named(events, "STRATEGY_SELECTED")[0]["kind"] == "verify"


def test_pack_example2_boundaries_ids_and_checksums(tmp_path):
    """Full reproduction of the spec's Example 2 pack-boundary logic and PACK_CREATED fields."""
    order = ["file1", "file2", "file3", "file4", "file5"]
    sizes = {"file1": 28, "file2": 4, "file3": 31, "file4": 33, "file5": 10}
    contents = {name: bytes([i + 1]) * sizes[name] for i, name in enumerate(order)}
    write_tree(tmp_path / "mount", contents)
    events = events_of(
        run_scheduler(tmp_path, strategy_schedule("pack", options={"max_pack_bytes": 32}))
    )

    packed = [(e["path"], e["pack_id"], e["size"]) for e in named(events, "FILE_PACKED")]
    assert packed == [
        ("file1", 1, 28),
        ("file2", 1, 4),
        ("file3", 2, 31),
        ("file4", 3, 33),
        ("file5", 4, 10),
    ]

    created = named(events, "PACK_CREATED")
    assert [c["name"] for c in created] == [
        "pack-1.tar", "pack-2.tar", "pack-3.tar", "pack-4.tar",
    ]
    assert [c["size"] for c in created] == [32, 31, 33, 10]

    expected_members = {
        "pack-1.tar": [("file1", contents["file1"]), ("file2", contents["file2"])],
        "pack-2.tar": [("file3", contents["file3"])],
        "pack-3.tar": [("file4", contents["file4"])],
        "pack-4.tar": [("file5", contents["file5"])],
    }
    by_name = {c["name"]: c for c in created}
    for name, members in expected_members.items():
        tar = canonical_tar_bytes(members)
        assert by_name[name]["checksum"] == sha256_prefixed(tar)
        assert by_name[name]["tar_size"] == len(tar)
        assert by_name[name]["timestamp"] == "2025-09-10T03:30:00Z"

    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["packs"] == 4
    assert jc["total_size"] == 106
