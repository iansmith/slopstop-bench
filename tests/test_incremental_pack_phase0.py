"""Phase 0 (TDD RED) — behavioral spec for backup_scheduler.py checkpoint_4.

Incremental pack backups (BENCH-4): the pack strategy learns to read existing
pack files from the destination and only rewrite the packs that actually changed.
This adds four events — PACK_LOADED, PACK_SKIP_UNCHANGED, PACK_UNCHANGED, and
PACK_UPDATED — layered on top of the checkpoint_2 pack strategy.

Driven end-to-end through the real CLI contract (`python backup_scheduler.py
...`), parsing the JSON Lines on stdout, mirroring the conventions in
tests/test_strategy_phase0.py and tests/test_incremental_phase0.py. These
describe the EXPECTED post-implementation behavior: the genuinely-new ones FAIL
on the current checkpoint_3 code and turn GREEN once checkpoint_4 lands. Kept in
a separate file so test names never collide.

Interpretation notes (recorded because the spec leaves room):
  * The pack destination layout is <backup_root>/<dest>/<job_id>/pack-N.tar,
    mirroring how the `full` strategy uses <...>/<job_id>/<relative_path>. The
    job-id segment is mandatory, exactly as in checkpoint_3.
  * "Unchanged" is byte-identity: a source file is unchanged when its current
    SHA-256 matches the copy carried in the existing pack of the same name.
  * The packing algorithm reruns from scratch (checkpoint_2 boundaries), so the
    example's stable file sizes reproduce the original pack partition. A rebuilt
    pack whose bytes are identical to the loaded pack is PACK_UNCHANGED; one that
    differs is PACK_UPDATED (carrying old_size / old_checksum from the loaded
    pack). A pack with no pre-existing counterpart stays PACK_CREATED.
  * The scheduler is simulation-only (as in checkpoints 1-3): it reads existing
    packs and emits events but never writes the destination. These tests set up
    the destination packs by hand using the same deterministic GNU-tar bytes the
    scheduler produces, so an all-unchanged rerun reproduces identical checksums.
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
def run_scheduler(tmp_path, schedule_text, now, mount, backup=None, duration=None):
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
    if backup is not None:
        cmd += ["--backup", str(backup)]
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
    """Deterministic GNU-tar bytes for `entries` = [(arcname, content), ...].

    The spec fixes the per-entry metadata (format GNU, mtime=0, mode=0o644,
    uid=0, gid=0, uname="", gname=""), so rebuilding a pack from identical
    members reproduces byte-for-byte identical archives — the property that makes
    PACK_UNCHANGED detectable by checksum.
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


def write_pack(dest_job_dir, name, entries):
    """Materialize <dest_job_dir>/<name> as a deterministic pack tar."""
    dest_job_dir.mkdir(parents=True, exist_ok=True)
    (dest_job_dir / name).write_bytes(gnu_tar_bytes(entries))


# The spec's worked example: A (1), B (2), C (2), D (3), E (1) with
# max_pack_bytes=4. Sorted source order is A < B < C < D < E. The checkpoint_2
# packing boundaries put A+B in pack-1 (3), C alone in pack-2 (A+B+C overflows),
# and D+E in pack-3 (4, exactly at the limit). The "changed" variants keep the
# same byte length so the partition is stable across runs.
A_OLD = b"A"     # 1 byte
A_NEW = b"X"     # 1 byte, changed content
B = b"BB"        # 2 bytes
C_OLD = b"CC"    # 2 bytes
C_NEW = b"ZZ"    # 2 bytes, changed content
D = b"DDD"       # 3 bytes
E = b"E"         # 1 byte

SRC_ORIGINAL = {"A": A_OLD, "B": B, "C": C_OLD, "D": D, "E": E}
PACK_1 = [("A", A_OLD), ("B", B)]
PACK_2 = [("C", C_OLD)]
PACK_3 = [("D", D), ("E", E)]

PACK_SCHEDULE = """
version: 1
timezone: UTC
jobs:
  - id: arc
    source: mount://
    destination: backup://store
    exclude: []
    when:
      kind: daily
      at: "02:00"
    strategy:
      kind: pack
      options:
        max_pack_bytes: 4
"""

NOW = "2025-09-10T02:00:00Z"


def seed_destination(backup):
    """Write the three original packs under <backup>/store/arc/."""
    dest = backup / "store" / "arc"
    write_pack(dest, "pack-1.tar", PACK_1)
    write_pack(dest, "pack-2.tar", PACK_2)
    write_pack(dest, "pack-3.tar", PACK_3)
    return dest


# --------------------------------------------------------------------------- #
# First run — empty destination behaves exactly like the checkpoint_2 pack run
# (backward-compat guard: no incremental events, every file packed fresh).
# --------------------------------------------------------------------------- #
def test_first_run_empty_destination_packs_like_checkpoint2(tmp_path):
    mount = tmp_path / "files"
    write_files(mount, SRC_ORIGINAL)
    backup = tmp_path / "backups"  # intentionally does not exist yet

    events = events_for(
        parse_events(
            run_scheduler(tmp_path, PACK_SCHEDULE, NOW, mount, backup=backup, duration=0)
        ),
        "arc",
    )
    kinds = {e["event"] for e in events}
    assert "PACK_LOADED" not in kinds
    assert "PACK_UNCHANGED" not in kinds
    assert "PACK_UPDATED" not in kinds
    assert "PACK_SKIP_UNCHANGED" not in kinds

    created = [e for e in events if e["event"] == "PACK_CREATED"]
    assert [c["name"] for c in created] == ["pack-1.tar", "pack-2.tar", "pack-3.tar"]
    assert [c["size"] for c in created] == [3, 2, 4]

    packed = {e["path"]: e["pack_id"] for e in events if e["event"] == "FILE_PACKED"}
    assert packed == {"A": 1, "B": 1, "C": 2, "D": 3, "E": 3}

    completed = [e for e in events if e["event"] == "JOB_COMPLETED"][0]
    assert completed["packs"] == 3
    assert completed["total_size"] == 9


# --------------------------------------------------------------------------- #
# Subsequent run — all files unchanged: every pack loads, every file is skipped,
# every pack reports PACK_UNCHANGED, nothing is created or updated.
# --------------------------------------------------------------------------- #
def test_subsequent_run_all_unchanged_loads_skips_and_marks_unchanged(tmp_path):
    mount = tmp_path / "files"
    write_files(mount, SRC_ORIGINAL)
    backup = tmp_path / "backups"
    seed_destination(backup)

    events = events_for(
        parse_events(
            run_scheduler(tmp_path, PACK_SCHEDULE, NOW, mount, backup=backup, duration=0)
        ),
        "arc",
    )

    loaded = {e["name"]: e for e in events if e["event"] == "PACK_LOADED"}
    assert set(loaded) == {"pack-1.tar", "pack-2.tar", "pack-3.tar"}
    assert loaded["pack-1.tar"]["files_total"] == 2
    assert loaded["pack-2.tar"]["files_total"] == 1
    assert loaded["pack-3.tar"]["files_total"] == 2
    assert loaded["pack-1.tar"]["checksum"] == sha256_tag(gnu_tar_bytes(PACK_1))
    assert loaded["pack-2.tar"]["checksum"] == sha256_tag(gnu_tar_bytes(PACK_2))
    assert loaded["pack-3.tar"]["checksum"] == sha256_tag(gnu_tar_bytes(PACK_3))

    # Nothing re-packed; every file skipped as unchanged, carrying its hash.
    assert not any(e["event"] == "FILE_PACKED" for e in events)
    skipped = {e["path"]: e for e in events if e["event"] == "PACK_SKIP_UNCHANGED"}
    assert set(skipped) == {"A", "B", "C", "D", "E"}
    assert skipped["A"]["pack_id"] == 1
    assert skipped["A"]["size"] == 1
    assert skipped["A"]["hash"] == sha256_tag(A_OLD)
    assert skipped["C"]["pack_id"] == 2
    assert skipped["C"]["hash"] == sha256_tag(C_OLD)
    assert skipped["E"]["pack_id"] == 3
    assert skipped["E"]["size"] == 1

    # Every pack unchanged; none created or updated.
    assert not any(e["event"] in ("PACK_CREATED", "PACK_UPDATED") for e in events)
    unchanged = {e["name"]: e for e in events if e["event"] == "PACK_UNCHANGED"}
    assert set(unchanged) == {"pack-1.tar", "pack-2.tar", "pack-3.tar"}
    assert unchanged["pack-1.tar"]["checksum"] == sha256_tag(gnu_tar_bytes(PACK_1))
    assert unchanged["pack-3.tar"]["checksum"] == sha256_tag(gnu_tar_bytes(PACK_3))

    completed = [e for e in events if e["event"] == "JOB_COMPLETED"][0]
    assert completed["packs"] == 3


# --------------------------------------------------------------------------- #
# Subsequent run — A and C changed: their packs are PACK_UPDATED (with old_size /
# old_checksum), the untouched pack stays PACK_UNCHANGED, changed files re-pack
# while their unchanged pack-mates skip.
# --------------------------------------------------------------------------- #
def test_subsequent_run_some_changed_updates_affected_packs(tmp_path):
    mount = tmp_path / "files"
    write_files(mount, {"A": A_NEW, "B": B, "C": C_NEW, "D": D, "E": E})
    backup = tmp_path / "backups"
    seed_destination(backup)

    events = events_for(
        parse_events(
            run_scheduler(tmp_path, PACK_SCHEDULE, NOW, mount, backup=backup, duration=0)
        ),
        "arc",
    )

    loaded = {e["name"] for e in events if e["event"] == "PACK_LOADED"}
    assert loaded == {"pack-1.tar", "pack-2.tar", "pack-3.tar"}

    # Changed files re-pack; unchanged files skip.
    packed = {e["path"]: e for e in events if e["event"] == "FILE_PACKED"}
    assert set(packed) == {"A", "C"}
    assert packed["A"]["pack_id"] == 1
    assert packed["A"]["size"] == 1
    assert packed["C"]["pack_id"] == 2
    assert packed["C"]["size"] == 2

    skipped = {e["path"]: e for e in events if e["event"] == "PACK_SKIP_UNCHANGED"}
    assert set(skipped) == {"B", "D", "E"}
    assert skipped["B"]["pack_id"] == 1
    assert skipped["B"]["hash"] == sha256_tag(B)
    assert skipped["D"]["pack_id"] == 3
    assert skipped["E"]["pack_id"] == 3

    # pack-1 / pack-2 changed -> PACK_UPDATED; pack-3 untouched -> PACK_UNCHANGED.
    assert not any(e["event"] == "PACK_CREATED" for e in events)
    updated = {e["name"]: e for e in events if e["event"] == "PACK_UPDATED"}
    assert set(updated) == {"pack-1.tar", "pack-2.tar"}

    new_p1 = gnu_tar_bytes([("A", A_NEW), ("B", B)])
    assert updated["pack-1.tar"]["size"] == 3
    assert updated["pack-1.tar"]["old_size"] == 3
    assert updated["pack-1.tar"]["checksum"] == sha256_tag(new_p1)
    assert updated["pack-1.tar"]["old_checksum"] == sha256_tag(gnu_tar_bytes(PACK_1))
    assert updated["pack-1.tar"]["tar_size"] == len(new_p1)
    assert updated["pack-1.tar"]["timestamp"] == "2025-09-10T02:00:00Z"

    new_p2 = gnu_tar_bytes([("C", C_NEW)])
    assert updated["pack-2.tar"]["size"] == 2
    assert updated["pack-2.tar"]["old_size"] == 2
    assert updated["pack-2.tar"]["checksum"] == sha256_tag(new_p2)
    assert updated["pack-2.tar"]["old_checksum"] == sha256_tag(gnu_tar_bytes(PACK_2))

    unchanged = {e["name"]: e for e in events if e["event"] == "PACK_UNCHANGED"}
    assert set(unchanged) == {"pack-3.tar"}
    assert unchanged["pack-3.tar"]["checksum"] == sha256_tag(gnu_tar_bytes(PACK_3))

    completed = [e for e in events if e["event"] == "JOB_COMPLETED"][0]
    assert completed["packs"] == 3


# --------------------------------------------------------------------------- #
# Event ordering — every existing pack is announced (in pack-number order) right
# after STRATEGY_SELECTED and before any file work begins.
# --------------------------------------------------------------------------- #
def test_pack_loaded_ordering_after_strategy_selected(tmp_path):
    mount = tmp_path / "files"
    write_files(mount, SRC_ORIGINAL)
    backup = tmp_path / "backups"
    seed_destination(backup)

    events = events_for(
        parse_events(
            run_scheduler(tmp_path, PACK_SCHEDULE, NOW, mount, backup=backup, duration=0)
        ),
        "arc",
    )
    seq = [e["event"] for e in events]
    started = seq.index("JOB_STARTED")
    assert seq[started + 1] == "STRATEGY_SELECTED"
    assert seq[started + 2 : started + 5] == ["PACK_LOADED", "PACK_LOADED", "PACK_LOADED"]

    loaded_names = [e["name"] for e in events if e["event"] == "PACK_LOADED"]
    assert loaded_names == ["pack-1.tar", "pack-2.tar", "pack-3.tar"]
    assert seq.index("PACK_LOADED") < seq.index("FILE_SELECTED")
