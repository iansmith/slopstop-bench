"""Phase 0 (TDD RED) — behavioral spec for backup_scheduler.py checkpoint_3.

Destinations + incremental backups (BENCH-3): the new --backup CLI flag, the
required `destination` field, the <destination>/<job_id>/<relative_path> layout,
SHA-256-based skipping of unchanged files, and the new DEST_STATE_LOADED /
FILE_SKIPPED_UNCHANGED events plus the files_skipped_unchanged / dest_state_files
fields on JOB_COMPLETED.

Driven end-to-end through the real CLI contract (`python backup_scheduler.py
...`), parsing the JSON Lines emitted on stdout, mirroring the conventions in
tests/test_strategy_phase0.py. These describe the EXPECTED post-implementation
behavior: they FAIL on the current checkpoint_2 code and turn GREEN once
checkpoint_3 lands. Kept in a separate file so test names never collide.

Interpretation notes (recorded because the spec leaves room):
  * `--backup` is the filesystem root for the `backup://` scheme, exactly as
    `--mount` is the root for `mount://`. `destination: backup://store` resolves
    to <backup_root>/store, and a job's backups live at
    <backup_root>/store/<job_id>/<source_relative_path>.
  * The new JOB_COMPLETED fields (files_skipped_unchanged, dest_state_files)
    appear whenever a job DECLARES a `destination` and runs an incremental-capable
    strategy (full). A job with no `destination` keeps the checkpoint_2 shape —
    that backward-compat guarantee is enforced by the existing 19 tests staying
    green, not re-asserted here.
"""

import hashlib
import json
import subprocess
import sys
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


# A small tree with KNOWN byte content so sizes/checksums are fully determined by
# the test (not by whatever a grading fixture happens to contain). Paths sort as
# A/K.html < A/L.md < O.md, the order list_source_files walks them.
SRC = {
    "A/K.html": b"<html></html>",      # 13 bytes
    "A/L.md": b"new markdown body!!",  # 19 bytes
    "O.md": b"# Title\n",              # 8 bytes
}

FULL_SCHEDULE = """
version: 1
timezone: UTC
jobs:
  - id: j
    source: mount://
    destination: backup://store
    exclude: []
    when:
      kind: daily
      at: "03:30"
    strategy:
      kind: full
"""

NOW = "2025-09-10T03:30:00Z"


# --------------------------------------------------------------------------- #
# First run — destination declared, nothing backed up yet
# --------------------------------------------------------------------------- #
def test_first_run_empty_destination_backs_up_all(tmp_path):
    """A --backup root that doesn't exist yet (or is empty) means no prior state:
    every selected file is backed up, no DEST_STATE_LOADED is emitted, and
    JOB_COMPLETED reports files_skipped_unchanged=0 / dest_state_files=0."""
    mount = tmp_path / "files"
    write_files(mount, SRC)
    backup = tmp_path / "backups"  # intentionally does not exist yet

    events = events_for(
        parse_events(
            run_scheduler(tmp_path, FULL_SCHEDULE, NOW, mount, backup=backup, duration=0)
        ),
        "j",
    )
    kinds = [e["event"] for e in events]
    assert "DEST_STATE_LOADED" not in kinds
    assert "FILE_SKIPPED_UNCHANGED" not in kinds

    backed = {e["path"] for e in events if e["event"] == "FILE_BACKED_UP"}
    assert backed == set(SRC)

    completed = [e for e in events if e["event"] == "JOB_COMPLETED"][0]
    assert completed["selected"] == 3
    assert completed["files_skipped_unchanged"] == 0
    assert completed["dest_state_files"] == 0
    assert completed["total_size"] == sum(len(c) for c in SRC.values())


# --------------------------------------------------------------------------- #
# Subsequent run — skip unchanged, back up changed
# --------------------------------------------------------------------------- #
def test_subsequent_run_skips_unchanged_backs_up_changed(tmp_path):
    """With existing backups under <backup>/store/<job_id>/, unchanged files
    (matching SHA-256) are skipped and changed files are re-backed-up.
    files_total / dest_state_files count all existing backup files; total_size
    counts only the files actually backed up this run."""
    mount = tmp_path / "files"
    write_files(mount, SRC)
    backup = tmp_path / "backups"
    # Existing backups: K.html and O.md match the source byte-for-byte; L.md
    # differs (the source changed since the last run).
    write_files(
        backup / "store" / "j",
        {
            "A/K.html": SRC["A/K.html"],  # unchanged -> skipped
            "A/L.md": b"old",             # changed   -> backed up
            "O.md": SRC["O.md"],          # unchanged -> skipped
        },
    )

    events = events_for(
        parse_events(
            run_scheduler(tmp_path, FULL_SCHEDULE, NOW, mount, backup=backup, duration=0)
        ),
        "j",
    )

    loaded = [e for e in events if e["event"] == "DEST_STATE_LOADED"]
    assert len(loaded) == 1
    assert loaded[0]["files_total"] == 3

    skipped = {e["path"]: e for e in events if e["event"] == "FILE_SKIPPED_UNCHANGED"}
    assert set(skipped) == {"A/K.html", "O.md"}
    assert skipped["A/K.html"]["hash"] == sha256_tag(SRC["A/K.html"])
    assert skipped["O.md"]["hash"] == sha256_tag(SRC["O.md"])

    backed = {e["path"]: e for e in events if e["event"] == "FILE_BACKED_UP"}
    assert set(backed) == {"A/L.md"}
    assert backed["A/L.md"]["checksum"] == sha256_tag(SRC["A/L.md"])
    assert backed["A/L.md"]["size"] == len(SRC["A/L.md"])

    completed = [e for e in events if e["event"] == "JOB_COMPLETED"][0]
    assert completed["selected"] == 3
    assert completed["files_skipped_unchanged"] == 2
    assert completed["dest_state_files"] == 3
    assert completed["total_size"] == len(SRC["A/L.md"])


# --------------------------------------------------------------------------- #
# Event ordering + per-file adjacency
# --------------------------------------------------------------------------- #
def test_dest_state_loaded_ordering_and_skip_follows_select(tmp_path):
    """DEST_STATE_LOADED is emitted right after JOB_STARTED and before
    STRATEGY_SELECTED; each FILE_SELECTED is immediately followed by its
    resolution (here all unchanged -> FILE_SKIPPED_UNCHANGED for the same path)."""
    mount = tmp_path / "files"
    write_files(mount, SRC)
    backup = tmp_path / "backups"
    write_files(backup / "store" / "j", {p: SRC[p] for p in SRC})  # all unchanged

    events = events_for(
        parse_events(
            run_scheduler(tmp_path, FULL_SCHEDULE, NOW, mount, backup=backup, duration=0)
        ),
        "j",
    )
    seq = [e["event"] for e in events]
    started = seq.index("JOB_STARTED")
    assert seq[started + 1] == "DEST_STATE_LOADED"
    assert seq[started + 2] == "STRATEGY_SELECTED"

    for i, e in enumerate(events):
        if e["event"] == "FILE_SELECTED":
            nxt = events[i + 1]
            assert nxt["event"] == "FILE_SKIPPED_UNCHANGED"
            assert nxt["path"] == e["path"]


# --------------------------------------------------------------------------- #
# Destination layout — the <job_id> segment is mandatory
# --------------------------------------------------------------------------- #
def test_dest_layout_requires_job_id_segment(tmp_path):
    """Existing backups must live under <dest>/<job_id>/<rel>. Files placed
    directly under <dest>/<rel> (missing the job_id segment) are NOT this job's
    backups: nothing is loaded and nothing is skipped."""
    mount = tmp_path / "files"
    write_files(mount, SRC)
    backup = tmp_path / "backups"
    # Wrong layout: missing the "/j/" job-id segment.
    write_files(backup / "store", {p: SRC[p] for p in SRC})

    events = events_for(
        parse_events(
            run_scheduler(tmp_path, FULL_SCHEDULE, NOW, mount, backup=backup, duration=0)
        ),
        "j",
    )
    assert not any(e["event"] == "DEST_STATE_LOADED" for e in events)
    assert not any(e["event"] == "FILE_SKIPPED_UNCHANGED" for e in events)
    backed = {e["path"] for e in events if e["event"] == "FILE_BACKED_UP"}
    assert backed == set(SRC)


# --------------------------------------------------------------------------- #
# Pack strategy does NOT use incremental backups
# --------------------------------------------------------------------------- #
PACK_SRC = {"file1": b"a" * 28, "file2": b"b" * 4}

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
      at: "03:30"
    strategy:
      kind: pack
"""


def test_pack_strategy_ignores_destination_state(tmp_path):
    """`.tar` archives are not tracked in destination state, so pack never loads
    state and never skips — every file is packed even when identical copies
    already sit in the destination."""
    mount = tmp_path / "files"
    write_files(mount, PACK_SRC)
    backup = tmp_path / "backups"
    # Identical copies already present — a `full` job would skip these.
    write_files(backup / "store" / "arc", {p: PACK_SRC[p] for p in PACK_SRC})

    events = events_for(
        parse_events(
            run_scheduler(tmp_path, PACK_SCHEDULE, NOW, mount, backup=backup, duration=0)
        ),
        "arc",
    )
    assert not any(e["event"] == "DEST_STATE_LOADED" for e in events)
    assert not any(e["event"] == "FILE_SKIPPED_UNCHANGED" for e in events)
    packed = {e["path"] for e in events if e["event"] == "FILE_PACKED"}
    assert packed == set(PACK_SRC)
    completed = [e for e in events if e["event"] == "JOB_COMPLETED"][0]
    assert completed["packs"] == 1


# --------------------------------------------------------------------------- #
# Destination declared but --backup omitted -> assume no existing backups
# --------------------------------------------------------------------------- #
def test_destination_without_backup_flag_emits_zero_fields(tmp_path):
    """When a job declares a `destination` but no --backup is given, the spec
    says to assume no existing backups exist: all files are backed up, no
    DEST_STATE_LOADED is emitted, and JOB_COMPLETED still carries the new fields
    as zero (the fields are triggered by declaring a destination)."""
    mount = tmp_path / "files"
    write_files(mount, SRC)

    events = events_for(
        parse_events(
            run_scheduler(tmp_path, FULL_SCHEDULE, NOW, mount, duration=0)  # no --backup
        ),
        "j",
    )
    kinds = {e["event"] for e in events}
    assert "DEST_STATE_LOADED" not in kinds
    assert "FILE_SKIPPED_UNCHANGED" not in kinds

    completed = [e for e in events if e["event"] == "JOB_COMPLETED"][0]
    assert completed["files_skipped_unchanged"] == 0
    assert completed["dest_state_files"] == 0
    backed = {e["path"] for e in events if e["event"] == "FILE_BACKED_UP"}
    assert backed == set(SRC)
