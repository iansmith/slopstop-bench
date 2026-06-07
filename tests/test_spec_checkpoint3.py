"""Phase 0 red tests for BENCH-15 (file_backup checkpoint_3 — Core Tests).

These tests describe the expected post-implementation behavior of the
destination + incremental-backup feature added in checkpoint 3. They fail on the
current (checkpoint-2) implementation, which knows nothing about ``--backup``,
the job ``destination`` field, or skipping unchanged files.

Spec model encoded here:

  * ``--backup <dir>`` is the filesystem root for the ``backup://`` scheme,
    exactly as ``--mount`` is the root for ``mount://``. So a job whose
    ``destination`` is ``backup://store`` stores its files under
    ``<backup>/store/<job_id>/<relative_path_from_source>``.
  * Before processing a per-file (``full``/``verify``) job, the scheduler scans
    that per-job destination directory, hashing every existing file into a
    dest-state map ``{rel_path: sha256}``. If any file is found it emits
    ``DEST_STATE_LOADED`` (after ``JOB_STARTED``) with ``files_total`` = the
    number of existing backup files.
  * For each selected file, if its current SHA-256 equals the stored hash the
    file is unchanged → ``FILE_SKIPPED_UNCHANGED`` (and it is NOT counted in
    ``total_size``); otherwise → ``FILE_BACKED_UP`` as before.
  * ``JOB_COMPLETED`` for per-file strategies gains ``files_skipped_unchanged``
    and ``dest_state_files``. The ``pack`` strategy does NOT use incremental
    state, so it emits neither ``DEST_STATE_LOADED`` nor the new summary fields.

Priority order within this file: edge/boundary cases, error/rejection cases,
cross-feature interaction cases, then happy-path.
"""

import hashlib
import json
import subprocess
import sys
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


def seed_dest(backup_root: Path, job_id: str, files: dict, store: str = "store"):
    """Pre-populate a job's destination tree: ``<backup_root>/<store>/<job_id>/<rel>``.

    This is how a "previous backup run" is represented: raw files on disk under
    the per-job destination directory, exactly where the spec says the scanner
    looks for existing backups.
    """
    write_tree(backup_root / store / job_id, files)


def run_scheduler(tmp_path, schedule_yaml, now=NOW, *, duration=0, mount=None, backup=None):
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
    if backup is not None:
        cmd += ["--backup", str(backup)]
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


def dest_schedule(
    kind="full",
    *,
    exclude="[]",
    options=None,
    job_id="job",
    source="mount://",
    destination="backup://store",
    with_strategy=True,
):
    """Render a single-job schedule carrying a ``destination`` and (optionally) a ``strategy``."""
    lines = [
        "version: 1",
        "timezone: UTC",
        "jobs:",
        f"  - id: {job_id}",
        f"    source: {source}",
        f"    destination: {destination}",
        '    when: {kind: daily, at: "03:30"}',
        f"    exclude: {exclude}",
    ]
    if with_strategy:
        lines.append("    strategy:")
        lines.append(f"      kind: {kind}")
        if options is not None:
            lines.append("      options:")
            for key, value in options.items():
                lines.append(f"        {key}: {value}")
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------- #
# 1. Edge / boundary cases
# --------------------------------------------------------------------------- #
def test_first_run_empty_dest_backs_up_all_with_zero_skip_counts(tmp_path):
    """Boundary: an empty destination yields no DEST_STATE_LOADED and zeroed skip counts."""
    write_tree(tmp_path / "mount", {"a.txt": b"A", "b.txt": b"BB"})
    backup = tmp_path / "backup"  # exists but empty -> no existing backups
    backup.mkdir()
    events = events_of(run_scheduler(tmp_path, dest_schedule("full"), backup=backup))
    assert named(events, "DEST_STATE_LOADED") == []
    backed = {e["path"] for e in named(events, "FILE_BACKED_UP")}
    assert backed == {"a.txt", "b.txt"}
    assert named(events, "FILE_SKIPPED_UNCHANGED") == []
    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["files_skipped_unchanged"] == 0
    assert jc["dest_state_files"] == 0
    assert jc["total_size"] == 3  # 1 + 2, nothing skipped


def test_nonexistent_backup_dir_is_treated_as_no_existing_backups(tmp_path):
    """Boundary: a ``--backup`` path that does not exist must not crash; it means no state."""
    write_tree(tmp_path / "mount", {"a.txt": b"A"})
    backup = tmp_path / "does-not-exist-yet"  # deliberately not created
    proc = run_scheduler(tmp_path, dest_schedule("full"), backup=backup)
    events = events_of(proc)  # asserts clean exit
    assert named(events, "DEST_STATE_LOADED") == []
    assert {e["path"] for e in named(events, "FILE_BACKED_UP")} == {"a.txt"}
    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["files_skipped_unchanged"] == 0
    assert jc["dest_state_files"] == 0


def test_all_files_unchanged_are_skipped_and_total_size_zero(tmp_path):
    """Boundary: when every selected file matches its backup, nothing is backed up."""
    files = {"a.txt": b"hello", "b.txt": b"world"}
    write_tree(tmp_path / "mount", files)
    backup = tmp_path / "backup"
    seed_dest(backup, "job", files)  # identical content -> all unchanged
    events = events_of(run_scheduler(tmp_path, dest_schedule("full"), backup=backup))
    loaded = named(events, "DEST_STATE_LOADED")[0]
    assert loaded["files_total"] == 2
    skipped = {e["path"]: e for e in named(events, "FILE_SKIPPED_UNCHANGED")}
    assert set(skipped) == {"a.txt", "b.txt"}
    assert skipped["a.txt"]["hash"] == sha256_prefixed(b"hello")
    assert skipped["b.txt"]["hash"] == sha256_prefixed(b"world")
    assert named(events, "FILE_BACKED_UP") == []
    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["total_size"] == 0
    assert jc["files_skipped_unchanged"] == 2
    assert jc["dest_state_files"] == 2
    assert jc["selected"] == 2


def test_empty_file_unchanged_is_skipped(tmp_path):
    """Boundary: a zero-byte file whose backup is also empty is skipped (hash of no bytes)."""
    write_tree(tmp_path / "mount", {"empty.txt": b""})
    backup = tmp_path / "backup"
    seed_dest(backup, "job", {"empty.txt": b""})
    events = events_of(run_scheduler(tmp_path, dest_schedule("full"), backup=backup))
    skipped = named(events, "FILE_SKIPPED_UNCHANGED")
    assert len(skipped) == 1
    assert skipped[0]["path"] == "empty.txt"
    assert skipped[0]["hash"] == sha256_prefixed(b"")
    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["total_size"] == 0
    assert jc["files_skipped_unchanged"] == 1
    assert jc["dest_state_files"] == 1


def test_single_byte_change_triggers_backup_not_skip(tmp_path):
    """Boundary: one differing byte makes the hash differ, so the file is backed up, not skipped."""
    write_tree(tmp_path / "mount", {"f.txt": b"abc"})
    backup = tmp_path / "backup"
    seed_dest(backup, "job", {"f.txt": b"abd"})  # last byte differs
    events = events_of(run_scheduler(tmp_path, dest_schedule("full"), backup=backup))
    assert named(events, "FILE_SKIPPED_UNCHANGED") == []
    backed = named(events, "FILE_BACKED_UP")
    assert len(backed) == 1 and backed[0]["path"] == "f.txt"
    assert backed[0]["checksum"] == sha256_prefixed(b"abc")
    assert named(events, "DEST_STATE_LOADED")[0]["files_total"] == 1
    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["total_size"] == 3
    assert jc["files_skipped_unchanged"] == 0
    assert jc["dest_state_files"] == 1


# --------------------------------------------------------------------------- #
# 2. Error / rejection-adjacent cases
#    (The spec defines no hard error conditions; the closest "rejection"
#     semantics are that excluded files never participate in incremental logic.)
# --------------------------------------------------------------------------- #
def test_excluded_file_is_neither_skipped_nor_backed_up(tmp_path):
    """An excluded source file is filtered before incremental logic runs."""
    write_tree(tmp_path / "mount", {"keep.txt": b"k", "skip.bin": b"xxxx"})
    backup = tmp_path / "backup"
    seed_dest(backup, "job", {"keep.txt": b"k"})  # only keep.txt has a backup
    events = events_of(
        run_scheduler(tmp_path, dest_schedule("full", exclude='["**/*.bin"]'), backup=backup)
    )
    assert {e["path"] for e in named(events, "FILE_SKIPPED_UNCHANGED")} == {"keep.txt"}
    assert named(events, "FILE_BACKED_UP") == []
    assert {e["path"] for e in named(events, "FILE_EXCLUDED")} == {"skip.bin"}
    # skip.bin must not surface as either skipped or backed up.
    assert all(e["path"] != "skip.bin" for e in named(events, "FILE_SKIPPED_UNCHANGED"))
    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["selected"] == 1
    assert jc["excluded"] == 1
    assert jc["files_skipped_unchanged"] == 1
    assert jc["total_size"] == 0


def test_backup_flag_absent_assumes_no_existing_backups(tmp_path):
    """With ``--backup`` omitted, an on-disk matching backup is ignored: the file is backed up."""
    files = {"a.txt": b"same"}
    write_tree(tmp_path / "mount", files)
    backup = tmp_path / "backup"
    seed_dest(backup, "job", files)  # a matching backup exists on disk...
    # ...but we never tell the scheduler about it (no --backup).
    events = events_of(run_scheduler(tmp_path, dest_schedule("full"), backup=None))
    assert named(events, "DEST_STATE_LOADED") == []
    assert {e["path"] for e in named(events, "FILE_BACKED_UP")} == {"a.txt"}
    assert named(events, "FILE_SKIPPED_UNCHANGED") == []
    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["files_skipped_unchanged"] == 0
    assert jc["dest_state_files"] == 0


# --------------------------------------------------------------------------- #
# 3. Cross-feature interaction cases
# --------------------------------------------------------------------------- #
def test_pack_strategy_ignores_incremental_state(tmp_path):
    """Carve-out: ``pack`` never loads dest-state, never skips, and gains no skip fields."""
    contents = {"a": b"\x01" * 20, "b": b"\x02" * 12}  # 20 + 12 == 32 == limit
    write_tree(tmp_path / "mount", contents)
    backup = tmp_path / "backup"
    seed_dest(backup, "job", contents)  # identical -> WOULD match if incremental applied
    events = events_of(
        run_scheduler(
            tmp_path,
            dest_schedule("pack", options={"max_pack_bytes": 32}),
            backup=backup,
        )
    )
    assert named(events, "DEST_STATE_LOADED") == []
    assert named(events, "FILE_SKIPPED_UNCHANGED") == []
    assert [e["path"] for e in named(events, "FILE_PACKED")] == ["a", "b"]
    assert len(named(events, "PACK_CREATED")) == 1
    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["packs"] == 1
    assert jc["total_size"] == 32
    assert "files_skipped_unchanged" not in jc
    assert "dest_state_files" not in jc


def test_no_strategy_job_unaffected_by_destination(tmp_path):
    """Regression guard: a job without a strategy stays checkpoint-1 byte-compatible.

    Even with a ``destination`` field, a matching on-disk backup, and ``--backup``
    set, a strategy-less job emits no STRATEGY_SELECTED, no DEST_STATE_LOADED, no
    per-file backup/skip events, and a bare JOB_COMPLETED.
    """
    write_tree(tmp_path / "mount", {"a.txt": b"x", "b.bin": b"y"})
    backup = tmp_path / "backup"
    seed_dest(backup, "job", {"a.txt": b"x"})
    schedule = """
    version: 1
    timezone: UTC
    jobs:
      - id: job
        source: mount://
        destination: backup://store
        when: {kind: daily, at: "03:30"}
        exclude: ["**/*.bin"]
    """
    events = events_of(run_scheduler(tmp_path, schedule, backup=backup))
    assert named(events, "STRATEGY_SELECTED") == []
    assert named(events, "DEST_STATE_LOADED") == []
    assert named(events, "FILE_BACKED_UP") == []
    assert named(events, "FILE_SKIPPED_UNCHANGED") == []
    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["selected"] == 1
    assert jc["excluded"] == 1
    assert "total_size" not in jc
    assert "packs" not in jc
    assert "files_skipped_unchanged" not in jc
    assert "dest_state_files" not in jc


def test_dest_state_is_scoped_per_job_id(tmp_path):
    """Two jobs over the same source: only the job with a seeded destination skips."""
    write_tree(tmp_path / "mount", {"x.txt": b"data"})
    backup = tmp_path / "backup"
    seed_dest(backup, "job-a", {"x.txt": b"data"})  # only job-a has a prior backup
    schedule = """
    version: 1
    timezone: UTC
    jobs:
      - id: job-a
        source: mount://
        destination: backup://store
        when: {kind: daily, at: "03:30"}
        exclude: []
        strategy: {kind: full}
      - id: job-b
        source: mount://
        destination: backup://store
        when: {kind: daily, at: "03:30"}
        exclude: []
        strategy: {kind: full}
    """
    events = events_of(run_scheduler(tmp_path, schedule, backup=backup))

    loaded_ids = [e["job_id"] for e in named(events, "DEST_STATE_LOADED")]
    assert loaded_ids == ["job-a"]
    skipped = [(e["job_id"], e["path"]) for e in named(events, "FILE_SKIPPED_UNCHANGED")]
    assert skipped == [("job-a", "x.txt")]
    backed = [(e["job_id"], e["path"]) for e in named(events, "FILE_BACKED_UP")]
    assert backed == [("job-b", "x.txt")]

    by_job = {e["job_id"]: e for e in named(events, "JOB_COMPLETED")}
    assert by_job["job-a"]["files_skipped_unchanged"] == 1
    assert by_job["job-a"]["dest_state_files"] == 1
    assert by_job["job-b"]["files_skipped_unchanged"] == 0
    assert by_job["job-b"]["dest_state_files"] == 0


def test_changed_unchanged_and_new_file_mix(tmp_path):
    """The spec's key example shape: unchanged skipped, changed re-backed, new backed up."""
    source = {
        "A/K.html": b"kkk",          # unchanged
        "A/L.md": b"LL-changed-now",  # changed
        "O.md": b"ooo",               # unchanged
        "P.txt": b"brand-new",        # new (no prior backup)
    }
    write_tree(tmp_path / "mount", source)
    backup = tmp_path / "backup"
    seed_dest(backup, "job", {
        "A/K.html": b"kkk",     # same
        "A/L.md": b"LL-orig",   # different
        "O.md": b"ooo",         # same
    })
    events = events_of(run_scheduler(tmp_path, dest_schedule("full"), backup=backup))

    assert named(events, "DEST_STATE_LOADED")[0]["files_total"] == 3
    assert {e["path"] for e in named(events, "FILE_SKIPPED_UNCHANGED")} == {"A/K.html", "O.md"}
    assert {e["path"] for e in named(events, "FILE_BACKED_UP")} == {"A/L.md", "P.txt"}

    # Each selected file is immediately resolved to exactly one outcome event.
    seq = [
        (e["event"], e["path"])
        for e in events
        if e["event"] in ("FILE_SELECTED", "FILE_SKIPPED_UNCHANGED", "FILE_BACKED_UP")
    ]
    assert seq == [
        ("FILE_SELECTED", "A/K.html"),
        ("FILE_SKIPPED_UNCHANGED", "A/K.html"),
        ("FILE_SELECTED", "A/L.md"),
        ("FILE_BACKED_UP", "A/L.md"),
        ("FILE_SELECTED", "O.md"),
        ("FILE_SKIPPED_UNCHANGED", "O.md"),
        ("FILE_SELECTED", "P.txt"),
        ("FILE_BACKED_UP", "P.txt"),
    ]

    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["selected"] == 4
    assert jc["files_skipped_unchanged"] == 2
    assert jc["dest_state_files"] == 3
    assert jc["total_size"] == len(b"LL-changed-now") + len(b"brand-new")


def test_nested_paths_map_to_job_relative_layout(tmp_path):
    """Skip detection honors the ``<dest>/<job_id>/<rel>`` layout for nested files."""
    source = {"A/I.py": b"i-content", "A/K.html": b"k-content", "M.py": b"m-content"}
    write_tree(tmp_path / "mount", source)
    backup = tmp_path / "backup"
    seed_dest(backup, "job", source)  # identical nested layout
    events = events_of(run_scheduler(tmp_path, dest_schedule("full"), backup=backup))
    assert named(events, "DEST_STATE_LOADED")[0]["files_total"] == 3
    assert {e["path"] for e in named(events, "FILE_SKIPPED_UNCHANGED")} == {
        "A/I.py", "A/K.html", "M.py",
    }
    assert named(events, "FILE_BACKED_UP") == []
    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["files_skipped_unchanged"] == 3
    assert jc["dest_state_files"] == 3
    assert jc["total_size"] == 0


def test_dest_state_counts_orphan_backups(tmp_path):
    """A backed-up file with no current source counterpart still counts in dest-state totals."""
    write_tree(tmp_path / "mount", {"a.txt": b"A"})
    backup = tmp_path / "backup"
    seed_dest(backup, "job", {"a.txt": b"A", "gone.txt": b"deleted-from-source"})
    events = events_of(run_scheduler(tmp_path, dest_schedule("full"), backup=backup))
    assert named(events, "DEST_STATE_LOADED")[0]["files_total"] == 2
    # gone.txt is not in the source tree, so it is never selected/backed up/skipped.
    assert {e["path"] for e in named(events, "FILE_SKIPPED_UNCHANGED")} == {"a.txt"}
    assert all(e["path"] != "gone.txt" for e in named(events, "FILE_BACKED_UP"))
    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["dest_state_files"] == 2
    assert jc["files_skipped_unchanged"] == 1
    assert jc["total_size"] == 0


# --------------------------------------------------------------------------- #
# 4. Happy-path / structural cases
# --------------------------------------------------------------------------- #
def test_dest_state_loaded_position_and_fields(tmp_path):
    """DEST_STATE_LOADED sits after JOB_STARTED, before STRATEGY_SELECTED, with correct fields."""
    write_tree(tmp_path / "mount", {"a.txt": b"hi"})
    backup = tmp_path / "backup"
    seed_dest(backup, "job", {"a.txt": b"hi"})
    events = events_of(run_scheduler(tmp_path, dest_schedule("full"), backup=backup))
    order = [e["event"] for e in events]
    assert "DEST_STATE_LOADED" in order
    i_started = order.index("JOB_STARTED")
    i_loaded = order.index("DEST_STATE_LOADED")
    i_strategy = order.index("STRATEGY_SELECTED")
    i_first_file = order.index("FILE_SELECTED")
    assert i_started < i_loaded < i_strategy < i_first_file
    loaded = named(events, "DEST_STATE_LOADED")[0]
    assert loaded["job_id"] == "job"
    assert loaded["files_total"] == 1


def test_new_events_are_compact_json(tmp_path):
    """DEST_STATE_LOADED and FILE_SKIPPED_UNCHANGED obey the compact-JSON formatting rule."""
    files = {"a.txt": b"same1", "b.txt": b"same2"}
    write_tree(tmp_path / "mount", files)
    backup = tmp_path / "backup"
    seed_dest(backup, "job", files)
    proc = run_scheduler(tmp_path, dest_schedule("full"), backup=backup)
    assert proc.returncode == 0, proc.stderr
    saw_new_event = False
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        assert ", " not in line, f"space after comma: {line!r}"
        assert '": ' not in line, f"space after colon: {line!r}"
        obj = json.loads(line)
        if obj["event"] in ("DEST_STATE_LOADED", "FILE_SKIPPED_UNCHANGED"):
            saw_new_event = True
    assert saw_new_event, "expected at least one new checkpoint-3 event in the stream"
