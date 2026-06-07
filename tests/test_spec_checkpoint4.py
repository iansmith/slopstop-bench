"""Phase 0 red tests for BENCH-16 (file_backup checkpoint_4 — Core Tests).

These tests describe the expected post-implementation behavior of the
*incremental pack* feature added in checkpoint 4. They fail on the current
(checkpoint-3) implementation, whose ``pack`` strategy never consults the
destination: it always emits ``FILE_PACKED`` + ``PACK_CREATED`` and tracks no
incremental state.

Spec model encoded here
-----------------------
The destination for a pack job holds finished archives named ``pack-<N>.tar``::

    <backup>/<store>/<job_id>/pack-1.tar
                             /pack-2.tar
                             ...

Before packing, the strategy loads every ``pack-<N>.tar`` it finds, emitting one
``PACK_LOADED`` (``name``, ``files_total``, ``checksum``) per pack *after*
``STRATEGY_SELECTED``. Loading a pack contributes ``{member_path: sha256}`` to a
dest-state map, and remembers each old pack's content-size sum and tar checksum
by name.

Packing then reruns *from scratch* over the current files under the same
``max_pack_bytes`` limit (identical algorithm to checkpoint 2 — every file is
always placed into a pack). The only differences:

  * A selected file whose current hash equals its dest-state hash emits
    ``PACK_SKIP_UNCHANGED`` (``pack_id``/``path``/``size``/``hash``) instead of
    ``FILE_PACKED`` — but it is still packed into the new archive.
  * When a new pack ``pack-<N>.tar`` is finalized it is compared, by name, to the
    old pack of the same index:
      - byte-identical tar  -> ``PACK_UNCHANGED`` (``name``, ``checksum``)
      - otherwise           -> ``PACK_UPDATED``  (``name``, ``size``, ``checksum``,
        ``timestamp``, ``tar_size``, ``old_size``, ``old_checksum``)
    These *replace* ``PACK_CREATED``.
  * ``JOB_COMPLETED`` gains ``files_skipped_unchanged`` and ``dest_state_files``
    (always ``0`` for pack) — but only when existing packs were loaded.

Preservation requirement: with no ``--backup``, no destination packs, or only
non-pack files present, the pack job behaves *exactly* like checkpoint 2
(``FILE_PACKED`` + ``PACK_CREATED``, no extra summary fields). Three tests below
guard that and therefore already pass on the current code.

Priority order within this file: edge/boundary, error/rejection, cross-feature
interaction, then happy-path.
"""

import json
import subprocess
import sys
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "backup_scheduler.py"
sys.path.insert(0, str(REPO_ROOT))
import backup_scheduler as bs  # noqa: E402  (path set above so seed packs match production tar bytes)

NOW = "2025-09-10T03:30:00Z"  # daily job at 03:30 UTC, due with duration 0
NOW_LOCAL = "2025-09-10T03:30:00Z"

# The spec's canonical five-file scenario; byte lengths are 1,2,2,3,1.
A, B, C, D, E = b"A", b"BB", b"CC", b"DDD", b"E"
STD_FILES = {"A": A, "B": B, "C": C, "D": D, "E": E}


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


def seed_packs(backup_root: Path, job_id: str, packs, store: str = "store"):
    """Pre-populate a job's destination with ``pack-<N>.tar`` archives.

    ``packs`` is an ordered list of packs; each pack is an ordered list of
    ``(arcname, content)`` members. Archives are serialized with the *production*
    tar normalizer so that an unchanged repack is byte-identical (and therefore
    detected as ``PACK_UNCHANGED``).
    """
    d = backup_root / store / job_id
    d.mkdir(parents=True, exist_ok=True)
    for i, members in enumerate(packs, start=1):
        (d / f"pack-{i}.tar").write_bytes(bs._pack_tar_bytes(members))


def tar_bytes(members):
    return bs._pack_tar_bytes(members)


def tar_cksum(members):
    return bs._sha256(bs._pack_tar_bytes(members))


def content_sha(content: bytes) -> str:
    return bs._sha256(content)


def pack_schedule(max_pack_bytes=4, job_id="job", destination="backup://store", source="mount://"):
    return textwrap.dedent(
        f"""
        version: 1
        timezone: UTC
        jobs:
          - id: {job_id}
            source: {source}
            destination: {destination}
            when: {{kind: daily, at: "03:30"}}
            exclude: []
            strategy:
              kind: pack
              options:
                max_pack_bytes: {max_pack_bytes}
        """
    )


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


def ident(e):
    """File events carry ``path``; pack lifecycle events carry ``name``."""
    return e.get("path", e.get("name"))


# --------------------------------------------------------------------------- #
# 1. Edge / boundary cases
# --------------------------------------------------------------------------- #
def test_first_run_empty_destination_matches_checkpoint2_pack(tmp_path):
    """Boundary: an empty destination yields the verbatim checkpoint-2 pack stream.

    No ``PACK_LOADED``, every file ``FILE_PACKED``, ``PACK_CREATED`` per pack, and
    JOB_COMPLETED carries neither incremental summary field. (Preservation guard —
    already passes on checkpoint-3 code, must keep passing.)
    """
    write_tree(tmp_path / "mount", STD_FILES)
    backup = tmp_path / "backup"
    backup.mkdir()  # exists but holds no packs for this job
    events = events_of(run_scheduler(tmp_path, pack_schedule(), backup=backup))

    assert named(events, "PACK_LOADED") == []
    packed = [(e["path"], e["pack_id"]) for e in named(events, "FILE_PACKED")]
    assert packed == [("A", 1), ("B", 1), ("C", 2), ("D", 3), ("E", 3)]
    assert [e["name"] for e in named(events, "PACK_CREATED")] == [
        "pack-1.tar", "pack-2.tar", "pack-3.tar",
    ]
    assert named(events, "PACK_UNCHANGED") == []
    assert named(events, "PACK_UPDATED") == []
    assert named(events, "PACK_SKIP_UNCHANGED") == []
    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["selected"] == 5 and jc["excluded"] == 0
    assert jc["packs"] == 3 and jc["total_size"] == 9
    assert "files_skipped_unchanged" not in jc
    assert "dest_state_files" not in jc


def test_pack_loaded_emitted_after_strategy_selected_with_fields(tmp_path):
    """Boundary/structural: one PACK_LOADED per existing pack, after STRATEGY_SELECTED.

    Each PACK_LOADED reports the pack name, its member count, and the tar's
    SHA-256, and all of them precede the first FILE_SELECTED.
    """
    write_tree(tmp_path / "mount", STD_FILES)
    backup = tmp_path / "backup"
    seed_packs(backup, "job", [[("A", A), ("B", B)], [("C", C)], [("D", D), ("E", E)]])
    events = events_of(run_scheduler(tmp_path, pack_schedule(), backup=backup))

    order = [e["event"] for e in events]
    i_strategy = order.index("STRATEGY_SELECTED")
    first_loaded = order.index("PACK_LOADED")
    last_loaded = len(order) - 1 - order[::-1].index("PACK_LOADED")
    i_first_file = order.index("FILE_SELECTED")
    assert i_strategy < first_loaded
    assert last_loaded < i_first_file

    loaded = named(events, "PACK_LOADED")
    assert [e["name"] for e in loaded] == ["pack-1.tar", "pack-2.tar", "pack-3.tar"]
    assert [e["files_total"] for e in loaded] == [2, 1, 2]
    assert loaded[0]["checksum"] == tar_cksum([("A", A), ("B", B)])
    assert loaded[1]["checksum"] == tar_cksum([("C", C)])
    assert loaded[2]["checksum"] == tar_cksum([("D", D), ("E", E)])
    for e in loaded:
        assert set(e) >= {"event", "job_id", "name", "files_total", "checksum"}
        assert e["job_id"] == "job"


# --------------------------------------------------------------------------- #
# 2. Error / rejection cases
# --------------------------------------------------------------------------- #
def test_backup_flag_absent_ignores_existing_packs(tmp_path):
    """Rejection: with ``--backup`` omitted, on-disk packs are invisible.

    The job runs as a clean first pack run. (Preservation guard.)
    """
    write_tree(tmp_path / "mount", STD_FILES)
    backup = tmp_path / "backup"
    seed_packs(backup, "job", [[("A", A), ("B", B)], [("C", C)], [("D", D), ("E", E)]])
    events = events_of(run_scheduler(tmp_path, pack_schedule(), backup=None))

    assert named(events, "PACK_LOADED") == []
    assert named(events, "PACK_SKIP_UNCHANGED") == []
    assert [e["path"] for e in named(events, "FILE_PACKED")] == ["A", "B", "C", "D", "E"]
    assert len(named(events, "PACK_CREATED")) == 3
    jc = named(events, "JOB_COMPLETED")[0]
    assert "files_skipped_unchanged" not in jc
    assert "dest_state_files" not in jc


def test_non_pack_files_in_destination_are_ignored(tmp_path):
    """Rejection: only ``pack-<N>.tar`` files count as existing packs.

    A destination holding unrelated files (raw blobs, a stray ``.tar.bak``) loads
    no packs, so the job stays in checkpoint-2 behavior. (Preservation guard that
    keeps the checkpoint-3 pack carve-out test green.)
    """
    write_tree(tmp_path / "mount", {"A": A, "B": B})
    backup = tmp_path / "backup"
    write_tree(
        backup / "store" / "job",
        {"notes.txt": b"hello", "data": b"\x00\x01", "pack-1.tar.bak": tar_bytes([("A", A)])},
    )
    events = events_of(run_scheduler(tmp_path, pack_schedule(), backup=backup))

    assert named(events, "PACK_LOADED") == []
    assert named(events, "PACK_SKIP_UNCHANGED") == []
    assert [e["path"] for e in named(events, "FILE_PACKED")] == ["A", "B"]
    assert len(named(events, "PACK_CREATED")) == 1
    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["packs"] == 1
    assert "files_skipped_unchanged" not in jc
    assert "dest_state_files" not in jc


# --------------------------------------------------------------------------- #
# 3. Cross-feature interaction cases
# --------------------------------------------------------------------------- #
def test_all_files_unchanged_skips_every_file_and_marks_packs_unchanged(tmp_path):
    """Every file matches its packed copy: all PACK_SKIP_UNCHANGED, all PACK_UNCHANGED."""
    write_tree(tmp_path / "mount", STD_FILES)
    backup = tmp_path / "backup"
    seed_packs(backup, "job", [[("A", A), ("B", B)], [("C", C)], [("D", D), ("E", E)]])
    events = events_of(run_scheduler(tmp_path, pack_schedule(), backup=backup))

    assert len(named(events, "PACK_LOADED")) == 3
    assert named(events, "FILE_PACKED") == []
    skips = [(e["path"], e["pack_id"], e["size"], e["hash"]) for e in named(events, "PACK_SKIP_UNCHANGED")]
    assert skips == [
        ("A", 1, 1, content_sha(A)),
        ("B", 1, 2, content_sha(B)),
        ("C", 2, 2, content_sha(C)),
        ("D", 3, 3, content_sha(D)),
        ("E", 3, 1, content_sha(E)),
    ]
    unchanged = named(events, "PACK_UNCHANGED")
    assert [e["name"] for e in unchanged] == ["pack-1.tar", "pack-2.tar", "pack-3.tar"]
    assert unchanged[0]["checksum"] == tar_cksum([("A", A), ("B", B)])
    assert named(events, "PACK_UPDATED") == []
    assert named(events, "PACK_CREATED") == []

    seq = [
        (e["event"], ident(e))
        for e in events
        if e["event"] in ("FILE_SELECTED", "PACK_SKIP_UNCHANGED", "PACK_UNCHANGED")
    ]
    assert seq == [
        ("FILE_SELECTED", "A"), ("PACK_SKIP_UNCHANGED", "A"),
        ("FILE_SELECTED", "B"), ("PACK_SKIP_UNCHANGED", "B"),
        ("PACK_UNCHANGED", "pack-1.tar"),
        ("FILE_SELECTED", "C"), ("PACK_SKIP_UNCHANGED", "C"),
        ("PACK_UNCHANGED", "pack-2.tar"),
        ("FILE_SELECTED", "D"), ("PACK_SKIP_UNCHANGED", "D"),
        ("FILE_SELECTED", "E"), ("PACK_SKIP_UNCHANGED", "E"),
        ("PACK_UNCHANGED", "pack-3.tar"),
    ]

    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["selected"] == 5 and jc["excluded"] == 0
    assert jc["packs"] == 3 and jc["total_size"] == 9
    assert jc["files_skipped_unchanged"] == 5
    assert jc["dest_state_files"] == 0


def test_some_files_changed_emits_pack_updated_with_old_metadata(tmp_path):
    """The spec's mixed example: changed packs UPDATED (with old_*), untouched pack UNCHANGED."""
    write_tree(tmp_path / "mount", STD_FILES)  # A=1,B=2,C=2,D=3,E=1
    backup = tmp_path / "backup"
    old_p1 = [("A", b"AAA")]          # A was 3 bytes
    old_p2 = [("C", b"CCCC")]         # C was 4 bytes
    old_p3 = [("D", D), ("E", E)]     # D,E unchanged
    seed_packs(backup, "job", [old_p1, old_p2, old_p3])
    events = events_of(run_scheduler(tmp_path, pack_schedule(), backup=backup))

    assert len(named(events, "PACK_LOADED")) == 3

    seq = [
        (e["event"], ident(e))
        for e in events
        if e["event"] in (
            "FILE_SELECTED", "FILE_PACKED", "PACK_SKIP_UNCHANGED", "PACK_UPDATED", "PACK_UNCHANGED"
        )
    ]
    assert seq == [
        ("FILE_SELECTED", "A"), ("FILE_PACKED", "A"),
        ("FILE_SELECTED", "B"), ("FILE_PACKED", "B"),
        ("FILE_SELECTED", "C"), ("PACK_UPDATED", "pack-1.tar"), ("FILE_PACKED", "C"),
        ("FILE_SELECTED", "D"), ("PACK_UPDATED", "pack-2.tar"), ("PACK_SKIP_UNCHANGED", "D"),
        ("FILE_SELECTED", "E"), ("PACK_SKIP_UNCHANGED", "E"),
        ("PACK_UNCHANGED", "pack-3.tar"),
    ]

    by_name = {e["name"]: e for e in named(events, "PACK_UPDATED")}
    pu1 = by_name["pack-1.tar"]
    assert set(pu1) >= {
        "event", "job_id", "name", "size", "checksum",
        "timestamp", "tar_size", "old_size", "old_checksum",
    }
    assert pu1["size"] == 3                       # A(1)+B(2)
    assert pu1["old_size"] == 3                    # A-old (3 bytes)
    assert pu1["old_checksum"] == tar_cksum(old_p1)
    assert pu1["checksum"] == tar_cksum([("A", A), ("B", B)])
    assert pu1["tar_size"] == len(tar_bytes([("A", A), ("B", B)]))
    assert pu1["timestamp"] == NOW_LOCAL

    pu2 = by_name["pack-2.tar"]
    assert pu2["size"] == 2                        # C(2)
    assert pu2["old_size"] == 4                    # C-old (4 bytes)
    assert pu2["old_checksum"] == tar_cksum(old_p2)
    assert pu2["checksum"] == tar_cksum([("C", C)])

    unchanged = named(events, "PACK_UNCHANGED")
    assert [e["name"] for e in unchanged] == ["pack-3.tar"]
    assert unchanged[0]["checksum"] == tar_cksum(old_p3)

    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["selected"] == 5 and jc["packs"] == 3
    assert jc["total_size"] == 9
    assert jc["files_skipped_unchanged"] == 2
    assert jc["dest_state_files"] == 0


def test_max_pack_bytes_constraint_applies_when_repacking(tmp_path):
    """Repacking reruns the size-limited algorithm on *current* sizes, not old boundaries.

    Shrinking D (3 -> 1 byte) lets C+D+E share one 4-byte pack, collapsing the old
    three-pack layout to two. The new packing must still honor ``max_pack_bytes``.
    """
    current = {"A": A, "B": B, "C": C, "D": b"D", "E": E}  # D shrinks 3 -> 1
    write_tree(tmp_path / "mount", current)
    backup = tmp_path / "backup"
    old_p1 = [("A", A), ("B", B)]
    old_p2 = [("C", C)]
    old_p3 = [("D", D), ("E", E)]
    seed_packs(backup, "job", [old_p1, old_p2, old_p3])
    events = events_of(run_scheduler(tmp_path, pack_schedule(max_pack_bytes=4), backup=backup))

    # New layout: pack-1=(A,B); pack-2=(C,D',E) with C+D'+E = 2+1+1 = 4 == limit.
    skips = {e["path"]: e["pack_id"] for e in named(events, "PACK_SKIP_UNCHANGED")}
    packed = {e["path"]: e["pack_id"] for e in named(events, "FILE_PACKED")}
    assert skips == {"A": 1, "B": 1, "C": 2, "E": 2}  # unchanged content
    assert packed == {"D": 2}                          # only D changed
    # Every member of the new second pack lands in pack_id 2 (constraint respected).
    assert {**skips, **packed}["C"] == 2
    assert {**skips, **packed}["D"] == 2
    assert {**skips, **packed}["E"] == 2

    assert [e["name"] for e in named(events, "PACK_UNCHANGED")] == ["pack-1.tar"]
    updated = named(events, "PACK_UPDATED")
    assert [e["name"] for e in updated] == ["pack-2.tar"]
    assert updated[0]["old_size"] == 2               # old pack-2 was just C (2 bytes)
    assert updated[0]["old_checksum"] == tar_cksum(old_p2)

    jc = named(events, "JOB_COMPLETED")[0]
    assert jc["packs"] == 2
    assert jc["total_size"] == 7                      # 1+2+2+1+1
    assert jc["files_skipped_unchanged"] == 4
    assert jc["dest_state_files"] == 0


# --------------------------------------------------------------------------- #
# 4. Happy-path / structural cases
# --------------------------------------------------------------------------- #
def test_new_pack_events_are_compact_json(tmp_path):
    """The checkpoint-4 events obey the compact-JSON formatting rule."""
    write_tree(tmp_path / "mount", STD_FILES)
    backup = tmp_path / "backup"
    seed_packs(backup, "job", [[("A", A), ("B", B)], [("C", C)], [("D", D), ("E", E)]])
    proc = run_scheduler(tmp_path, pack_schedule(), backup=backup)
    assert proc.returncode == 0, proc.stderr

    saw_new_event = False
    new_events = {"PACK_LOADED", "PACK_SKIP_UNCHANGED", "PACK_UNCHANGED", "PACK_UPDATED"}
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        assert ", " not in line, f"space after comma: {line!r}"
        assert '": ' not in line, f"space after colon: {line!r}"
        obj = json.loads(line)
        if obj["event"] in new_events:
            saw_new_event = True
    assert saw_new_event, "expected at least one new checkpoint-4 pack event in the stream"
