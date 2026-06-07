"""Phase 0 red tests for BENCH-13 (file_backup checkpoint_1 — Core Tests).

These tests describe the expected post-implementation behavior of
``backup_scheduler.py`` as specified in the ticket. They drive the binary
end-to-end via subprocess and assert on the JSON Lines event stream emitted to
stdout. They fail on the current (absent/incomplete) implementation.

Priority order within this file: edge/boundary cases, error/rejection cases,
cross-feature interaction cases, then happy-path.
"""

import json
import subprocess
import sys
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "backup_scheduler.py"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def make_tree(root: Path, rel_paths):
    """Create empty files at the given relative POSIX paths under ``root``."""
    root.mkdir(parents=True, exist_ok=True)
    for rel in rel_paths:
        p = root / Path(rel)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("x")


def run_scheduler(tmp_path, schedule_yaml, now, *, duration=None, mount=None):
    sched = tmp_path / "schedule.yaml"
    sched.write_text(textwrap.dedent(schedule_yaml))
    if mount is None:
        mount = tmp_path / "mount"
        mount.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        str(SCRIPT),
        "--schedule",
        str(sched),
        "--now",
        now,
        "--mount",
        str(mount),
    ]
    if duration is not None:
        cmd += ["--duration", str(duration)]
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


def file_events(events, job_id):
    return [
        e
        for e in events
        if e["event"] in ("FILE_SELECTED", "FILE_EXCLUDED") and e["job_id"] == job_id
    ]


def eligible_ids(events):
    return [e["job_id"] for e in events if e["event"] == "JOB_ELIGIBLE"]


# A schedule + tree that makes a single daily job due at 2025-09-10T03:30Z.
DAILY_AT = """
version: 1
timezone: UTC
jobs:
  - id: job
    source: mount://
    when:
      kind: daily
      at: "{at}"
    exclude: {exclude}
"""


# --------------------------------------------------------------------------- #
# 1. Edge / boundary cases
# --------------------------------------------------------------------------- #
def test_duration_upper_bound_inclusive(tmp_path):
    """Trigger at exactly now+duration is due (inclusive bound)."""
    make_tree(tmp_path / "mount", [])
    proc = run_scheduler(
        tmp_path,
        DAILY_AT.format(at="04:00", exclude="[]"),
        "2025-09-10T03:00:00Z",
        duration=1,
    )
    assert eligible_ids(events_of(proc)) == ["job"]


def test_trigger_just_past_window_not_due(tmp_path):
    """Trigger one minute past now+duration is NOT due."""
    make_tree(tmp_path / "mount", [])
    proc = run_scheduler(
        tmp_path,
        DAILY_AT.format(at="04:01", exclude="[]"),
        "2025-09-10T03:00:00Z",
        duration=1,
    )
    assert eligible_ids(events_of(proc)) == []


def test_duration_zero_is_exact_minute_only(tmp_path):
    make_tree(tmp_path / "mount", [])
    due = run_scheduler(
        tmp_path, DAILY_AT.format(at="03:30", exclude="[]"), "2025-09-10T03:30:00Z", duration=0
    )
    assert eligible_ids(events_of(due)) == ["job"]
    not_due = run_scheduler(
        tmp_path, DAILY_AT.format(at="03:31", exclude="[]"), "2025-09-10T03:30:00Z", duration=0
    )
    assert eligible_ids(events_of(not_due)) == []


def test_seconds_are_floored_to_the_minute(tmp_path):
    """Seconds on --now are ignored; now_local reports :00 seconds."""
    make_tree(tmp_path / "mount", [])
    proc = run_scheduler(
        tmp_path, DAILY_AT.format(at="03:30", exclude="[]"), "2025-09-10T03:30:45Z", duration=0
    )
    events = events_of(proc)
    assert eligible_ids(events) == ["job"]
    elig = next(e for e in events if e["event"] == "JOB_ELIGIBLE")
    assert elig["now_local"] == "2025-09-10T03:30:00Z"


def test_daily_not_due_outside_window(tmp_path):
    make_tree(tmp_path / "mount", [])
    proc = run_scheduler(
        tmp_path, DAILY_AT.format(at="03:30", exclude="[]"), "2025-09-10T10:00:00Z", duration=1
    )
    assert eligible_ids(events_of(proc)) == []


def test_doublestar_matches_zero_directories(tmp_path):
    """``**/*.bin`` matches a root-level ``E.bin`` (zero intermediate dirs)."""
    make_tree(tmp_path / "mount", ["E.bin", "keep.txt"])
    proc = run_scheduler(
        tmp_path, DAILY_AT.format(at="03:30", exclude='["**/*.bin"]'), "2025-09-10T03:30:00Z", duration=0
    )
    fe = file_events(events_of(proc), "job")
    by_path = {e["path"]: e for e in fe}
    assert by_path["E.bin"]["event"] == "FILE_EXCLUDED"
    assert by_path["E.bin"]["pattern"] == "**/*.bin"
    assert by_path["keep.txt"]["event"] == "FILE_SELECTED"


def test_single_star_does_not_cross_slash(tmp_path):
    """``*.py`` matches root ``M.py`` but not nested ``A/B/C/D.py``."""
    make_tree(tmp_path / "mount", ["M.py", "A/B/C/D.py"])
    proc = run_scheduler(
        tmp_path, DAILY_AT.format(at="03:30", exclude='["*.py"]'), "2025-09-10T03:30:00Z", duration=0
    )
    by_path = {e["path"]: e for e in file_events(events_of(proc), "job")}
    assert by_path["M.py"]["event"] == "FILE_EXCLUDED"
    assert by_path["A/B/C/D.py"]["event"] == "FILE_SELECTED"


# --------------------------------------------------------------------------- #
# 2. Error / rejection cases
# --------------------------------------------------------------------------- #
def test_disabled_job_emits_no_events(tmp_path):
    make_tree(tmp_path / "mount", ["a.txt"])
    schedule = """
    version: 1
    timezone: UTC
    jobs:
      - id: on-job
        source: mount://
        when: {kind: daily, at: "03:30"}
        exclude: []
      - id: off-job
        enabled: false
        source: mount://
        when: {kind: daily, at: "03:30"}
        exclude: []
    """
    events = events_of(
        run_scheduler(tmp_path, schedule, "2025-09-10T03:30:00Z", duration=0)
    )
    assert all(e.get("job_id") != "off-job" for e in events)
    assert eligible_ids(events) == ["on-job"]


def test_jobs_total_counts_all_jobs_in_schedule(tmp_path):
    """jobs_total counts every job, including disabled ones."""
    make_tree(tmp_path / "mount", [])
    schedule = """
    version: 1
    timezone: UTC
    jobs:
      - id: a
        enabled: false
        source: mount://
        when: {kind: daily, at: "03:30"}
        exclude: []
      - id: b
        source: mount://
        when: {kind: daily, at: "03:30"}
        exclude: []
    """
    events = events_of(run_scheduler(tmp_path, schedule, "2025-09-10T03:30:00Z", duration=0))
    parsed = events[0]
    assert parsed["event"] == "SCHEDULE_PARSED"
    assert parsed["jobs_total"] == 2


def test_once_triggers_only_at_exact_timestamp(tmp_path):
    make_tree(tmp_path / "mount", [])
    schedule = """
    version: 1
    timezone: UTC
    jobs:
      - id: one
        source: mount://
        when: {kind: once, at: "2025-09-10T03:30:00"}
        exclude: []
    """
    due = run_scheduler(tmp_path, schedule, "2025-09-10T03:00:00Z", duration=1)
    assert eligible_ids(events_of(due)) == ["one"]
    elig = next(e for e in events_of(due) if e["event"] == "JOB_ELIGIBLE")
    assert elig["kind"] == "once"
    # Window that starts after the once-timestamp: not due.
    not_due = run_scheduler(tmp_path, schedule, "2025-09-10T04:00:00Z", duration=1)
    assert eligible_ids(events_of(not_due)) == []


def test_weekly_triggers_only_on_listed_days(tmp_path):
    # 2025-09-10 is a Wednesday.
    make_tree(tmp_path / "mount", [])
    schedule = """
    version: 1
    timezone: UTC
    jobs:
      - id: wk
        source: mount://
        when: {kind: weekly, at: "03:30", days: ["wed"]}
        exclude: []
    """
    on_day = run_scheduler(tmp_path, schedule, "2025-09-10T03:30:00Z", duration=0)
    assert eligible_ids(events_of(on_day)) == ["wk"]
    # Thursday 2025-09-11: not in days list.
    off_day = run_scheduler(tmp_path, schedule, "2025-09-11T03:30:00Z", duration=0)
    assert eligible_ids(events_of(off_day)) == []


# --------------------------------------------------------------------------- #
# 3. Cross-feature interaction cases
# --------------------------------------------------------------------------- #
def test_first_matching_exclude_pattern_recorded(tmp_path):
    """When two patterns match, the first in list order is recorded."""
    make_tree(tmp_path / "mount", ["A/J.bin"])
    proc = run_scheduler(
        tmp_path,
        DAILY_AT.format(at="03:30", exclude='["A/*", "**/*.bin"]'),
        "2025-09-10T03:30:00Z",
        duration=0,
    )
    fe = file_events(events_of(proc), "job")
    assert fe[0]["event"] == "FILE_EXCLUDED"
    assert fe[0]["path"] == "A/J.bin"
    assert fe[0]["pattern"] == "A/*"


def test_jobs_executed_in_id_order(tmp_path):
    make_tree(tmp_path / "mount", [])
    schedule = """
    version: 1
    timezone: UTC
    jobs:
      - id: zebra-job
        source: mount://
        when: {kind: daily, at: "03:30"}
        exclude: []
      - id: alpha-job
        source: mount://
        when: {kind: daily, at: "03:30"}
        exclude: []
      - id: beta-job
        source: mount://
        when: {kind: daily, at: "03:30"}
        exclude: []
    """
    events = events_of(run_scheduler(tmp_path, schedule, "2025-09-10T03:30:00Z", duration=0))
    assert eligible_ids(events) == ["alpha-job", "beta-job", "zebra-job"]


def test_files_processed_in_lexicographic_path_order(tmp_path):
    make_tree(tmp_path / "mount", ["b.txt", "a/z.txt", "a/b.txt", "C.txt"])
    proc = run_scheduler(
        tmp_path, DAILY_AT.format(at="03:30", exclude="[]"), "2025-09-10T03:30:00Z", duration=0
    )
    paths = [e["path"] for e in file_events(events_of(proc), "job")]
    assert paths == sorted(paths)
    assert paths == ["C.txt", "a/b.txt", "a/z.txt", "b.txt"]


def test_now_local_reflects_schedule_timezone(tmp_path):
    """now_local is --now expressed in the schedule timezone with an offset."""
    make_tree(tmp_path / "mount", [])
    schedule = """
    version: 1
    timezone: America/New_York
    jobs:
      - id: ny
        source: mount://
        when: {kind: daily, at: "08:00"}
        exclude: []
    """
    # 12:00Z == 08:00 EDT (-04:00) on 2025-09-10.
    events = events_of(run_scheduler(tmp_path, schedule, "2025-09-10T12:00:00Z", duration=0))
    assert eligible_ids(events) == ["ny"]
    elig = next(e for e in events if e["event"] == "JOB_ELIGIBLE")
    assert elig["now_local"] == "2025-09-10T08:00:00-04:00"


# --------------------------------------------------------------------------- #
# 4. Happy-path / structural cases
# --------------------------------------------------------------------------- #
def test_schedule_parsed_is_first_and_singleton(tmp_path):
    make_tree(tmp_path / "mount", ["a.txt"])
    events = events_of(
        run_scheduler(tmp_path, DAILY_AT.format(at="03:30", exclude="[]"), "2025-09-10T03:30:00Z", duration=0)
    )
    assert events[0]["event"] == "SCHEDULE_PARSED"
    assert events[0]["timezone"] == "UTC"
    assert sum(1 for e in events if e["event"] == "SCHEDULE_PARSED") == 1


def test_compact_json_formatting(tmp_path):
    make_tree(tmp_path / "mount", ["a.txt", "b.bin"])
    proc = run_scheduler(
        tmp_path, DAILY_AT.format(at="03:30", exclude='["*.bin"]'), "2025-09-10T03:30:00Z", duration=0
    )
    assert proc.returncode == 0, proc.stderr
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        # Compact: no whitespace after separators.
        assert ", " not in line, f"space after comma: {line!r}"
        assert '": ' not in line, f"space after colon: {line!r}"
        json.loads(line)  # each line is valid JSON


def test_example1_relative_paths_and_counts(tmp_path):
    """Full Example 1 from the spec: two jobs, relative paths, exact counts."""
    tree = [
        "A/B/C/D.py",
        "A/B/E.bin",
        "A/F/G.bin",
        "A/F/H.py",
        "A/I.py",
        "A/J.bin",
        "A/K.html",
        "A/L.md",
        "M.py",
        "N.py",
        "O.md",
    ]
    make_tree(tmp_path / "mount", tree)
    schedule = """
    version: 1
    timezone: UTC
    jobs:
      - id: job-root
        source: mount://
        exclude: ["A/*", "*.py", "**/*.bin"]
        when:
          kind: daily
          at: "03:30"
      - id: job-subdir
        source: mount://A/B
        exclude: ["*.py", "**/*.bin"]
        when:
          kind: daily
          at: "03:30"
    """
    events = events_of(run_scheduler(tmp_path, schedule, "2025-09-10T03:30:00Z", duration=1))

    # job-root: full ordered file stream.
    root_expected = [
        ("FILE_SELECTED", "A/B/C/D.py", None),
        ("FILE_EXCLUDED", "A/B/E.bin", "**/*.bin"),
        ("FILE_EXCLUDED", "A/F/G.bin", "**/*.bin"),
        ("FILE_SELECTED", "A/F/H.py", None),
        ("FILE_EXCLUDED", "A/I.py", "A/*"),
        ("FILE_EXCLUDED", "A/J.bin", "A/*"),
        ("FILE_EXCLUDED", "A/K.html", "A/*"),
        ("FILE_EXCLUDED", "A/L.md", "A/*"),
        ("FILE_EXCLUDED", "M.py", "*.py"),
        ("FILE_EXCLUDED", "N.py", "*.py"),
        ("FILE_SELECTED", "O.md", None),
    ]
    root_actual = [
        (e["event"], e["path"], e.get("pattern")) for e in file_events(events, "job-root")
    ]
    assert root_actual == root_expected

    root_completed = next(
        e for e in events if e["event"] == "JOB_COMPLETED" and e["job_id"] == "job-root"
    )
    assert (root_completed["selected"], root_completed["excluded"]) == (3, 8)

    started = next(
        e for e in events if e["event"] == "JOB_STARTED" and e["job_id"] == "job-root"
    )
    assert started["exclude_count"] == 3

    # job-subdir: paths relative to A/B.
    sub_actual = [
        (e["event"], e["path"], e.get("pattern")) for e in file_events(events, "job-subdir")
    ]
    assert sub_actual == [
        ("FILE_SELECTED", "C/D.py", None),
        ("FILE_EXCLUDED", "E.bin", "**/*.bin"),
    ]
    sub_completed = next(
        e for e in events if e["event"] == "JOB_COMPLETED" and e["job_id"] == "job-subdir"
    )
    assert (sub_completed["selected"], sub_completed["excluded"]) == (1, 1)

    # Jobs run in id order: job-root before job-subdir.
    assert eligible_ids(events) == ["job-root", "job-subdir"]
