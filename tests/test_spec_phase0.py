"""Phase 0 (TDD RED) — behavioral spec for backup_scheduler.py.

These tests describe the EXPECTED post-implementation behavior derived from the
BENCH-1 ticket spec. They drive the build end-to-end through the real CLI
contract (`python backup_scheduler.py ...`) and parse the JSON Lines emitted on
stdout. They are expected to FAIL until backup_scheduler.py is implemented.

Kept in a separate file from any grading suite so naming never collides.
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

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


def make_tree(root, rel_paths):
    for rel in rel_paths:
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("x")


def events_for(events, job_id):
    return [e for e in events if e.get("job_id") == job_id]


EXAMPLE1_TREE = [
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


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
def test_schedule_parsed_is_first_and_singleton(tmp_path):
    mount = tmp_path / "files"
    make_tree(mount, ["a.txt"])
    schedule = """
version: 1
timezone: UTC
jobs:
  - id: only-job
    source: mount://
    exclude: []
    when:
      kind: daily
      at: "03:30"
"""
    events = parse_events(
        run_scheduler(tmp_path, schedule, "2025-09-10T03:30:00Z", mount, duration=1)
    )
    assert events, "no events emitted"
    assert events[0]["event"] == "SCHEDULE_PARSED"
    assert events[0]["timezone"] == "UTC"
    assert events[0]["jobs_total"] == 1
    assert sum(1 for e in events if e["event"] == "SCHEDULE_PARSED") == 1


def test_compact_json_formatting(tmp_path):
    mount = tmp_path / "files"
    make_tree(mount, ["a.txt", "b.bin"])
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
"""
    proc = run_scheduler(tmp_path, schedule, "2025-09-10T03:30:00Z", mount, duration=1)
    assert proc.returncode == 0, proc.stderr
    # trailing newline, one object per line
    assert proc.stdout.endswith("\n")
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        json.loads(line)  # each line is valid JSON
        assert ", " not in line, f"non-compact (space after comma): {line!r}"
        assert ": " not in line, f"non-compact (space after colon): {line!r}"


def test_example1_relative_paths_and_counts(tmp_path):
    mount = tmp_path / "files"
    make_tree(mount, EXAMPLE1_TREE)
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
    events = parse_events(
        run_scheduler(tmp_path, schedule, "2025-09-10T03:30:00Z", mount, duration=1)
    )
    assert events[0]["jobs_total"] == 2

    # job-root
    root = events_for(events, "job-root")
    selected = {e["path"] for e in root if e["event"] == "FILE_SELECTED"}
    excluded = {e["path"]: e["pattern"] for e in root if e["event"] == "FILE_EXCLUDED"}
    assert selected == {"A/B/C/D.py", "A/F/H.py", "O.md"}
    assert excluded["A/B/E.bin"] == "**/*.bin"
    assert excluded["A/I.py"] == "A/*"
    assert excluded["M.py"] == "*.py"
    completed = [e for e in root if e["event"] == "JOB_COMPLETED"][0]
    assert completed["selected"] == 3
    assert completed["excluded"] == 8

    # files processed in lexicographic order by full relative path
    root_file_paths = [e["path"] for e in root if e["event"] in ("FILE_SELECTED", "FILE_EXCLUDED")]
    assert root_file_paths == sorted(EXAMPLE1_TREE)

    # job-subdir: paths relative to A/B
    sub = events_for(events, "job-subdir")
    sub_selected = {e["path"] for e in sub if e["event"] == "FILE_SELECTED"}
    sub_excluded = {e["path"]: e["pattern"] for e in sub if e["event"] == "FILE_EXCLUDED"}
    assert sub_selected == {"C/D.py"}
    assert sub_excluded == {"E.bin": "**/*.bin"}
    sub_completed = [e for e in sub if e["event"] == "JOB_COMPLETED"][0]
    assert sub_completed["selected"] == 1
    assert sub_completed["excluded"] == 1


def test_first_matching_exclude_pattern_recorded(tmp_path):
    mount = tmp_path / "files"
    make_tree(mount, ["data/x.bin"])
    # both patterns match; the FIRST in list order must be reported
    schedule_a = """
version: 1
timezone: UTC
jobs:
  - id: j
    source: mount://
    exclude: ["**/*.bin", "data/**"]
    when:
      kind: daily
      at: "00:00"
"""
    events = parse_events(
        run_scheduler(tmp_path, schedule_a, "2025-09-10T00:00:00Z", mount, duration=0)
    )
    exc = [e for e in events if e["event"] == "FILE_EXCLUDED"][0]
    assert exc["pattern"] == "**/*.bin"

    schedule_b = """
version: 1
timezone: UTC
jobs:
  - id: j
    source: mount://
    exclude: ["data/**", "**/*.bin"]
    when:
      kind: daily
      at: "00:00"
"""
    events = parse_events(
        run_scheduler(tmp_path, schedule_b, "2025-09-10T00:00:00Z", mount, duration=0)
    )
    exc = [e for e in events if e["event"] == "FILE_EXCLUDED"][0]
    assert exc["pattern"] == "data/**"


def test_jobs_executed_in_id_order(tmp_path):
    mount = tmp_path / "files"
    make_tree(mount, ["a.txt"])
    schedule = """
version: 1
timezone: UTC
jobs:
  - id: zebra-job
    source: mount://
    exclude: []
    when:
      kind: daily
      at: "03:30"
  - id: alpha-job
    source: mount://
    exclude: []
    when:
      kind: daily
      at: "03:30"
  - id: beta-job
    source: mount://
    exclude: []
    when:
      kind: daily
      at: "03:30"
"""
    events = parse_events(
        run_scheduler(tmp_path, schedule, "2025-09-10T03:30:00Z", mount, duration=1)
    )
    eligible_order = [e["job_id"] for e in events if e["event"] == "JOB_ELIGIBLE"]
    assert eligible_order == ["alpha-job", "beta-job", "zebra-job"]


def test_disabled_job_emits_no_events(tmp_path):
    mount = tmp_path / "files"
    make_tree(mount, ["a.txt"])
    schedule = """
version: 1
timezone: UTC
jobs:
  - id: on-job
    enabled: true
    source: mount://
    exclude: []
    when:
      kind: daily
      at: "03:30"
  - id: off-job
    enabled: false
    source: mount://
    exclude: []
    when:
      kind: daily
      at: "03:30"
"""
    events = parse_events(
        run_scheduler(tmp_path, schedule, "2025-09-10T03:30:00Z", mount, duration=1)
    )
    assert events_for(events, "off-job") == []
    assert events_for(events, "on-job"), "enabled job should still run"


def test_duration_upper_bound_inclusive(tmp_path):
    mount = tmp_path / "files"
    make_tree(mount, ["a.txt"])
    schedule = """
version: 1
timezone: UTC
jobs:
  - id: j
    source: mount://
    exclude: []
    when:
      kind: daily
      at: "04:00"
"""
    # window [03:00, 04:00] — trigger at exactly now+duration must be due
    events = parse_events(
        run_scheduler(tmp_path, schedule, "2025-09-10T03:00:00Z", mount, duration=1)
    )
    assert [e for e in events if e["event"] == "JOB_ELIGIBLE"], "inclusive upper bound failed"

    # duration 0 at the exact trigger minute -> due
    events = parse_events(
        run_scheduler(tmp_path, schedule, "2025-09-10T04:00:00Z", mount, duration=0)
    )
    assert [e for e in events if e["event"] == "JOB_ELIGIBLE"]


def test_trigger_just_past_window_not_due(tmp_path):
    mount = tmp_path / "files"
    make_tree(mount, ["a.txt"])
    schedule = """
version: 1
timezone: UTC
jobs:
  - id: j
    source: mount://
    exclude: []
    when:
      kind: daily
      at: "04:00"
"""
    # at 04:01 with duration 0, today's 04:00 already passed -> not due
    events = parse_events(
        run_scheduler(tmp_path, schedule, "2025-09-10T04:01:00Z", mount, duration=0)
    )
    assert [e for e in events if e["event"] == "JOB_ELIGIBLE"] == []
    # but SCHEDULE_PARSED is still emitted
    assert events[0]["event"] == "SCHEDULE_PARSED"


def test_doublestar_matches_zero_directories(tmp_path):
    mount = tmp_path / "files"
    make_tree(mount, ["top.bin", "nested/deep/x.bin", "keep.txt"])
    schedule = """
version: 1
timezone: UTC
jobs:
  - id: j
    source: mount://
    exclude: ["**/*.bin"]
    when:
      kind: daily
      at: "00:00"
"""
    events = parse_events(
        run_scheduler(tmp_path, schedule, "2025-09-10T00:00:00Z", mount, duration=0)
    )
    excluded = {e["path"] for e in events if e["event"] == "FILE_EXCLUDED"}
    selected = {e["path"] for e in events if e["event"] == "FILE_SELECTED"}
    # `**/*.bin` must match both a top-level .bin (zero dirs) and a nested one
    assert excluded == {"top.bin", "nested/deep/x.bin"}
    assert selected == {"keep.txt"}


def test_weekly_triggers_only_on_listed_days(tmp_path):
    mount = tmp_path / "files"
    make_tree(mount, ["a.txt"])
    # 2025-09-10 is a Wednesday; 2025-09-15 is a Monday.
    schedule = """
version: 1
timezone: UTC
jobs:
  - id: j
    source: mount://
    exclude: []
    when:
      kind: weekly
      at: "03:30"
      days: ["mon"]
"""
    # Wednesday window -> not due
    events = parse_events(
        run_scheduler(tmp_path, schedule, "2025-09-10T03:30:00Z", mount, duration=0)
    )
    assert [e for e in events if e["event"] == "JOB_ELIGIBLE"] == []

    # Monday window (case-insensitive "mon") -> due
    events = parse_events(
        run_scheduler(tmp_path, schedule, "2025-09-15T03:30:00Z", mount, duration=0)
    )
    assert [e for e in events if e["event"] == "JOB_ELIGIBLE"]


def test_once_triggers_only_at_exact_timestamp(tmp_path):
    mount = tmp_path / "files"
    make_tree(mount, ["a.txt"])
    schedule = """
version: 1
timezone: UTC
jobs:
  - id: j
    source: mount://
    exclude: []
    when:
      kind: once
      at: "2025-09-10T03:30:00"
"""
    # exact timestamp -> due
    events = parse_events(
        run_scheduler(tmp_path, schedule, "2025-09-10T03:30:00Z", mount, duration=0)
    )
    eligible = [e for e in events if e["event"] == "JOB_ELIGIBLE"]
    assert eligible and eligible[0]["kind"] == "once"

    # a different day -> not due
    events = parse_events(
        run_scheduler(tmp_path, schedule, "2025-09-11T03:30:00Z", mount, duration=0)
    )
    assert [e for e in events if e["event"] == "JOB_ELIGIBLE"] == []


def test_now_local_reflects_schedule_timezone(tmp_path):
    mount = tmp_path / "files"
    make_tree(mount, ["a.txt"])
    # Asia/Kolkata is a fixed +05:30 offset (no DST) -> deterministic.
    schedule = """
version: 1
timezone: "Asia/Kolkata"
jobs:
  - id: j
    source: mount://
    exclude: []
    when:
      kind: daily
      at: "05:30"
"""
    # 2025-09-10T00:00:00Z == 2025-09-10T05:30:00+05:30 local
    events = parse_events(
        run_scheduler(tmp_path, schedule, "2025-09-10T00:00:00Z", mount, duration=0)
    )
    eligible = [e for e in events if e["event"] == "JOB_ELIGIBLE"]
    assert eligible, "daily job at local 05:30 should be due"
    assert eligible[0]["now_local"] == "2025-09-10T05:30:00+05:30"

    # And in UTC schedules now_local uses the 'Z' suffix.
    schedule_utc = """
version: 1
timezone: UTC
jobs:
  - id: j
    source: mount://
    exclude: []
    when:
      kind: daily
      at: "03:30"
"""
    events = parse_events(
        run_scheduler(tmp_path, schedule_utc, "2025-09-10T03:30:00Z", mount, duration=0)
    )
    eligible = [e for e in events if e["event"] == "JOB_ELIGIBLE"]
    assert eligible[0]["now_local"] == "2025-09-10T03:30:00Z"
