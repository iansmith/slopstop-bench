#!/usr/bin/env python3
"""Single-run, CLI-driven backup scheduler (simulation only).

Parses a YAML schedule, decides which jobs are due within a time window, walks
each due job's source tree applying glob exclusion rules, and emits an event
history as JSON Lines on stdout.

See the BENCH-1 ticket for the full specification.
"""

import argparse
import json
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

MOUNT_PREFIX = "mount://"

_WEEKDAYS = {
    "mon": 0,
    "tue": 1,
    "wed": 2,
    "thu": 3,
    "fri": 4,
    "sat": 5,
    "sun": 6,
}


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #
def emit(obj):
    """Write one event as a compact JSON object on its own line."""
    sys.stdout.write(json.dumps(obj, separators=(",", ":")) + "\n")


def iso_with_offset(dt):
    """ISO 8601 with a numeric offset, using 'Z' for UTC."""
    s = dt.isoformat()
    if s.endswith("+00:00"):
        s = s[:-6] + "Z"
    return s


# --------------------------------------------------------------------------- #
# Time
# --------------------------------------------------------------------------- #
def parse_now(raw):
    """Parse the --now wall clock into an aware UTC datetime, floored to minute."""
    s = raw.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.replace(second=0, microsecond=0)


def floor_minute(dt):
    return dt.replace(second=0, microsecond=0)


# --------------------------------------------------------------------------- #
# Glob matching
# --------------------------------------------------------------------------- #
def glob_to_regex(pattern):
    """Translate a glob pattern into an anchored, case-sensitive regex.

    Semantics (POSIX-style '/' paths):
      *   -> any run of non-'/' characters
      ?   -> a single non-'/' character
      **  -> any characters, may cross '/'; '**/' may also match zero directories
      [..]-> character class (with leading '!' negation)
    """
    i = 0
    n = len(pattern)
    out = []
    while i < n:
        c = pattern[i]
        if c == "*":
            if i + 1 < n and pattern[i + 1] == "*":
                # '**'
                if i + 2 < n and pattern[i + 2] == "/":
                    # '**/' matches zero or more leading path segments
                    out.append("(?:.*/)?")
                    i += 3
                else:
                    out.append(".*")
                    i += 2
            else:
                out.append("[^/]*")
                i += 1
        elif c == "?":
            out.append("[^/]")
            i += 1
        elif c == "[":
            j = i + 1
            if j < n and pattern[j] in ("!", "^"):
                j += 1
            if j < n and pattern[j] == "]":
                j += 1
            while j < n and pattern[j] != "]":
                j += 1
            if j >= n:
                # No closing bracket: treat '[' literally.
                out.append(re.escape("["))
                i += 1
            else:
                inner = pattern[i + 1:j]
                if inner.startswith("!"):
                    inner = "^" + inner[1:]
                out.append("[" + inner + "]")
                i = j + 1
        elif c == "/":
            out.append("/")
            i += 1
        else:
            out.append(re.escape(c))
            i += 1
    return re.compile("^" + "".join(out) + "$")


def build_matchers(patterns):
    return [(pat, glob_to_regex(pat)) for pat in patterns]


def first_matching_pattern(path, matchers):
    """Return the first pattern (list order) that matches, else None."""
    for pat, rx in matchers:
        if rx.match(path):
            return pat
    return None


# --------------------------------------------------------------------------- #
# Scheduling
# --------------------------------------------------------------------------- #
def job_is_due(when, tz, window_start, window_end):
    """Decide whether a job triggers at least once within [start, end] inclusive."""
    kind = when.get("kind")

    if kind == "once":
        raw = when.get("at")
        candidate = floor_minute(datetime.fromisoformat(raw).replace(tzinfo=tz))
        return window_start <= candidate <= window_end

    if kind in ("daily", "weekly"):
        hh, mm = (int(part) for part in str(when.get("at")).split(":"))
        allowed_days = None
        if kind == "weekly":
            allowed_days = {
                _WEEKDAYS[str(d)[:3].lower()] for d in (when.get("days") or [])
            }
        # Walk each local calendar date the window can touch.
        day = window_start.date()
        last = window_end.date()
        while day <= last:
            if allowed_days is None or day.weekday() in allowed_days:
                candidate = datetime(
                    day.year, day.month, day.day, hh, mm, tzinfo=tz
                )
                if window_start <= candidate <= window_end:
                    return True
            day += timedelta(days=1)
        return False

    return False


# --------------------------------------------------------------------------- #
# File selection
# --------------------------------------------------------------------------- #
def resolve_source_dir(source, mount):
    """Resolve a job's source (e.g. 'mount://A/B') to a filesystem directory."""
    rel = source[len(MOUNT_PREFIX):] if source.startswith(MOUNT_PREFIX) else source
    rel = rel.strip("/")
    return mount if rel == "" else mount / rel


def list_source_files(source_dir):
    """All files under source_dir, as source-relative POSIX paths, sorted."""
    if not source_dir.exists():
        return []
    files = [
        p.relative_to(source_dir).as_posix()
        for p in source_dir.rglob("*")
        if p.is_file()
    ]
    files.sort()
    return files


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def run(args):
    schedule = yaml_load(args.schedule)
    timezone = schedule.get("timezone") or "UTC"
    jobs = schedule.get("jobs") or []
    tz = ZoneInfo(timezone)

    emit({"event": "SCHEDULE_PARSED", "timezone": timezone, "jobs_total": len(jobs)})

    now_utc = parse_now(args.now)
    now_local = floor_minute(now_utc.astimezone(tz))
    window_start = now_local
    window_end = now_local + timedelta(hours=args.duration)
    now_local_str = iso_with_offset(now_local)
    mount = Path(args.mount)

    due = []
    for job in jobs:
        if not job.get("enabled", True):
            continue
        when = job.get("when") or {}
        if job_is_due(when, tz, window_start, window_end):
            due.append(job)

    due.sort(key=lambda j: j["id"])

    for job in due:
        job_id = job["id"]
        when = job.get("when") or {}
        exclude = job.get("exclude") or []
        matchers = build_matchers(exclude)

        emit({
            "event": "JOB_ELIGIBLE",
            "job_id": job_id,
            "kind": when.get("kind"),
            "now_local": now_local_str,
        })
        emit({
            "event": "JOB_STARTED",
            "job_id": job_id,
            "exclude_count": len(exclude),
        })

        source_dir = resolve_source_dir(job.get("source", MOUNT_PREFIX), mount)
        selected = 0
        excluded = 0
        for rel_path in list_source_files(source_dir):
            pattern = first_matching_pattern(rel_path, matchers)
            if pattern is not None:
                emit({
                    "event": "FILE_EXCLUDED",
                    "job_id": job_id,
                    "path": rel_path,
                    "pattern": pattern,
                })
                excluded += 1
            else:
                emit({
                    "event": "FILE_SELECTED",
                    "job_id": job_id,
                    "path": rel_path,
                })
                selected += 1

        emit({
            "event": "JOB_COMPLETED",
            "job_id": job_id,
            "selected": selected,
            "excluded": excluded,
        })


def yaml_load(path):
    import yaml

    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Single-run backup scheduler (simulation)."
    )
    parser.add_argument("--schedule", required=True, help="Path to YAML schedule file.")
    parser.add_argument(
        "--now", required=True, help="Wall clock (RFC 3339 / ISO-8601, e.g. 2025-09-10T13:45:00Z)."
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=24.0,
        help="Hours to simulate (inclusive bound). Default 24. 0 = only the --now minute.",
    )
    parser.add_argument(
        "--mount", required=True, help="Path treated as the 'mount://' root."
    )
    args = parser.parse_args(argv)
    run(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
