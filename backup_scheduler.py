#!/usr/bin/env python3
"""Single-run, CLI-driven backup scheduler.

Parses a YAML schedule, decides which jobs are *due* within a time window,
simulates running each due job by applying its glob ``exclude`` rules to the
source file tree, and emits the resulting event history as JSON Lines on stdout.

No backup is ever written or copied: the deliverable is the deterministic event
stream. A job's ``strategy`` (``full``/``verify``/``pack``) does read each
selected file to report its real size and SHA-256 — and, for ``pack``, to build
the archive bytes — but nothing is persisted.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import sys
import tarfile
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import yaml

MOUNT_PREFIX = "mount://"
DEFAULT_MAX_PACK_BYTES = 1048576

# Python's date.weekday(): Monday == 0 ... Sunday == 6.
WEEKDAYS = {
    "mon": 0,
    "tue": 1,
    "wed": 2,
    "thu": 3,
    "fri": 4,
    "sat": 5,
    "sun": 6,
}


# --------------------------------------------------------------------------- #
# Glob exclusion engine
# --------------------------------------------------------------------------- #
def _glob_to_regex(pattern: str) -> "re.Pattern[str]":
    """Compile a backup-spec glob into an anchored, case-sensitive regex.

    Semantics (POSIX-style ``/`` paths):
      ``*``   matches any run of characters except ``/``
      ``?``   matches a single character except ``/``
      ``**``  matches any run of characters including ``/``
      ``**/`` matches zero or more leading directories, so ``**/x`` matches ``x``
      ``[]``  a character class; a leading ``!`` negates it
    """
    i, n = 0, len(pattern)
    out: list[str] = []
    while i < n:
        char = pattern[i]
        if char == "*":
            if i + 1 < n and pattern[i + 1] == "*":
                if i + 2 < n and pattern[i + 2] == "/":
                    out.append("(?:.*/)?")  # '**/' may match zero directories
                    i += 3
                else:
                    out.append(".*")
                    i += 2
            else:
                out.append("[^/]*")
                i += 1
        elif char == "?":
            out.append("[^/]")
            i += 1
        elif char == "[":
            i = _consume_class(pattern, i, out)
        else:
            out.append(re.escape(char))
            i += 1
    return re.compile("^" + "".join(out) + "$")


def _consume_class(pattern: str, start: int, out: list[str]) -> int:
    """Translate a ``[...]`` character class starting at ``start``; return next index."""
    n = len(pattern)
    j = start + 1
    if j < n and pattern[j] in ("!", "^"):
        j += 1
    if j < n and pattern[j] == "]":  # a ']' right after '[' is a literal member
        j += 1
    while j < n and pattern[j] != "]":
        j += 1
    if j >= n:  # unterminated class -> treat '[' as a literal
        out.append(re.escape("["))
        return start + 1
    inner = pattern[start + 1:j]
    if inner.startswith("!"):
        inner = "^" + inner[1:]
    out.append("[" + inner + "]")
    return j + 1


def _first_match(path: str, compiled):
    """Return the first pattern (list order) whose regex matches ``path``, else None."""
    for pattern, regex in compiled:
        if regex.match(path):
            return pattern
    return None


# --------------------------------------------------------------------------- #
# Scheduling
# --------------------------------------------------------------------------- #
def _parse_now(value: str) -> datetime:
    """Parse ``--now`` to a tz-aware datetime floored to the minute (UTC if naive)."""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.replace(second=0, microsecond=0)


def _format_local(dt: datetime, tz: ZoneInfo) -> str:
    """ISO 8601 of ``dt`` in ``tz``; UTC is rendered with ``Z`` rather than ``+00:00``."""
    text = dt.astimezone(tz).isoformat()
    return text[:-6] + "Z" if text.endswith("+00:00") else text


def _parse_hhmm(value: str) -> tuple[int, int]:
    hour, minute = value.split(":")
    return int(hour), int(minute)


def _allowed_weekdays(when: dict):
    """Set of weekday numbers for a weekly job, or None for non-weekly jobs."""
    if when["kind"] != "weekly":
        return None
    days = (str(d).strip().lower()[:3] for d in when.get("days", []))
    return {WEEKDAYS[d] for d in days if d in WEEKDAYS}


def _is_due(when: dict, tz: ZoneInfo, w_start: datetime, w_end: datetime) -> bool:
    """True if any of the job's trigger instants falls within [w_start, w_end]."""
    if when["kind"] == "once":
        return _once_due(when, tz, w_start, w_end)
    return _recurring_due(when, tz, w_start, w_end)


def _once_due(when: dict, tz: ZoneInfo, w_start: datetime, w_end: datetime) -> bool:
    dt = datetime.fromisoformat(when["at"])
    if dt.tzinfo is None:  # spec: 'once' timestamps carry no tz; interpret in schedule tz
        dt = dt.replace(tzinfo=tz)
    trigger = dt.replace(second=0, microsecond=0)
    return w_start <= trigger <= w_end


def _recurring_due(when: dict, tz: ZoneInfo, w_start: datetime, w_end: datetime) -> bool:
    hour, minute = _parse_hhmm(when["at"])
    weekdays = _allowed_weekdays(when)
    day = w_start.astimezone(tz).date()
    last = w_end.astimezone(tz).date()
    step = timedelta(days=1)
    while day <= last:
        if weekdays is None or day.weekday() in weekdays:
            # A DST fall-back makes the wall time ambiguous (two UTC instants);
            # treat the job as due if either instant lands in the window so a
            # scheduled backup is never silently skipped on the repeated hour.
            for fold in (0, 1):
                trigger = datetime(day.year, day.month, day.day, hour, minute, tzinfo=tz, fold=fold)
                if w_start <= trigger <= w_end:
                    return True
        day += step
    return False


# --------------------------------------------------------------------------- #
# Job execution
# --------------------------------------------------------------------------- #
def _source_dir(mount: str, source: str) -> str:
    """Resolve a job's ``mount://`` source to a filesystem directory under ``mount``."""
    sub = source[len(MOUNT_PREFIX):] if source.startswith(MOUNT_PREFIX) else source
    sub = sub.strip("/")
    return os.path.join(mount, sub) if sub else mount


def _list_files(source_dir: str) -> list[str]:
    """All files under ``source_dir`` as POSIX paths relative to it, lexicographically sorted."""
    found: list[str] = []
    for dirpath, _dirnames, filenames in os.walk(source_dir):
        for name in filenames:
            rel = os.path.relpath(os.path.join(dirpath, name), source_dir)
            found.append(rel.replace(os.sep, "/"))
    found.sort()
    return found


# --------------------------------------------------------------------------- #
# Backup strategies
# --------------------------------------------------------------------------- #
def _read_file(source_dir: str, rel: str) -> bytes:
    """Read a selected file whole.

    Reading the entire file is acceptable here: this is a simulated single run
    over a small tree and the deliverable is the event stream, not a streaming
    copy.
    """
    with open(os.path.join(source_dir, rel), "rb") as handle:
        return handle.read()


def _sha256(content: bytes) -> str:
    """SHA-256 of ``content`` as the spec's ``sha256:{hex}`` string."""
    return "sha256:" + hashlib.sha256(content).hexdigest()


def _pack_tar_bytes(members) -> bytes:
    """Serialize ``members`` (ordered ``(arcname, content)``) to deterministic GNU-tar bytes.

    Every entry's metadata is normalized so the archive — and therefore its
    SHA-256 — depends only on the names, contents, and order. ``tarfile`` pads
    the closed archive up to its 10240-byte record size; those padding bytes are
    part of both the checksum and ``tar_size``.
    """
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.GNU_FORMAT) as tar:
        for arcname, content in members:
            info = tarfile.TarInfo(name=arcname)
            info.size = len(content)
            info.mtime = 0
            info.mode = 0o644
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            tar.addfile(info, io.BytesIO(content))
    return buf.getvalue()


class _NullStrategy:
    """No strategy declared: emit nothing extra and leave JOB_COMPLETED unchanged."""

    def handle(self, rel: str) -> None:
        pass

    def finalize(self) -> None:
        pass

    def summary(self) -> dict:
        return {}


class _PerFileStrategy:
    """``full`` / ``verify``: emit one event per selected file with its size and checksum."""

    def __init__(self, emit, job_id: str, source_dir: str, event_name: str):
        self._emit = emit
        self._job_id = job_id
        self._source_dir = source_dir
        self._event_name = event_name
        self._total_size = 0

    def handle(self, rel: str) -> None:
        content = _read_file(self._source_dir, rel)
        self._total_size += len(content)
        self._emit({
            "event": self._event_name,
            "job_id": self._job_id,
            "path": rel,
            "size": len(content),
            "checksum": _sha256(content),
        })

    def finalize(self) -> None:
        pass

    def summary(self) -> dict:
        return {"total_size": self._total_size}


class _PackStrategy:
    """``pack``: group selected files into sequential, size-limited GNU-tar archives."""

    def __init__(self, emit, job_id: str, source_dir: str, now_local: str, max_pack_bytes: int):
        self._emit = emit
        self._job_id = job_id
        self._source_dir = source_dir
        self._now_local = now_local
        self._max_pack_bytes = max_pack_bytes
        self._pack_index = 0
        self._pending = []        # (rel, content) accumulated in the currently open pack
        self._pending_size = 0
        self._packs = 0
        self._total_size = 0

    def handle(self, rel: str) -> None:
        content = _read_file(self._source_dir, rel)
        size = len(content)
        # A file is always packed; if it would overflow a non-empty open pack,
        # finalize that pack first so the file starts a fresh one.
        if self._pending and self._pending_size + size > self._max_pack_bytes:
            self._finalize_pack()
        if not self._pending:
            self._pack_index += 1
        self._pending.append((rel, content))
        self._pending_size += size
        self._total_size += size
        self._emit({
            "event": "FILE_PACKED",
            "job_id": self._job_id,
            "pack_id": self._pack_index,
            "path": rel,
            "size": size,
        })

    def _finalize_pack(self) -> None:
        tar = _pack_tar_bytes(self._pending)
        self._emit({
            "event": "PACK_CREATED",
            "job_id": self._job_id,
            "name": f"pack-{self._pack_index}.tar",
            "size": self._pending_size,
            "timestamp": self._now_local,
            "checksum": _sha256(tar),
            "tar_size": len(tar),
        })
        self._packs += 1
        self._pending = []
        self._pending_size = 0

    def finalize(self) -> None:
        if self._pending:
            self._finalize_pack()

    def summary(self) -> dict:
        return {"packs": self._packs, "total_size": self._total_size}


_PER_FILE_EVENTS = {"full": "FILE_BACKED_UP", "verify": "FILE_VERIFIED"}


def _build_strategy(job: dict, emit, job_id: str, source_dir: str, now_local: str):
    """Construct the job's strategy, emitting STRATEGY_SELECTED when one is declared.

    A job with no ``strategy`` block gets a no-op strategy so the checkpoint-1
    event stream is reproduced byte-for-byte.
    """
    strategy = job.get("strategy")
    if not strategy:
        return _NullStrategy()
    kind = strategy.get("kind")
    options = strategy.get("options") or {}
    emit({"event": "STRATEGY_SELECTED", "job_id": job_id, "kind": kind})
    if kind == "pack":
        max_pack_bytes = options.get("max_pack_bytes", DEFAULT_MAX_PACK_BYTES)
        return _PackStrategy(emit, job_id, source_dir, now_local, max_pack_bytes)
    return _PerFileStrategy(emit, job_id, source_dir, _PER_FILE_EVENTS.get(kind, "FILE_BACKED_UP"))


def _run_job(job: dict, mount: str, now_local: str, emit) -> None:
    job_id = job["id"]
    emit({"event": "JOB_ELIGIBLE", "job_id": job_id, "kind": job["when"]["kind"], "now_local": now_local})
    exclude = job.get("exclude") or []
    compiled = [(p, _glob_to_regex(p)) for p in exclude]
    emit({"event": "JOB_STARTED", "job_id": job_id, "exclude_count": len(exclude)})

    source_dir = _source_dir(mount, job.get("source", MOUNT_PREFIX))
    strategy = _build_strategy(job, emit, job_id, source_dir, now_local)

    selected = excluded = 0
    for rel in _list_files(source_dir):
        pattern = _first_match(rel, compiled)
        if pattern is not None:
            emit({"event": "FILE_EXCLUDED", "job_id": job_id, "path": rel, "pattern": pattern})
            excluded += 1
            continue
        emit({"event": "FILE_SELECTED", "job_id": job_id, "path": rel})
        selected += 1
        strategy.handle(rel)
    strategy.finalize()

    completed = {"event": "JOB_COMPLETED", "job_id": job_id, "selected": selected, "excluded": excluded}
    completed.update(strategy.summary())
    emit(completed)


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def _parse_args(argv):
    parser = argparse.ArgumentParser(description="Single-run YAML backup scheduler.")
    parser.add_argument("--schedule", required=True, help="Path to the YAML schedule file.")
    parser.add_argument("--now", required=True, help="Wall clock (ISO 8601 / RFC 3339).")
    parser.add_argument("--duration", type=float, default=24.0,
                        help="Window length in hours (inclusive). Default 24.")
    parser.add_argument("--mount", required=True, help="Filesystem path treated as the mount:// root.")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv)
    with open(args.schedule, "r", encoding="utf-8") as handle:
        schedule = yaml.safe_load(handle) or {}

    tz_name = schedule.get("timezone") or "UTC"
    tz = ZoneInfo(tz_name)
    jobs = schedule.get("jobs") or []

    now = _parse_now(args.now)
    w_start, w_end = now, now + timedelta(hours=args.duration)
    now_local = _format_local(now, tz)

    def emit(obj: dict) -> None:
        sys.stdout.write(json.dumps(obj, separators=(",", ":")) + "\n")

    emit({"event": "SCHEDULE_PARSED", "timezone": tz_name, "jobs_total": len(jobs)})

    # Only an explicit `enabled: false` disables a job; a missing or null value
    # falls back to the spec default (enabled).
    due = [
        j for j in jobs
        if j.get("enabled", True) is not False and _is_due(j["when"], tz, w_start, w_end)
    ]
    due.sort(key=lambda job: job["id"])
    for job in due:
        _run_job(job, args.mount, now_local, emit)
    return 0


if __name__ == "__main__":
    sys.exit(main())
