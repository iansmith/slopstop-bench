#!/usr/bin/env python3
"""Single-run, CLI-driven backup scheduler (simulation only).

Parses a YAML schedule, decides which jobs are due within a time window, walks
each due job's source tree applying glob exclusion rules, and emits an event
history as JSON Lines on stdout.

See the BENCH-1 ticket for the full specification.
"""

import argparse
import hashlib
import io
import json
import re
import sys
import tarfile
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

DEFAULT_MAX_PACK_BYTES = 1048576

MOUNT_PREFIX = "mount://"
BACKUP_PREFIX = "backup://"

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


def resolve_dest_dir(destination, backup_root, job_id):
    """Resolve a job's backup directory: <backup_root>/<dest_rel>/<job_id>.

    Mirrors resolve_source_dir for the 'backup://' scheme. Returns None when no
    backup root was given (--backup omitted) or the job declares no destination,
    in which case the caller treats the job as having no existing backups.
    """
    if backup_root is None or not destination:
        return None
    rel = (
        destination[len(BACKUP_PREFIX):]
        if destination.startswith(BACKUP_PREFIX)
        else destination
    )
    rel = rel.strip("/")
    base = backup_root if rel == "" else backup_root / rel
    return base / job_id


def load_dest_state(dest_dir):
    """Map source-relative path -> content digest for existing backups, or {}.

    Tolerates a missing directory. Reuses list_source_files so the relative-path
    convention matches the source walk exactly.
    """
    state = {}
    if dest_dir is None:
        return state
    for rel in list_source_files(dest_dir):
        state[rel] = content_digest((dest_dir / rel).read_bytes())
    return state


def load_dest_packs(dest_dir):
    """Map pack name -> loaded-pack info for existing pack-*.tar files, or {}.

    Backs the pack strategy's incremental mode. Reads each `pack-N.tar` directly
    under dest_dir (top level only), in numeric pack order, returning each pack's
    per-member content hashes, the SHA-256 of the raw tar file, the member count,
    and the summed member size. Tolerates a missing directory.
    """
    packs = {}
    if dest_dir is None or not dest_dir.exists():
        return packs

    def pack_index(path):
        m = re.match(r"pack-(\d+)\.tar$", path.name)
        return int(m.group(1)) if m else -1

    pack_paths = sorted(
        (p for p in dest_dir.glob("pack-*.tar") if p.is_file() and pack_index(p) >= 0),
        key=pack_index,
    )
    for path in pack_paths:
        raw = path.read_bytes()
        member_hashes = {}
        size = 0
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r") as tar:
            for info in tar.getmembers():
                if not info.isfile():
                    continue
                content = tar.extractfile(info).read()
                member_hashes[info.name] = content_digest(content)
                size += len(content)
        packs[path.name] = {
            "member_hashes": member_hashes,
            "checksum": content_digest(raw),
            "files_total": len(member_hashes),
            "size": size,
        }
    return packs


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
# Backup strategies
# --------------------------------------------------------------------------- #
def content_digest(data):
    """SHA-256 over the given bytes, tagged 'sha256:{hex}'."""
    return "sha256:" + hashlib.sha256(data).hexdigest()


def build_pack_tar(entries):
    """Build a deterministic GNU tar archive from (arcname, content) entries.

    Determinism requires fixed per-entry metadata, so each member is added via
    a hand-built TarInfo (never tarfile.add, which would stat the real file):
    mtime=0, mode=0o644, uid=0, gid=0, uname="", gname="". Returns the raw tar
    bytes (padded to a full record on close).
    """
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.GNU_FORMAT) as tar:
        for arcname, content in entries:
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
    backup_root = Path(args.backup) if args.backup else None

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

        # A job opts into backup operations via an optional `strategy` block.
        # Without it, behavior is identical to checkpoint 1.
        strategy = job.get("strategy")
        has_strategy = isinstance(strategy, dict)
        strat_kind = None
        options = {}
        if has_strategy:
            strat_kind = strategy.get("kind")
            options = strategy.get("options") or {}

        # Incremental backups (full strategy only): scan the destination for this
        # job's existing backups before selecting files. Pack never tracks state
        # (.tar archives aren't per-file), and a job without a destination keeps
        # checkpoint_2 behavior. DEST_STATE_LOADED is emitted between JOB_STARTED
        # and STRATEGY_SELECTED.
        destination = job.get("destination")
        dest_incremental = strat_kind == "full" and bool(destination)
        dest_state = {}
        if dest_incremental:
            dest_state = load_dest_state(
                resolve_dest_dir(destination, backup_root, job_id)
            )
        dest_state_files = len(dest_state)
        if dest_state_files:
            emit({
                "event": "DEST_STATE_LOADED",
                "job_id": job_id,
                "files_total": dest_state_files,
            })

        # Incremental pack backups: when a pack job points at a destination that
        # already holds pack-*.tar archives, load them so unchanged files and
        # unchanged packs can be detected. PACK_LOADED is emitted just below,
        # after STRATEGY_SELECTED and before any file work.
        pack_incremental = strat_kind == "pack" and bool(destination)
        loaded_packs = {}
        pack_member_hashes = {}
        if pack_incremental:
            loaded_packs = load_dest_packs(
                resolve_dest_dir(destination, backup_root, job_id)
            )
            for info in loaded_packs.values():
                pack_member_hashes.update(info["member_hashes"])

        if has_strategy:
            emit({
                "event": "STRATEGY_SELECTED",
                "job_id": job_id,
                "kind": strat_kind,
            })

        for name, info in loaded_packs.items():
            emit({
                "event": "PACK_LOADED",
                "job_id": job_id,
                "name": name,
                "files_total": info["files_total"],
                "checksum": info["checksum"],
            })

        source_dir = resolve_source_dir(job.get("source", MOUNT_PREFIX), mount)
        selected = 0
        excluded = 0
        total_size = 0
        files_skipped_unchanged = 0

        # Pack-strategy accumulators (untouched by other strategies).
        max_pack_bytes = options.get("max_pack_bytes", DEFAULT_MAX_PACK_BYTES)
        pack_members = []   # (rel_path, data) currently in the open pack
        pack_size = 0       # content bytes in the open pack
        pack_index = 0      # 1-based id of the open pack; names pack-{id}.tar
        packs_done = 0

        def finalize_pack():
            nonlocal pack_members, pack_size, packs_done
            name = "pack-{}.tar".format(pack_index)
            tar_bytes = build_pack_tar(pack_members)
            checksum = content_digest(tar_bytes)
            prior = loaded_packs.get(name)
            if prior is None:
                # No counterpart in the destination: a brand-new pack.
                emit({
                    "event": "PACK_CREATED",
                    "job_id": job_id,
                    "name": name,
                    "size": pack_size,
                    "timestamp": now_local_str,
                    "checksum": checksum,
                    "tar_size": len(tar_bytes),
                })
            elif checksum == prior["checksum"]:
                # Rebuilt bytes match the stored pack exactly: nothing to do.
                emit({
                    "event": "PACK_UNCHANGED",
                    "job_id": job_id,
                    "name": name,
                    "checksum": checksum,
                })
            else:
                # Contents shifted: rewrite, carrying the prior size/checksum.
                emit({
                    "event": "PACK_UPDATED",
                    "job_id": job_id,
                    "name": name,
                    "size": pack_size,
                    "checksum": checksum,
                    "timestamp": now_local_str,
                    "tar_size": len(tar_bytes),
                    "old_size": prior["size"],
                    "old_checksum": prior["checksum"],
                })
            packs_done += 1
            pack_members = []
            pack_size = 0

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
                continue

            emit({
                "event": "FILE_SELECTED",
                "job_id": job_id,
                "path": rel_path,
            })
            selected += 1

            if strat_kind is None:
                continue

            data = (source_dir / rel_path).read_bytes()
            size = len(data)

            if strat_kind == "full":
                digest = content_digest(data)
                # Incremental: skip a file whose backup already matches its hash.
                if rel_path in dest_state and dest_state[rel_path] == digest:
                    emit({
                        "event": "FILE_SKIPPED_UNCHANGED",
                        "job_id": job_id,
                        "path": rel_path,
                        "hash": digest,
                    })
                    files_skipped_unchanged += 1
                    continue
                total_size += size
                emit({
                    "event": "FILE_BACKED_UP",
                    "job_id": job_id,
                    "path": rel_path,
                    "size": size,
                    "checksum": digest,
                })
            elif strat_kind == "verify":
                total_size += size
                emit({
                    "event": "FILE_VERIFIED",
                    "job_id": job_id,
                    "path": rel_path,
                    "size": size,
                    "checksum": content_digest(data),
                })
            elif strat_kind == "pack":
                total_size += size
                # Finalize the open pack before a file that would overflow it.
                if pack_members and pack_size + size > max_pack_bytes:
                    finalize_pack()
                if not pack_members:
                    pack_index += 1
                # The file joins the rebuilt pack regardless of change status; the
                # event only distinguishes whether its bytes already match the
                # copy carried in the loaded pack (incremental mode).
                pack_members.append((rel_path, data))
                pack_size += size
                digest = content_digest(data)
                if pack_incremental and pack_member_hashes.get(rel_path) == digest:
                    emit({
                        "event": "PACK_SKIP_UNCHANGED",
                        "job_id": job_id,
                        "pack_id": pack_index,
                        "path": rel_path,
                        "size": size,
                        "hash": digest,
                    })
                else:
                    emit({
                        "event": "FILE_PACKED",
                        "job_id": job_id,
                        "pack_id": pack_index,
                        "path": rel_path,
                        "size": size,
                    })
                # A lone file that lands strictly over the cap closes its pack.
                if pack_size > max_pack_bytes:
                    finalize_pack()

        if strat_kind == "pack" and pack_members:
            finalize_pack()

        completed = {
            "event": "JOB_COMPLETED",
            "job_id": job_id,
            "selected": selected,
            "excluded": excluded,
        }
        if strat_kind is not None:
            completed["total_size"] = total_size
        if strat_kind == "pack":
            completed["packs"] = packs_done
        if dest_incremental:
            completed["files_skipped_unchanged"] = files_skipped_unchanged
            completed["dest_state_files"] = dest_state_files
        emit(completed)


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
    parser.add_argument(
        "--backup",
        default=None,
        help="Path treated as the 'backup://' root (may not yet exist). "
        "If omitted, assume no existing backups exist.",
    )
    args = parser.parse_args(argv)
    run(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
