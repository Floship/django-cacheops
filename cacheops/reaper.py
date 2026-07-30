import logging
import math
import time

from django.db import DEFAULT_DB_ALIAS
from redis.exceptions import RedisError

from .redis import load_script, redis_client
from .sharding import get_prefix

logger = logging.getLogger(__name__)


def reap_conjs(
    chunk_size: int = 1000,
    min_conj_set_size: int = 1000,
    using=DEFAULT_DB_ALIAS,
    dry_run: bool = False,
    max_conj_sets: int = 1,
    max_members: int = 1000,
    time_budget_seconds: float = 1,
):
    """Remove stale query references from cacheops conjunction sets within hard limits.

    Cacheops records cached query keys in conjunction sets for invalidation. Query keys
    expire independently, so stale references can accumulate and make later invalidations
    block Redis. This reaper removes only references whose query keys are absent.
    """
    _validate_limits(
        chunk_size=chunk_size,
        min_conj_set_size=min_conj_set_size,
        max_conj_sets=max_conj_sets,
        max_members=max_members,
        time_budget_seconds=time_budget_seconds,
    )
    started = time.monotonic()
    deadline = started + time_budget_seconds
    stats = {
        "sets_scanned": 0,
        "sets_eligible": 0,
        "sets_processed": 0,
        "members_scanned": 0,
        "stale_members_found": 0,
        "stale_members_removed": 0,
        "elapsed_seconds": 0,
        "stopped_reason": "completed",
        "errors": [],
    }

    if _time_exhausted(deadline):
        stats["stopped_reason"] = "time_budget"
        return _finish(stats, started)

    prefix = get_prefix(dbs=[using])
    try:
        for conj_key in redis_client.scan_iter(prefix + "conj:*", count=chunk_size):
            if _time_exhausted(deadline):
                stats["stopped_reason"] = "time_budget"
                break

            stats["sets_scanned"] += 1
            try:
                total = redis_client.scard(conj_key)
            except RedisError as exc:
                _record_error(stats, "scard", exc)
                break

            if _time_exhausted(deadline):
                stats["stopped_reason"] = "time_budget"
                break
            if total < min_conj_set_size:
                continue

            stats["sets_eligible"] += 1
            if stats["sets_processed"] >= max_conj_sets:
                stats["stopped_reason"] = "max_conj_sets"
                break

            remaining_members = max_members - stats["members_scanned"]
            if remaining_members <= 0:
                stats["stopped_reason"] = "max_members"
                break

            stats["sets_processed"] += 1
            outcome = _clear_conj_key(
                conj_key,
                chunk_size=chunk_size,
                max_members=remaining_members,
                deadline=deadline,
                dry_run=dry_run,
            )
            for name in ("members_scanned", "stale_members_found", "stale_members_removed"):
                stats[name] += outcome[name]
            stats["errors"].extend(outcome["errors"])
            if outcome["stopped_reason"] != "completed":
                stats["stopped_reason"] = outcome["stopped_reason"]
                break
    except RedisError as exc:
        _record_error(stats, "scan", exc)

    return _finish(stats, started)


def _clear_conj_key(conj_key, *, chunk_size, max_members, deadline, dry_run):
    stats = {
        "members_scanned": 0,
        "stale_members_found": 0,
        "stale_members_removed": 0,
        "stopped_reason": "completed",
        "errors": [],
    }
    cursor = 0

    while True:
        if _time_exhausted(deadline):
            stats["stopped_reason"] = "time_budget"
            break

        try:
            cursor, members = redis_client.sscan(conj_key, cursor, count=chunk_size)
        except RedisError as exc:
            _record_error(stats, "sscan", exc)
            break

        if _time_exhausted(deadline):
            stats["stopped_reason"] = "time_budget"
            break

        remaining = max_members - stats["members_scanned"]
        batch = members[:remaining]
        count_overrun = len(members) > len(batch)

        if batch:
            try:
                if dry_run:
                    found = len(batch) - redis_client.exists(*batch)
                    removed = 0
                else:
                    found, removed = _reap_chunk_atomic(conj_key, batch)
            except RedisError as exc:
                _record_error(stats, "exists" if dry_run else "atomic_reap", exc)
                break

            stats["members_scanned"] += len(batch)
            stats["stale_members_found"] += found
            stats["stale_members_removed"] += removed

        if _time_exhausted(deadline):
            stats["stopped_reason"] = "time_budget"
            break
        if count_overrun or (stats["members_scanned"] >= max_members and cursor != 0):
            stats["stopped_reason"] = "max_members"
            break
        if cursor == 0:
            break

    return stats


def _reap_chunk_atomic(conj_key, members):
    found, removed = load_script("reap_conjs")(keys=[conj_key, *members])
    return found, removed


def _validate_limits(**limits):
    for name in ("chunk_size", "min_conj_set_size", "max_conj_sets", "max_members"):
        value = limits[name]
        minimum = 1 if name == "chunk_size" else 0
        if not isinstance(value, int) or value < minimum:
            raise ValueError(f"{name} must be an integer >= {minimum}")

    time_budget = limits["time_budget_seconds"]
    if (
        not isinstance(time_budget, (int, float))
        or not math.isfinite(time_budget)
        or time_budget < 0
    ):
        raise ValueError("time_budget_seconds must be a finite number >= 0")


def _time_exhausted(deadline):
    return time.monotonic() >= deadline


def _record_error(stats, stage, exc):
    stats["errors"].append({"stage": stage, "type": type(exc).__name__})
    stats["stopped_reason"] = "error"
    logger.error(
        "Cacheops conjunction reaper Redis error at %s (%s)",
        stage,
        type(exc).__name__,
    )


def _finish(stats, started):
    stats["elapsed_seconds"] = round(time.monotonic() - started, 6)
    logger.info(
        "Cacheops conjunction reaper finished: "
        "sets=%s members=%s stale=%s removed=%s reason=%s errors=%s",
        stats["sets_processed"],
        stats["members_scanned"],
        stats["stale_members_found"],
        stats["stale_members_removed"],
        stats["stopped_reason"],
        len(stats["errors"]),
    )
    return stats
