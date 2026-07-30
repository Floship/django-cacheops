import inspect
import json
import math
from io import StringIO
from unittest.mock import patch
from uuid import uuid4

import pytest
from django.core.management import call_command
from django.db import DEFAULT_DB_ALIAS
from redis.exceptions import ResponseError

from cacheops import reaper
from cacheops.redis import redis_client


@pytest.fixture()
def prefix(monkeypatch):
    value = f"fp17970:{uuid4().hex}:"
    monkeypatch.setattr(reaper, "get_prefix", lambda dbs: value)
    yield value
    keys = list(redis_client.scan_iter(f"{value}*"))
    if keys:
        redis_client.delete(*keys)


def add_conj(prefix, name, *members):
    key = f"{prefix}conj:{name}"
    redis_client.sadd(key, *members)
    return key


def result(**overrides):
    value = {
        "sets_scanned": 1,
        "sets_eligible": 1,
        "sets_processed": 1,
        "members_scanned": 1,
        "stale_members_found": 1,
        "stale_members_removed": 1,
        "elapsed_seconds": 0.01,
        "stopped_reason": "completed",
        "errors": [],
    }
    value.update(overrides)
    return value


def reap(**kwargs):
    return reaper.reap_conjs(
        chunk_size=2,
        min_conj_set_size=1,
        max_conj_sets=10,
        max_members=100,
        time_budget_seconds=10,
        **kwargs,
    )


def test_public_signature_keeps_legacy_parameters_and_adds_finite_defaults():
    parameters = inspect.signature(reaper.reap_conjs).parameters

    assert list(parameters) == [
        "chunk_size",
        "min_conj_set_size",
        "using",
        "dry_run",
        "max_conj_sets",
        "max_members",
        "time_budget_seconds",
    ]
    assert parameters["chunk_size"].default == 1000
    assert parameters["min_conj_set_size"].default == 1000
    assert parameters["using"].default == DEFAULT_DB_ALIAS
    assert parameters["dry_run"].default is False
    assert parameters["max_conj_sets"].default == 1
    assert parameters["max_members"].default == 1000
    assert parameters["time_budget_seconds"].default == 1


def test_write_removes_missing_and_preserves_falsey_present_members(prefix):
    stale = f"{prefix}q:stale"
    empty = f"{prefix}q:empty"
    zero = f"{prefix}q:zero"
    conj = add_conj(prefix, "mixed", stale, empty, zero)
    redis_client.set(empty, b"")
    redis_client.set(zero, b"0")

    reaped = reap()

    assert redis_client.smembers(conj) == {empty.encode(), zero.encode()}
    assert reaped["members_scanned"] == 3
    assert reaped["stale_members_found"] == 1
    assert reaped["stale_members_removed"] == 1
    assert reaped["stopped_reason"] == "completed"


def test_atomic_primitive_preserves_recreated_query_membership(prefix):
    query = f"{prefix}q:recreated"
    conj = add_conj(prefix, "race", query)
    assert redis_client.mget([query]) == [None]

    redis_client.set(query, b"live")
    redis_client.sadd(conj, query)
    found, removed = reaper._reap_chunk_atomic(conj, [query])

    assert (found, removed) == (0, 0)
    assert redis_client.sismember(conj, query)


def test_dry_run_estimates_without_mutation(prefix, monkeypatch):
    stale = f"{prefix}q:stale"
    live = f"{prefix}q:live"
    conj = add_conj(prefix, "dry", stale, live)
    redis_client.set(live, b"")
    monkeypatch.setattr(
        reaper,
        "_reap_chunk_atomic",
        lambda *args, **kwargs: pytest.fail("dry-run called write primitive"),
        raising=False,
    )

    reaped = reap(dry_run=True)

    assert redis_client.smembers(conj) == {stale.encode(), live.encode()}
    assert reaped["stale_members_found"] == 1
    assert reaped["stale_members_removed"] == 0


def test_member_budget_slices_sscan_count_overrun(prefix):
    members = [f"{prefix}q:{index}" for index in range(3)]
    add_conj(prefix, "count-hint", *members)

    with (
        patch.object(
            reaper.redis_client,
            "sscan",
            return_value=(0, [member.encode() for member in members]),
        ),
        patch.object(
            reaper.redis_client,
            "exists",
            return_value=0,
        ) as exists,
    ):
        reaped = reaper.reap_conjs(
            chunk_size=1,
            min_conj_set_size=1,
            dry_run=True,
            max_conj_sets=1,
            max_members=2,
            time_budget_seconds=10,
        )

    assert len(exists.call_args.args) == 2
    assert reaped["members_scanned"] == 2
    assert reaped["stopped_reason"] == "max_members"


def test_eligible_set_budget_stops_independently(prefix):
    add_conj(prefix, "small", f"{prefix}q:small")
    add_conj(prefix, "one", f"{prefix}q:one-a", f"{prefix}q:one-b")
    add_conj(prefix, "two", f"{prefix}q:two-a", f"{prefix}q:two-b")

    reaped = reaper.reap_conjs(
        chunk_size=2,
        min_conj_set_size=2,
        max_conj_sets=1,
        max_members=100,
        time_budget_seconds=10,
    )

    assert reaped["sets_eligible"] == 2
    assert reaped["sets_processed"] == 1
    assert reaped["stopped_reason"] == "max_conj_sets"


def test_total_member_budget_stops_independently(prefix):
    members = [f"{prefix}q:{index}" for index in range(3)]
    conj = add_conj(prefix, "members", *members)

    reaped = reaper.reap_conjs(
        chunk_size=10,
        min_conj_set_size=1,
        max_conj_sets=10,
        max_members=2,
        time_budget_seconds=10,
    )

    assert redis_client.scard(conj) == 1
    assert reaped["members_scanned"] == 2
    assert reaped["stale_members_removed"] == 2
    assert reaped["stopped_reason"] == "max_members"


def test_zero_time_budget_stops_before_redis_scan(prefix):
    add_conj(prefix, "time", f"{prefix}q:one")

    reaped = reaper.reap_conjs(time_budget_seconds=0)

    assert reaped["sets_scanned"] == 0
    assert reaped["stopped_reason"] == "time_budget"


def test_time_budget_stops_after_sscan_before_presence_check(prefix):
    query = f"{prefix}q:one"
    conj = add_conj(prefix, "time-during-scan", query)

    with patch.object(
        reaper,
        "_time_exhausted",
        side_effect=[False, False, False, False, True],
    ):
        reaped = reaper.reap_conjs(
            chunk_size=1,
            min_conj_set_size=1,
            dry_run=True,
            max_conj_sets=1,
            max_members=1,
            time_budget_seconds=1,
        )

    assert redis_client.sismember(conj, query)
    assert reaped["members_scanned"] == 0
    assert reaped["stopped_reason"] == "time_budget"


def test_provider_error_is_structured_without_message_or_key(prefix):
    add_conj(prefix, "error", f"{prefix}q:one")

    with patch.object(reaper.redis_client, "scard", side_effect=ResponseError("secret key detail")):
        reaped = reap()

    assert reaped["stopped_reason"] == "error"
    assert reaped["errors"] == [{"stage": "scard", "type": "ResponseError"}]
    assert "secret" not in json.dumps(reaped)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("max_conj_sets", None),
        ("max_members", None),
        ("time_budget_seconds", math.inf),
        ("max_members", -1),
    ],
)
def test_rejects_values_that_disable_a_hard_ceiling(name, value):
    with pytest.raises(ValueError, match=name):
        reaper.reap_conjs(**{name: value})


def test_management_command_maps_budgets_and_prints_json():
    out = StringIO()
    expected = result()

    with patch(
        "cacheops.management.commands.reapconjs.reap_conjs",
        return_value=expected,
    ) as mocked:
        call_command(
            "reapconjs",
            chunk_size=20,
            min_conj_set_size=30,
            database_alias="analytics",
            dry_run=True,
            max_conj_sets=2,
            max_members=40,
            time_budget_seconds=3,
            stdout=out,
        )

    assert json.loads(out.getvalue()) == expected
    mocked.assert_called_once_with(
        chunk_size=20,
        min_conj_set_size=30,
        using="analytics",
        dry_run=True,
        max_conj_sets=2,
        max_members=40,
        time_budget_seconds=3,
    )
