import json

import pytest

from cacheops.conf import settings
from cacheops.getset import cache_thing
from cacheops.redis import load_script, redis_client

from .models import User
from .utils import BaseTestCase


@pytest.fixture()
def base(db):
    case = BaseTestCase()
    case.setUp()
    yield
    case.tearDown()


def test_ttl(base):
    user = User.objects.create(username='Suor')
    qs = User.objects.cache(timeout=100).filter(pk=user.pk)
    list(qs)
    assert 90 <= redis_client.ttl(qs._cache_key()) <= 100
    assert redis_client.ttl(f'{qs._prefix}conj:auth_user:id={user.id}') > 100


def test_conjunction_fields_are_canonical(base):
    prefix = 'canonical:'
    table = 'tests_ordering'
    values = {
        'client_id': 10,
        'status': 'approved',
        'action': 'ship',
        'event': 'created',
    }
    first_order = ('client_id', 'status', 'action', 'event')
    second_order = ('client_id', 'status', 'event', 'action')
    first_key = prefix + 'q:first'
    second_key = prefix + 'q:second'

    cache_thing(
        prefix,
        first_key,
        'first',
        {table: [{field: values[field] for field in first_order}]},
        100,
    )
    cache_thing(
        prefix,
        second_key,
        'second',
        {table: [{field: values[field] for field in second_order}]},
        100,
    )

    scheme = 'action,client_id,event,status'
    conj_key = prefix + 'conj:' + table + ':action=ship&client_id=10&event=created&status=approved'
    assert redis_client.smembers(prefix + 'schemes:' + table) == {scheme.encode()}
    if settings.CACHEOPS_INSIDEOUT:
        assert redis_client.get(conj_key) is not None
    else:
        assert redis_client.smembers(conj_key) == {first_key.encode(), second_key.encode()}


def test_invalidation_supports_legacy_conjunction_order(base):
    prefix = 'legacy:'
    table = 'tests_ordering'
    values = {
        'client_id': 10,
        'status': 'approved',
        'action': 'ship',
        'event': 'created',
    }
    canonical_scheme = 'action,client_id,event,status'
    legacy_scheme = 'status,event,action,client_id'

    def conj_key(scheme):
        parts = '&'.join(f'{field}={values[field]}' for field in scheme.split(','))
        return prefix + 'conj:' + table + ':' + parts

    canonical_query_key = prefix + 'q:canonical'
    legacy_query_key = prefix + 'q:legacy'
    canonical_conj_key = conj_key(canonical_scheme)
    legacy_conj_key = conj_key(legacy_scheme)
    redis_client.mset({canonical_query_key: b'canonical', legacy_query_key: b'legacy'})
    redis_client.sadd(prefix + 'schemes:' + table, canonical_scheme, legacy_scheme)
    redis_client.sadd(canonical_conj_key, canonical_query_key)
    redis_client.sadd(legacy_conj_key, legacy_query_key)

    load_script('invalidate')(keys=[prefix], args=[table, json.dumps(values)])

    assert redis_client.mget(canonical_query_key, legacy_query_key) == [None, None]
    assert redis_client.exists(canonical_conj_key, legacy_conj_key) == 0
