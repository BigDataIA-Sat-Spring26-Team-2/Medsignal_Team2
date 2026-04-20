"""
tests/integration/test_redis.py — Redis live (integration) tests

Requires Redis running on localhost:6379.

Run: poetry run pytest tests/integration/test_redis.py -v -s -m live
"""

import pytest
from app.utils.redis_client import (
    signal_cache_key,
    brief_cache_key,
    cache_get,
    cache_set,
    cache_delete,
    invalidate_signals,
    invalidate_brief,
    set_queue_depth,
    get_queue_depth,
)


@pytest.mark.live
def test_redis_connection():
    """Redis must be reachable before any live test runs."""
    import redis
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    result = r.ping()
    assert result is True, "Redis is not running — start with: docker compose up -d redis"
    print("\nRedis connected — OK")


@pytest.mark.live
def test_cache_set_and_get():
    """Value stored in Redis must be retrievable."""
    key   = "medsignal:test:basic"
    value = {"drug": "dupilumab", "prr": 5.97, "cases": 214}

    cache_set(key, value, ttl=60)
    result = cache_get(key)

    assert result is not None
    assert result["drug"]  == "dupilumab"
    assert result["prr"]   == 5.97
    assert result["cases"] == 214
    print(f"\nStored and retrieved: {result}")

    cache_delete(key)


@pytest.mark.live
def test_cache_get_missing_key_returns_none():
    """Non-existent key must return None — not raise an exception."""
    result = cache_get("medsignal:test:does_not_exist_xyz")
    assert result is None
    print("\nMissing key returned None — OK")


@pytest.mark.live
def test_cache_delete():
    """Deleted key must not be retrievable."""
    key   = "medsignal:test:delete"
    value = {"test": True}

    cache_set(key, value, ttl=60)
    assert cache_get(key) is not None

    cache_delete(key)
    assert cache_get(key) is None
    print("\nKey deleted and confirmed gone — OK")


@pytest.mark.live
def test_cache_ttl_respected():
    """
    Value stored with TTL=2 must expire after 2 seconds.
    Confirms TTL is actually being set in Redis.
    """
    import time
    key   = "medsignal:test:ttl"
    value = {"expires": True}

    cache_set(key, value, ttl=2)
    assert cache_get(key) is not None

    print("\nWaiting 3 seconds for TTL to expire...")
    time.sleep(3)

    assert cache_get(key) is None
    print("Key expired correctly — OK")


@pytest.mark.live
def test_signal_cache_full_flow():
    """
    Simulates the full GET /signals cache flow:
        1. First call — cache miss, store mock data
        2. Second call — cache hit, return from Redis
        3. Invalidate — cache cleared
        4. Third call — cache miss again
    """
    priority = "P1"
    limit    = 200
    key      = signal_cache_key(priority, limit)

    cache_delete(key)

    result = cache_get(key)
    assert result is None
    print("\nStep 1 — cache miss confirmed")

    mock_signals = [
        {"drug_key": "dupilumab", "pt": "conjunctivitis", "prr": 5.97},
        {"drug_key": "bupropion", "pt": "completed suicide", "prr": 9.98},
    ]
    cache_set(key, mock_signals, ttl=300)

    cached = cache_get(key)
    assert cached is not None
    assert len(cached) == 2
    assert cached[0]["drug_key"] == "dupilumab"
    print(f"Step 2+3 — stored and retrieved {len(cached)} signals")

    invalidate_signals()
    assert cache_get(key) is None
    print("Step 4 — invalidate_signals() cleared cache")


@pytest.mark.live
def test_brief_cache_full_flow():
    """
    Simulates the full GET /signals/{drug}/{pt}/brief cache flow:
        1. Cache miss
        2. Store mock brief
        3. Cache hit
        4. Invalidate specific brief
        5. Cache miss again
    """
    drug_key = "dupilumab"
    pt       = "conjunctivitis"
    key      = brief_cache_key(drug_key, pt)

    cache_delete(key)

    assert cache_get(key) is None
    print(f"\nStep 1 — brief cache miss for {drug_key} x {pt}")

    mock_brief = {
        "drug_key"  : drug_key,
        "pt"        : pt,
        "stat_score": 0.82,
        "lit_score" : 0.94,
        "priority"  : "P1",
        "brief_text": "dupilumab shows elevated PRR for conjunctivitis.",
    }
    cache_set(key, mock_brief, ttl=600)

    cached = cache_get(key)
    assert cached is not None
    assert cached["priority"]   == "P1"
    assert cached["stat_score"] == 0.82
    print(f"Step 2+3 — brief stored and retrieved priority={cached['priority']}")

    invalidate_brief(drug_key, pt)
    assert cache_get(key) is None
    print("Step 4 — invalidate_brief() cleared only this brief")


@pytest.mark.live
def test_queue_depth_set_and_get():
    """
    Queue depth must be stored and retrieved correctly.
    Simulates what happens after every HITL decision.
    """
    set_queue_depth(42)
    depth = get_queue_depth()
    assert depth == 42
    print(f"\nQueue depth stored and retrieved: {depth}")


@pytest.mark.live
def test_queue_depth_updates():
    """Queue depth update must overwrite previous value."""
    set_queue_depth(100)
    assert get_queue_depth() == 100

    set_queue_depth(99)
    assert get_queue_depth() == 99
    print("\nQueue depth updated correctly: 100 → 99")


@pytest.mark.live
def test_invalidate_signals_clears_all_priorities():
    """
    invalidate_signals() must clear ALL priority filter keys
    not just one specific key.
    """
    cache_set(signal_cache_key(None,  200), [{"drug": "a"}], ttl=300)
    cache_set(signal_cache_key("P1",  200), [{"drug": "b"}], ttl=300)
    cache_set(signal_cache_key("P2",  200), [{"drug": "c"}], ttl=300)

    assert cache_get(signal_cache_key(None, 200)) is not None
    assert cache_get(signal_cache_key("P1", 200)) is not None
    assert cache_get(signal_cache_key("P2", 200)) is not None
    print("\nAll three priority keys stored")

    invalidate_signals()

    assert cache_get(signal_cache_key(None, 200)) is None
    assert cache_get(signal_cache_key("P1", 200)) is None
    assert cache_get(signal_cache_key("P2", 200)) is None
    print("All three priority keys cleared by invalidate_signals()")
