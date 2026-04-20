"""
tests/unit/test_redis.py — Redis client unit tests

Pure key logic — no Redis connection needed.

Run: poetry run pytest tests/unit/test_redis.py -v -m unit
"""

import pytest
from app.utils.redis_client import (
    signal_cache_key,
    brief_cache_key,
)


@pytest.mark.unit
def test_signal_cache_key_no_priority():
    """No priority filter → key contains 'all'."""
    key = signal_cache_key(None, 200)
    assert "all" in key
    assert "200" in key
    assert "medsignal" in key


@pytest.mark.unit
def test_signal_cache_key_with_priority():
    """Priority filter → key contains priority value."""
    key = signal_cache_key("P1", 200)
    assert "P1" in key
    assert "200" in key


@pytest.mark.unit
def test_signal_cache_key_different_priorities_are_different():
    """P1 and P2 filters must produce different cache keys."""
    key_p1 = signal_cache_key("P1", 200)
    key_p2 = signal_cache_key("P2", 200)
    assert key_p1 != key_p2


@pytest.mark.unit
def test_signal_cache_key_different_limits_are_different():
    """Different limits must produce different cache keys."""
    key_200 = signal_cache_key(None, 200)
    key_50  = signal_cache_key(None, 50)
    assert key_200 != key_50


@pytest.mark.unit
def test_brief_cache_key_format():
    """Brief key must contain drug and reaction."""
    key = brief_cache_key("dupilumab", "conjunctivitis")
    assert "dupilumab" in key
    assert "conjunctivitis" in key
    assert "medsignal" in key


@pytest.mark.unit
def test_brief_cache_key_spaces_handled():
    """Spaces in pt must not break the key."""
    key = brief_cache_key("gabapentin", "cardio-respiratory arrest")
    assert " " not in key


@pytest.mark.unit
def test_brief_cache_key_different_signals_are_different():
    """Two different signals must have different cache keys."""
    key1 = brief_cache_key("dupilumab", "conjunctivitis")
    key2 = brief_cache_key("gabapentin", "cardio-respiratory arrest")
    assert key1 != key2
