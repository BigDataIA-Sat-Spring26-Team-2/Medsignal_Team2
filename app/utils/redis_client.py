"""
redis_client.py — MedSignal Redis utility

Purpose:
    Read-through cache for Snowflake queries that are called frequently
    during the demo and normal analyst usage.

Why Redis:
    Snowflake serverless warehouse has cold start latency of 3-8 seconds.
    Four Streamlit pages all query signals_flagged on load.
    Without caching the Signal Feed takes 8 seconds to render.
    Redis returns cached results in under 10ms.

Used in exactly two places:
    1. app/services/signal_service.py
           GET /signals         → cache_get / cache_set / invalidate_signals
           GET /signals/{}/brief → cache_get / cache_set
    2. app/observability/metrics.py
           Prometheus queue depth → set_queue_depth / get_queue_depth

Cache TTL:
    Signals   : 300 seconds (5 minutes)
    SafetyBrief: 600 seconds (10 minutes) — rarely changes after generation
    Queue depth: 60 seconds (1 minute) — updated after every HITL decision

Invalidation:
    Branch 2 re-run → invalidate_signals() clears all signal cache keys
    New SafetyBrief written → invalidate_brief(drug_key, pt) clears that key
    New HITL decision → set_queue_depth(new_count) updates immediately


"""

import json
import logging
import os
from typing import Any, Optional

from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

# TTL values in seconds
TTL_SIGNALS     = int(os.getenv("REDIS_TTL_SIGNALS",      "300"))   # 5 minutes
TTL_BRIEF       = int(os.getenv("REDIS_TTL_BRIEF",        "600"))   # 10 minutes
TTL_QUEUE_DEPTH = int(os.getenv("REDIS_TTL_QUEUE_DEPTH",  "60"))    # 1 minute

# Key prefixes — all MedSignal keys are namespaced to avoid collisions
PREFIX_SIGNALS     = "medsignal:signals"
PREFIX_BRIEF       = "medsignal:brief"
KEY_QUEUE_DEPTH    = "medsignal:hitl:queue_depth"



# ── Lazy initialization ───────────────────────────────────────────────────────
# Redis client loads on first call.
# If Redis is not running, functions fail gracefully and log a warning
# rather than crashing the application.
# This means the API and Streamlit still work — just slower (Snowflake directly).

_CLIENT = None


def _get_client():
    """
    Lazy loader for Redis client.
    Returns None if Redis is not available — callers must handle None gracefully.
    """
    global _CLIENT
    if _CLIENT is None:
        try:
            import redis
            _CLIENT = redis.Redis(
                host           =os.getenv("REDIS_HOST", "localhost"),
                port           =int(os.getenv("REDIS_PORT", "6379")),
                decode_responses=True,   # always return str, not bytes
            )
            # Test connection
            _CLIENT.ping()
            log.info("Redis connected host=%s port=%s",
                     os.getenv("REDIS_HOST", "localhost"),
                     os.getenv("REDIS_PORT", "6379"))
        except Exception as e:
            log.warning(
                "Redis not available — caching disabled. "
                "All reads will go directly to Snowflake. error=%s", e
            )
            _CLIENT = None
    return _CLIENT


# ── Generic get / set / delete ────────────────────────────────────────────────

def cache_get(key: str) -> Optional[Any]:
    """
    Read a value from Redis cache.

    Returns:
        Deserialized Python object if key exists and Redis is available.
        None if key does not exist, Redis is unavailable, or deserialization fails.

    Caller pattern:
        cached = cache_get(key)
        if cached is not None:
            return cached          # cache hit
        result = query_snowflake() # cache miss
        cache_set(key, result)
        return result
    """
    client = _get_client()
    if client is None:
        return None

    try:
        raw = client.get(key)
        if raw is None:
            log.debug("cache_miss key=%s", key)
            return None
        log.debug("cache_hit key=%s", key)
        return json.loads(raw)
    except Exception as e:
        log.warning("cache_get_failed key=%s error=%s", key, e)
        return None


def cache_set(key: str, value: Any, ttl: int = TTL_SIGNALS) -> bool:
    """
    Write a value to Redis cache with TTL.

    Args:
        key   : Redis key
        value : Python object — will be JSON serialized
        ttl   : time to live in seconds

    Returns:
        True if stored successfully, False otherwise.
    """
    client = _get_client()
    if client is None:
        return False

    try:
        client.setex(key, ttl, json.dumps(value))
        log.debug("cache_set key=%s ttl=%d", key, ttl)
        return True
    except Exception as e:
        log.warning("cache_set_failed key=%s error=%s", key, e)
        return False


def cache_delete(key: str) -> bool:
    """Delete one key from Redis cache."""
    client = _get_client()
    if client is None:
        return False

    try:
        client.delete(key)
        log.debug("cache_delete key=%s", key)
        return True
    except Exception as e:
        log.warning("cache_delete_failed key=%s error=%s", key, e)
        return False


# ── Signal cache helpers ──────────────────────────────────────────────────────

def signal_cache_key(priority: Optional[str], limit: int) -> str:
    """
    Build a consistent cache key for GET /signals queries.

    priority=None   → "medsignal:signals:all:200"
    priority="P1"   → "medsignal:signals:P1:200"
    """
    p = priority if priority else "all"
    return f"{PREFIX_SIGNALS}:{p}:{limit}"


def invalidate_signals() -> None:
    """
    Clear all signal cache keys.

    Called by Branch 2 after writing new signals_flagged data.
    Pattern delete on medsignal:signals:* so all priority filters
    are cleared simultaneously.
    """
    client = _get_client()
    if client is None:
        return

    try:
        keys = client.keys(f"{PREFIX_SIGNALS}:*")
        if keys:
            client.delete(*keys)
            cache_delete("medsignal:signals:counts")
            log.info("signal_cache_invalidated keys_cleared=%d", len(keys))
        else:
            log.debug("signal_cache_invalidate_noop — no keys found")
    except Exception as e:
        log.warning("signal_cache_invalidate_failed error=%s", e)


# ── SafetyBrief cache helpers ─────────────────────────────────────────────────

def brief_cache_key(drug_key: str, pt: str) -> str:
    """
    Build a consistent cache key for GET /signals/{drug}/{pt}/brief.

    Example: "medsignal:brief:dupilumab:conjunctivitis"
    Spaces in pt are replaced with underscores for key safety.
    """
    safe_pt = pt.replace(" ", "_").replace("/", "_")
    return f"{PREFIX_BRIEF}:{drug_key}:{safe_pt}"


def invalidate_brief(drug_key: str, pt: str) -> None:
    """
    Clear cache for one specific SafetyBrief.

    Called by Agent 3 after writing a new SafetyBrief to Snowflake
    so Streamlit Signal Detail page shows the updated brief immediately.
    """
    key = brief_cache_key(drug_key, pt)
    cache_delete(key)
    log.info("brief_cache_invalidated drug=%s pt=%s", drug_key, pt)


# ── HITL queue depth helpers ──────────────────────────────────────────────────

def set_queue_depth(depth: int) -> None:
    """
    Store current HITL queue depth in Redis.

    Called after every HITL decision write so Prometheus reads
    the updated value without querying Snowflake every 15 seconds.

    Args:
        depth : number of signals currently awaiting HITL review
    """
    client = _get_client()
    if client is None:
        return

    try:
        client.setex(KEY_QUEUE_DEPTH, TTL_QUEUE_DEPTH, str(depth))
        log.debug("queue_depth_set depth=%d ttl=%d", depth, TTL_QUEUE_DEPTH)
    except Exception as e:
        log.warning("queue_depth_set_failed error=%s", e)


def get_queue_depth() -> int:
    """
    Read current HITL queue depth from Redis.

    Called by Prometheus scrape endpoint every 15 seconds.
    Returns 0 if Redis is unavailable — Prometheus will show 0
    rather than crashing the metrics endpoint.

    Returns:
        int — number of signals awaiting HITL review
    """
    client = _get_client()
    if client is None:
        return 0

    try:
        val = client.get(KEY_QUEUE_DEPTH)
        return int(val) if val else 0
    except Exception as e:
        log.warning("queue_depth_get_failed error=%s", e)
        return 0