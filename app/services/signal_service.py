"""
signal_service.py — Business logic for signal and SafetyBrief queries.

Called by:
    app/routers/signals.py

Cache pattern:
    Redis first → Snowflake on miss → store in Redis → return
    
Invalidation:
    signals → Branch 2 calls invalidate_signals() after each run
    brief   → Agent 3 calls invalidate_brief(drug_key, pt) after write


"""

import os
import json
import logging
from typing import Optional

import snowflake.connector
from dotenv import load_dotenv

from app.utils.redis_client import (
    cache_get,
    cache_set,
    signal_cache_key,
    brief_cache_key,
    TTL_SIGNALS,
    TTL_BRIEF,
)

load_dotenv()
log = logging.getLogger(__name__)


# ── Database ──────────────────────────────────────────────────────────────────

def _get_conn():
    """Snowflake connection — called only on cache miss."""
    return snowflake.connector.connect(
        account  =os.getenv("SNOWFLAKE_ACCOUNT"),
        user     =os.getenv("SNOWFLAKE_USER"),
        password =os.getenv("SNOWFLAKE_PASSWORD"),
        database =os.getenv("SNOWFLAKE_DATABASE"),
        schema   =os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )


# ── Snowflake queries — called only on cache miss ─────────────────────────────

def _query_signals(priority: Optional[str], limit: int) -> list:
    """TODO: Siddharth implements full query against signals_flagged."""
    raise NotImplementedError


def _query_brief(drug_key: str, pt: str) -> Optional[dict]:
    """TODO: Siddharth implements full query against safety_briefs."""
    raise NotImplementedError


# ── Public API — Redis read-through ───────────────────────────────────────────

def get_all_signals(
    priority: Optional[str] = None,
    limit   : int           = 200,
) -> list:
    """
    Returns all flagged signals.
    Checks Redis first — queries Snowflake only on cache miss.
    """
    key    = signal_cache_key(priority, limit)
    cached = cache_get(key)

    if cached is not None:
        log.info("signals_cache_hit priority=%s limit=%d", priority, limit)
        return cached

    log.info("signals_cache_miss — querying Snowflake priority=%s", priority)
    signals = _query_signals(priority, limit)
    cache_set(key, signals, ttl=TTL_SIGNALS)
    return signals


def get_safety_brief(drug_key: str, pt: str) -> Optional[dict]:
    """
    Returns SafetyBrief for one signal.
    Checks Redis first — queries Snowflake only on cache miss.
    Returns None if no brief generated yet.
    """
    key    = brief_cache_key(drug_key, pt)
    cached = cache_get(key)

    if cached is not None:
        log.info("brief_cache_hit drug=%s pt=%s", drug_key, pt)
        return cached

    log.info("brief_cache_miss — querying Snowflake drug=%s pt=%s", drug_key, pt)
    brief = _query_brief(drug_key, pt)

    if brief:
        # Only cache if brief exists
        # Do not cache None — new briefs must appear immediately
        cache_set(key, brief, ttl=TTL_BRIEF)

    return brief