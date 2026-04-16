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
from app.utils.snowflake_client import get_conn

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




# ── Snowflake queries — called only on cache miss ─────────────────────────────

def _query_signals(priority: Optional[str], limit: int) -> list:
    """Query signals_flagged LEFT JOIN safety_briefs."""
    priority_filter = "AND sb.priority = %s" if priority else ""
    params = [priority, limit] if priority else [limit]

    query = f"""
        SELECT
            sf.drug_key,
            sf.pt,
            sf.prr,
            sf.stat_score,
            sf.drug_reaction_count,
            sf.death_count,
            sf.hosp_count,
            sf.lt_count,
            sf.drug_total,
            TO_CHAR(sf.computed_at, 'YYYY-MM-DD HH24:MI:SS') AS computed_at,
            sb.lit_score,
            sb.priority,
            sb.generation_error
        FROM signals_flagged sf
        LEFT JOIN safety_briefs sb
            ON sf.drug_key = sb.drug_key AND sf.pt = sb.pt
        WHERE 1=1 {priority_filter}
        ORDER BY
            CASE sb.priority
                WHEN 'P1' THEN 1
                WHEN 'P2' THEN 2
                WHEN 'P3' THEN 3
                WHEN 'P4' THEN 4
                ELSE 5
            END,
            sf.prr DESC
        LIMIT %s
    """

    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(query, params)
    rows    = cur.fetchall()
    columns = [desc[0].lower() for desc in cur.description]
    cur.close()
    conn.close()

    return [dict(zip(columns, row)) for row in rows]


def _query_brief(drug_key: str, pt: str) -> Optional[dict]:
    """Query full signal detail including SafetyBrief for one signal."""
    query = """
        SELECT
            sf.drug_key,
            sf.pt,
            sf.prr,
            sf.stat_score,
            sf.drug_reaction_count,
            sf.drug_no_reaction_count,
            sf.other_reaction_count,
            sf.other_no_reaction_count,
            sf.death_count,
            sf.hosp_count,
            sf.lt_count,
            sf.drug_total,
            TO_CHAR(sf.computed_at, 'YYYY-MM-DD HH24:MI:SS') AS computed_at,
            sb.brief_id,
            sb.lit_score,
            sb.priority,
            sb.brief_text,
            sb.key_findings,
            sb.pmids_cited,
            sb.recommended_action,
            sb.model_used,
            sb.generation_error,
            TO_CHAR(sb.generated_at, 'YYYY-MM-DD HH24:MI:SS') AS generated_at
        FROM signals_flagged sf
        LEFT JOIN safety_briefs sb
            ON sf.drug_key = sb.drug_key AND sf.pt = sb.pt
        WHERE sf.drug_key = %s AND sf.pt = %s
        LIMIT 1
    """

    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(query, (drug_key, pt))
    row     = cur.fetchone()
    columns = [desc[0].lower() for desc in cur.description]
    cur.close()
    conn.close()

    if row is None:
        return None

    return dict(zip(columns, row))


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