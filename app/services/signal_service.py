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
from decimal import Decimal

def _to_float(v):
    if isinstance(v, Decimal):
        return float(v)
    return v

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

def _query_signals(
    priority: Optional[str],
    limit: int,
    offset: int = 0,
    search: Optional[str] = None,
) -> list:
    """
    Query signals_flagged with optional filtering and pagination.

    Args:
        priority: Filter by priority tier (P1/P2/P3/P4). None = all priorities.
        limit: Maximum number of signals to return.
        offset: Number of signals to skip (for pagination).
        search: Case-insensitive substring match on drug_key or pt.

    Returns:
        List of signal dicts ordered by priority tier, then PRR descending.
    """
    filters = ["1=1"]
    params = []

    if priority:
        filters.append("sb.priority = %s")
        params.append(priority)

    if search:
        filters.append("(sf.drug_key ILIKE %s OR sf.pt ILIKE %s)")
        search_pattern = f"%{search}%"
        params.extend([search_pattern, search_pattern])

    where_clause = " AND ".join(filters)
    params.extend([limit, offset])

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
        WHERE {where_clause}
        ORDER BY
            CASE sb.priority
                WHEN 'P1' THEN 1
                WHEN 'P2' THEN 2
                WHEN 'P3' THEN 3
                WHEN 'P4' THEN 4
                ELSE 5
            END,
            sf.prr DESC
        LIMIT %s OFFSET %s
    """

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall()
    columns = [desc[0].lower() for desc in cur.description]
    cur.close()
    conn.close()

    return [_clean_row(dict(zip(columns, row))) for row in rows]


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

    return _clean_row(dict(zip(columns, row)))


# ── Public API — Redis read-through ───────────────────────────────────────────

def get_all_signals(
    priority: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
    search: Optional[str] = None,
) -> list:
    """
    Returns flagged signals with optional filtering and pagination.

    Args:
        priority: Filter by priority tier (P1/P2/P3/P4).
        limit: Maximum signals to return (default: 200).
        offset: Skip first N signals for pagination (default: 0).
        search: Case-insensitive substring match on drug_key or pt.

    Returns:
        List of signal dicts. Redis cached (5 min TTL) unless search is active.

    Notes:
        Search queries bypass cache to ensure fresh results.
        Pagination queries bypass cache to prevent cache explosion.
    """
    use_cache = (offset == 0 and search is None)

    if use_cache:
        key = signal_cache_key(priority, limit)
        cached = cache_get(key)
        if cached is not None:
            log.info("signals_cache_hit priority=%s limit=%d", priority, limit)
            return cached

    log.info(
        "signals_cache_miss — querying Snowflake priority=%s offset=%d search=%s",
        priority, offset, search
    )
    signals = _query_signals(priority, limit, offset, search)

    if use_cache:
        cache_set(key, signals, ttl=TTL_SIGNALS)

    return signals

# ── Count query — called by GET /signals/count ────────────────────────────────

TTL_COUNTS = 300  # same TTL as signals (5 min)
COUNT_CACHE_KEY = "medsignal:signals:counts"

def get_signal_counts() -> dict:
    cached = cache_get(COUNT_CACHE_KEY)
    if cached is not None:
        log.info("counts_cache_hit")
        return cached

    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(*)                                   AS total,
            COUNT_IF(sb.priority = 'P1')               AS p1,
            COUNT_IF(sb.priority = 'P2')               AS p2,
            COUNT_IF(sb.priority = 'P3')               AS p3,
            COUNT_IF(sb.priority = 'P4')               AS p4,
            COUNT_IF(sb.priority IS NULL)              AS uninvestigated
        FROM signals_flagged sf
        LEFT JOIN safety_briefs sb
            ON sf.drug_key = sb.drug_key AND sf.pt = sb.pt
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()

    counts = {
        "total": int(row[0]),
        "P1":    int(row[1]),
        "P2":    int(row[2]),
        "P3":    int(row[3]),
        "P4":    int(row[4]),
        "uninvestigated": int(row[5]),
    }
    cache_set(COUNT_CACHE_KEY, counts, ttl=TTL_COUNTS)
    return counts

def _clean_row(d: dict) -> dict:
    """
    Convert Snowflake types to JSON-serializable Python types.
    VARIANT columns (key_findings, pmids_cited) come back as
    raw JSON strings — parse them into Python lists.
    """
    cleaned = {}
    for k, v in d.items():
        if isinstance(v, Decimal):
            cleaned[k] = float(v)
        elif k in ("key_findings", "pmids_cited"):
            # Snowflake VARIANT — may be string or already parsed
            if isinstance(v, str):
                try:
                    cleaned[k] = json.loads(v)
                except Exception:
                    cleaned[k] = []
            elif isinstance(v, list):
                cleaned[k] = v
            else:
                cleaned[k] = []
        elif isinstance(v, bool):
            cleaned[k] = v
        elif v is None:
            cleaned[k] = None
        else:
            cleaned[k] = v
    return cleaned

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