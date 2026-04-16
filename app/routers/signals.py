"""
signals.py — FastAPI router for signal endpoints

Endpoints:
    GET /signals                    — all flagged signals ranked by priority tier
    GET /signals/{drug_key}/{pt}    — full detail for one signal including SafetyBrief

Data sources:
    signals_flagged  — PRR, stat_score, outcome counts (Branch 2 output)
    safety_briefs    — priority, lit_score, brief_text, key_findings,
                       pmids_cited, recommended_action (Agent 3 output)

Both endpoints join signals_flagged LEFT JOIN safety_briefs on (drug_key, pt).
LEFT JOIN means signals without a SafetyBrief yet still appear — brief fields
will be null if the agent pipeline hasn't run yet.
"""

import os
from typing import Optional

import snowflake.connector
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

load_dotenv()

router = APIRouter(prefix="/signals", tags=["signals"])


# ── Snowflake connection ──────────────────────────────────────────────────────

def get_conn() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account  = os.getenv("SNOWFLAKE_ACCOUNT"),
        user     = os.getenv("SNOWFLAKE_USER"),
        password = os.getenv("SNOWFLAKE_PASSWORD"),
        database = os.getenv("SNOWFLAKE_DATABASE"),
        schema   = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        warehouse= os.getenv("SNOWFLAKE_WAREHOUSE"),
    )


# ── Response models ───────────────────────────────────────────────────────────

class SignalSummary(BaseModel):
    """
    One row in the Signal Feed page.
    Joined from signals_flagged + safety_briefs.
    brief fields are null if agent pipeline hasn't run yet.
    """
    drug_key           : str
    pt                 : str
    prr                : float
    stat_score         : Optional[float]
    lit_score          : Optional[float]
    priority           : Optional[str]
    drug_reaction_count: int
    death_count        : int
    hosp_count         : int
    lt_count           : int
    drug_total         : int
    computed_at        : str


class SignalDetail(BaseModel):
    """
    Full detail for Signal Detail page.
    Includes everything in SignalSummary plus SafetyBrief fields.
    """
    drug_key                : str
    pt                      : str
    prr                     : float
    stat_score              : Optional[float]
    lit_score               : Optional[float]
    priority                : Optional[str]
    drug_reaction_count     : int
    drug_no_reaction_count  : int
    other_reaction_count    : int
    other_no_reaction_count : int
    death_count             : int
    hosp_count              : int
    lt_count                : int
    drug_total              : int
    computed_at             : str
    # SafetyBrief fields — null if agent pipeline hasn't run
    brief_id                : Optional[int]
    brief_text              : Optional[str]
    key_findings            : Optional[list]
    pmids_cited             : Optional[list]
    recommended_action      : Optional[str]
    model_used              : Optional[str]
    generation_error        : Optional[bool]
    generated_at            : Optional[str]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _priority_order(priority: Optional[str]) -> int:
    """Sort key for priority tier — P1 first, unranked last."""
    return {"P1": 1, "P2": 2, "P3": 3, "P4": 4}.get(priority or "", 5)


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("", response_model=list[SignalSummary])
def get_signals():
    """
    GET /signals

    Returns all flagged signals joined with safety_briefs,
    sorted by priority tier (P1 first) then PRR descending.

    Used by: Signal Feed page, Evaluation Dashboard
    """
    query = """
        SELECT
            sf.drug_key,
            sf.pt,
            sf.prr,
            sf.stat_score,
            sb.lit_score,
            sb.priority,
            sf.drug_reaction_count,
            sf.death_count,
            sf.hosp_count,
            sf.lt_count,
            sf.drug_total,
            TO_CHAR(sf.computed_at, 'YYYY-MM-DD HH24:MI:SS') AS computed_at
        FROM signals_flagged sf
        LEFT JOIN safety_briefs sb
            ON sf.drug_key = sb.drug_key AND sf.pt = sb.pt
        ORDER BY sf.prr DESC
    """

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(query)
        rows    = cur.fetchall()
        columns = [desc[0].lower() for desc in cur.description]
        cur.close()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Snowflake query failed: {e}")

    signals = [dict(zip(columns, row)) for row in rows]

    # Sort by priority tier then PRR descending
    signals.sort(key=lambda s: (_priority_order(s.get("priority")), -s["prr"]))

    return signals


@router.get("/{drug_key}/{pt}", response_model=SignalDetail)
def get_signal_detail(drug_key: str, pt: str):
    """
    GET /signals/{drug_key}/{pt}

    Returns full detail for one signal including all SafetyBrief fields.
    key_findings and pmids_cited are stored as VARIANT in Snowflake —
    returned as lists in JSON.

    Returns 404 if the signal does not exist in signals_flagged.

    Used by: Signal Detail page
    """
    query = """
        SELECT
            sf.drug_key,
            sf.pt,
            sf.prr,
            sf.stat_score,
            sb.lit_score,
            sb.priority,
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

    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute(query, (drug_key, pt))
        row     = cur.fetchone()
        columns = [desc[0].lower() for desc in cur.description]
        cur.close()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Snowflake query failed: {e}")

    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Signal not found: drug_key='{drug_key}' pt='{pt}'"
        )

    signal = dict(zip(columns, row))

    # Snowflake VARIANT columns (key_findings, pmids_cited) are returned
    # as Python objects (list/dict) by the connector — no JSON parsing needed
    return signal