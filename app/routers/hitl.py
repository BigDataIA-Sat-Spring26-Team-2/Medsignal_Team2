"""
hitl.py — FastAPI router for HITL decision endpoints.

Endpoints:
    GET  /hitl/queue       — all signals pending review, P1 first
    GET  /hitl/decisions   — full audit log of past decisions
    POST /hitl/decisions   — submit approve/reject/escalate decision

Design:
    Every decision is a new INSERT — never UPDATE.
    The audit log is immutable. If a reviewer changes their mind,
    a second row is written. The latest row per (drug_key, pt) wins.

    queue depth is written to Redis after every POST so Prometheus
    reads the updated value without hitting Snowflake every 15 seconds.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.utils.snowflake_client import get_conn
from app.utils.redis_client import set_queue_depth

log    = logging.getLogger(__name__)
router = APIRouter(prefix="/hitl", tags=["hitl"])

# ── Request schema ────────────────────────────────────────────────────────────

class HITLDecision(BaseModel):
    drug_key     : str
    pt           : str
    decision     : str   # APPROVE / REJECT / ESCALATE
    reviewer_note: Optional[str] = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_pending_count() -> int:
    """
    Count signals in safety_briefs that have no decision yet.
    Used to update Redis queue depth after every HITL write.
    """
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM   safety_briefs sb
        WHERE  sb.generation_error = FALSE
        AND    NOT EXISTS (
            SELECT 1 FROM hitl_decisions hd
            WHERE  hd.drug_key = sb.drug_key
            AND    hd.pt       = sb.pt
        )
        """
    )
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count

# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/queue")
def get_queue():
    """
    Returns all signals awaiting HITL review.

    Joins safety_briefs with signals_flagged for full signal context.
    Excludes signals that already have a decision in hitl_decisions.
    Sorted by priority tier (P1 first) then stat_score descending.
    """
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute(
        """
        SELECT
            sb.drug_key,
            sb.pt,
            sb.priority,
            sb.stat_score,
            sb.lit_score,
            sb.recommended_action,
            sb.brief_text,
            sb.generation_error,
            sf.prr,
            sf.drug_reaction_count  AS case_count,
            sf.death_count,
            sf.hosp_count,
            sf.lt_count,
            sb.generated_at
        FROM   safety_briefs sb
        JOIN   signals_flagged sf
               ON sb.drug_key = sf.drug_key
               AND sb.pt      = sf.pt
        WHERE  sb.generation_error = FALSE
        AND    NOT EXISTS (
            SELECT 1 FROM hitl_decisions hd
            WHERE  hd.drug_key = sb.drug_key
            AND    hd.pt       = sb.pt
        )
        ORDER BY
            CASE sb.priority
                WHEN 'P1' THEN 1
                WHEN 'P2' THEN 2
                WHEN 'P3' THEN 3
                WHEN 'P4' THEN 4
                ELSE 5
            END,
            sb.stat_score DESC NULLS LAST
        """
    )

    rows    = cur.fetchall()
    columns = [desc[0].lower() for desc in cur.description]
    cur.close()
    conn.close()

    return [dict(zip(columns, row)) for row in rows]
