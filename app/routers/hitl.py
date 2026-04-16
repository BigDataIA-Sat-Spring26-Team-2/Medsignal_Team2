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