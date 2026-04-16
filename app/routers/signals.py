"""
routers/signals.py — FastAPI router for signal endpoints.

Endpoints:
    GET /signals                      → list all flagged signals
    GET /signals/{drug_key}/{pt}/brief → get SafetyBrief for one signal

Calls:
    app/services/signal_service.py — all business logic and Redis caching live there
    Router stays thin — receive request, call service, return response.

Redis is handled entirely inside signal_service.py.
This router has no direct Redis dependency.

"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional

from app.services.signal_service import (
    get_all_signals,
    get_safety_brief,
)

router = APIRouter(prefix="/signals", tags=["signals"])


@router.get("")
def list_signals(
    priority: Optional[str] = Query(None, description="Filter by P1/P2/P3/P4"),
    limit   : int           = Query(200,  description="Max signals to return"),
):
    """
    Returns all flagged signals from signals_flagged.
    Redis cached — first call hits Snowflake, subsequent calls return in <10ms.
    Cache invalidated automatically when Branch 2 re-runs.
    """
    # TODO: Siddharth adds response_model once Signal Pydantic model is defined
    return get_all_signals(priority=priority, limit=limit)


@router.get("/{drug_key}/{pt}/brief")
def get_brief(drug_key: str, pt: str):
    """
    Returns full SafetyBrief for one drug-reaction signal.
    Redis cached — first call hits Snowflake, subsequent calls return in <10ms.
    Cache invalidated automatically when Agent 3 writes a new SafetyBrief.
    404 if no SafetyBrief has been generated yet for this signal.
    """
    # TODO: Siddharth adds response_model once SafetyBrief Pydantic model is defined
    brief = get_safety_brief(drug_key=drug_key, pt=pt)

    if not brief:
        raise HTTPException(
            status_code=404,
            detail=f"No SafetyBrief found for {drug_key} x {pt}. "
                   f"Agent 3 may not have run yet for this signal."
        )

    return brief