"""
routers/signals.py — FastAPI router for signal endpoints.

Endpoints:
    GET  /signals                              → list all flagged signals
    GET  /signals/{drug_key}/{pt}/brief        → get SafetyBrief for one signal
    POST /signals/{drug_key}/{pt}/investigate  → run on-demand agent pipeline

Calls:
    app/services/signal_service.py — all business logic and Redis caching
    app/agents/pipeline.py         — on-demand pipeline for investigate endpoint

Router stays thin — receive request, call service, return response.
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
    return get_all_signals(priority=priority, limit=limit)


# @router.post("/{drug_key}/{pt}/investigate")
# def investigate(drug_key: str, pt: str):
#     """
#     Triggers on-demand agent pipeline for one signal.
#     Called by Streamlit Signal Detail page when analyst clicks Investigate.
#     Auto-fetches PubMed abstracts if ChromaDB missing for this drug.
#     Returns priority and status after pipeline completes.
#     """
#     from app.agents.pipeline import run_single_signal

#     try:
#         result = run_single_signal(drug_key, pt)
#         return {
#             "priority": result.get("priority"),
#             "status"  : "complete",
#             "error"   : result.get("error"),
#         }
#     except ValueError as e:
#         raise HTTPException(
#             status_code=404,
#             detail=str(e),
#         )
#     except Exception as e:
#         raise HTTPException(
#             status_code=500,
#             detail=f"Pipeline failed: {e}",
#         )
@router.post("/{drug_key}/{pt}/investigate")
async def investigate(drug_key: str, pt: str):
    import asyncio
    from app.agents.pipeline import run_single_signal

    try:
        # Run blocking pipeline in a thread pool so FastAPI stays responsive
        loop   = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, run_single_signal, drug_key, pt
        )
        print(f"INVESTIGATE RESULT: priority={result.get('priority')} error={result.get('error')}")
        return {
            "priority": result.get("priority"),
            "status"  : "complete",
            "error"   : result.get("error"),
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pipeline failed: {e}")

@router.get("/{drug_key}/{pt}/brief")
def get_brief(drug_key: str, pt: str):
    """
    Returns full SafetyBrief for one drug-reaction signal.
    Redis cached — first call hits Snowflake, subsequent calls return in <10ms.
    Cache invalidated automatically when Agent 3 writes a new SafetyBrief.
    404 if no SafetyBrief has been generated yet for this signal.
    """
    brief = get_safety_brief(drug_key=drug_key, pt=pt)

    if not brief:
        raise HTTPException(
            status_code=404,
            detail=f"No SafetyBrief found for {drug_key} x {pt}. "
                   f"Agent 3 may not have run yet for this signal."
        )

    return brief

@router.get("/debug")
def debug():
    """Temporary — remove after fixing."""
    from app.services.signal_service import get_all_signals
    try:
        result = get_all_signals(priority=None, limit=5)
        return {"count": len(result), "first": result[0] if result else None}
    except Exception as e:
        return {"error": str(e)}


@router.post("/cache/invalidate")
def invalidate_cache():
    """
    Clears Redis signal cache.
    Called by Streamlit after on-demand investigation completes
    so the next fetch reflects the updated priority counts.
    """
    from app.utils.redis_client import invalidate_signals
    invalidate_signals()
    return {"status": "invalidated"}