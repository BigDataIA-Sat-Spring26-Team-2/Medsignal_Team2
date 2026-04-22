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
    get_signal_counts
)

router = APIRouter(prefix="/signals", tags=["signals"])


@router.get("")
def list_signals(
    priority: Optional[str] = Query(None, description="Filter by priority tier (P1/P2/P3/P4)"),
    limit: int = Query(200, ge=1, le=5000, description="Max signals to return"),
    offset: int = Query(0, ge=0, description="Skip first N signals (pagination)"),
    search: Optional[str] = Query(None, min_length=2, description="Search drug or reaction name"),
):
    """
    List flagged signals with optional filtering and pagination.

    Query Parameters:
        priority: Filter by tier (P1/P2/P3/P4). Omit for all.
        limit: Max results per page (1-5000). Default: 200.
        offset: Skip first N signals for pagination. Default: 0.
        search: Case-insensitive substring match on drug_key or pt (min 2 chars).

    Returns:
        List of signals ordered by priority tier, then PRR descending.

    Caching:
        First page (offset=0, no search): Redis cached (5 min TTL).
        Pagination/search: Direct Snowflake query (no cache).
    """
    return get_all_signals(priority=priority, limit=limit, offset=offset, search=search)

@router.get("/count")
def signal_counts():
    """
    Returns total signal count and per-priority tier breakdown.
    Used by Signal Feed header to show accurate stats independent of pagination.
    Redis cached (5 min TTL). Invalidated when signals cache is cleared.
    """
    return get_signal_counts()

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
    import traceback
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
        print(f"ValueError in investigate: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        print(f"Exception in investigate: {type(e).__name__}: {e}")
        traceback.print_exc()
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


@router.get("/{drug_key}/{pt}/evidence")
def get_evidence(drug_key: str, pt: str):
    """
    Returns top-10 fused PubMed abstracts from ChromaDB for a drug-reaction pair.

    Flow:
        1. Verify signal exists in signals_flagged — 404 if not.
        2. Call agent1 generate_queries to get 3 GPT-4o search queries.
        3. Run hnsw_search + bm25_search for each query → 6 result sets.
        4. Fuse with reciprocal_rank_fusion.
        5. Return top 10 results + summary stats.
    """
    from app.services.signal_service import get_all_signals
    from app.agents.agent1_detector import generate_queries
    from app.agents.agent2_retriever import hnsw_search, bm25_search, reciprocal_rank_fusion

    # ── Step 1: verify signal exists ──────────────────────────────────────────
    all_signals = get_all_signals(priority=None, limit=500)
    signal = next(
        (s for s in all_signals
         if s.get("drug_key") == drug_key and s.get("pt") == pt),
        None,
    )
    if signal is None:
        raise HTTPException(
            status_code=404,
            detail=f"Signal not found in signals_flagged: {drug_key} x {pt}. "
                   f"Run the agent pipeline for this signal first.",
        )

    prr        = float(signal.get("prr") or 2.0)
    case_count = int(signal.get("case_count") or signal.get("drug_reaction_count") or 1)

    # ── Step 2: generate 3 search queries via agent1 ──────────────────────────
    try:
        queries = generate_queries(drug_key, pt, prr, case_count)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Query generation failed: {e}",
        )

    # ── Step 3: run HNSW + BM25 for each query → 6 result sets ───────────────
    all_results = []
    hnsw_total  = 0
    bm25_total  = 0

    try:
        for query in queries:
            try:
                hnsw_res = hnsw_search(query, drug_key)
                hnsw_total += len(hnsw_res)
                all_results.append(hnsw_res)
            except Exception:
                all_results.append([])

            try:
                bm25_res = bm25_search(query, drug_key)
                bm25_total += len(bm25_res)
                all_results.append(bm25_res)
            except Exception:
                all_results.append([])
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"ChromaDB unreachable or has no abstracts for '{drug_key}': {e}",
        )

    # ── Step 4: fuse with RRF and take top 10 ─────────────────────────────────
    fused = reciprocal_rank_fusion(all_results)
    top10 = fused[:10]

    # ── Step 5: build summary ─────────────────────────────────────────────────
    hnsw_in_top = sum(1 for a in top10 if a.get("retriever") == "hnsw")
    bm25_in_top = sum(1 for a in top10 if a.get("retriever") == "bm25")
    avg_sim     = (
        round(sum(a["similarity"] for a in top10) / len(top10), 4)
        if top10 else 0.0
    )

    return {
        "abstracts": top10,
        "queries"  : queries,
        "summary"  : {
            "drug_key"       : drug_key,
            "pt"             : pt,
            "hnsw_count"     : hnsw_in_top,
            "bm25_count"     : bm25_in_top,
            "avg_similarity" : avg_sim,
            "total_retrieved": len(fused),
        },
    }