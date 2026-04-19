"""
pipeline.py — MedSignal LangGraph Agent Pipeline

Two workflows:

    Workflow 1 — Batch (run after Branch 2 completes)
        Loads all golden drug signals from Snowflake signals_flagged.
        Runs A1 → A2 → A3 for each signal sequentially.
        Agent 3 writes each SafetyBrief to Snowflake directly.
        Invalidates Redis cache after all signals complete.
        Triggered by: python -m app.agents.pipeline

    Workflow 2 — On Demand (triggered by analyst via FastAPI)
        Receives one drug_key + pt from the API.
        Ensures ChromaDB has abstracts for the drug — fetches from
        PubMed automatically if missing (first-time investigation).
        Runs A1 → A2 → A3 for that single signal.
        Invalidates Redis brief cache immediately after.
        Triggered by: POST /signals/{drug_key}/{pt}/investigate

Why Agent 3 writes to Snowflake directly:
    Agent 3 has all information needed — stat_score, lit_score,
    abstracts, priority, brief. Writing inside Agent 3 keeps the
    write co-located with generation logic and retry handling.
    pipeline.py does not need to know the safety_briefs schema.

Why MemorySaver:
    Checkpoints state between nodes for debugging — if a node fails
    mid-pipeline the state at each boundary can be inspected.
    In-memory only — does not persist across process restarts.

Owner: Prachi
"""

import logging
import os
from time import time
from typing import Optional
from unittest import result

from app.routers import signals
from dotenv import load_dotenv
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from app.agents.state import SignalState
from app.agents.agent1_detector  import agent1_node
from app.agents.agent2_retriever import agent2_node
from app.agents.agent3_assessor  import agent3_node
from app.utils.snowflake_client  import get_conn
from app.utils.redis_client      import invalidate_brief, invalidate_signals

load_dotenv()

log = logging.getLogger(__name__)


# ── Golden drugs ──────────────────────────────────────────────────────────────
# Agent pipeline runs for all signals belonging to these 10 drugs in batch mode.
# Selected because they have documented FDA safety communications in 2023
# — verified ground truth for evaluation.
# PubMed abstracts are pre-loaded in ChromaDB for these drugs.
# On-demand mode can investigate any drug — ChromaDB auto-populated if missing.

GOLDEN_DRUGS = [
    "dupilumab",
    "gabapentin",
    "pregabalin",
    "levetiracetam",
    "tirzepatide",
    "semaglutide",
    "empagliflozin",
    "bupropion",
    "dapagliflozin",
    "metformin",
]


# ── Pipeline graph ────────────────────────────────────────────────────────────

def create_pipeline():
    """
    Build and compile the LangGraph StateGraph.

    Structure: agent1 → agent2 → agent3 → END

    Linear, deterministic, no loops, no supervisor nodes.
    Proposal p26: "The pipeline is deterministic in structure —
    it always runs Agent 1, then Agent 2, then Agent 3, in that order."

    MemorySaver checkpoints state at each node boundary.
    Useful during debugging — inspect exactly what each agent
    received and produced for a failed signal.
    """
    graph = StateGraph(SignalState)

    graph.add_node("agent1", agent1_node)
    graph.add_node("agent2", agent2_node)
    graph.add_node("agent3", agent3_node)

    graph.set_entry_point("agent1")
    graph.add_edge("agent1", "agent2")
    graph.add_edge("agent2", "agent3")
    graph.add_edge("agent3", END)

    return graph.compile(checkpointer=MemorySaver())


# Module-level — compiled once, reused for every signal invocation.
# Avoids recompiling the graph on every run_single_signal() call.
pipeline = create_pipeline()


# ── ChromaDB population ───────────────────────────────────────────────────────

def ensure_drug_loaded(drug_key: str) -> int:
    """
    Ensure ChromaDB has PubMed abstracts for the given drug.

    Called before every on-demand pipeline run to guarantee Agent 2
    has literature to retrieve. Without this, on-demand investigation
    of non-golden drugs returns LitScore=0.0 and a SafetyBrief with
    no citations — which is misleading rather than informative.

    Threshold of 50 abstracts:
        Below 50 the BM25 IDF scores become unreliable — too few
        documents for meaningful inverse document frequency computation.
        50 is conservative — load_pubmed fetches up to 200 per drug.

    First-time investigation of a new drug takes ~30-60 seconds
    (NCBI API calls). Subsequent investigations are instant —
    ChromaDB persists across process restarts.

    Args:
        drug_key : canonical drug name from signals_flagged

    Returns:
        int — total abstract count for this drug in ChromaDB
    """
    from app.scripts.load_pubmed import load_drug
    from app.utils.chromadb_client import get_client, get_collection

    client     = get_client()
    collection = get_collection(client)

    # Check existing abstract count for this drug
    existing = collection.get(where={"drug_name": drug_key})
    count    = len(existing["ids"])

    if count >= 50:
        log.info(
            "chromadb_ready drug=%s abstracts=%d",
            drug_key, count,
        )
        return count

    # Not enough abstracts — fetch from PubMed now
    log.info(
        "chromadb_insufficient drug=%s abstracts=%d — fetching from PubMed",
        drug_key, count,
    )

    newly_loaded = load_drug(drug_key)

    total = count + newly_loaded
    log.info(
        "chromadb_loaded drug=%s newly_added=%d total=%d",
        drug_key, newly_loaded, total,
    )

    return total


# ── Signal loader ─────────────────────────────────────────────────────────────

def load_golden_signals() -> list:
    """
    Load all signals for the 10 golden drugs from Snowflake signals_flagged.

    Fetches all columns needed to populate SignalState at Stage 0.
    stat_score is loaded from the database — computed by Branch 2
    and stored in signals_flagged. Agent 1 reads it from state
    without recomputing.

    Uses ILIKE filter so salt form variants are caught:
        "bupropion" matches "bupropion hydrochloride" if normalization
        was not fully applied for some DRUG file records.

    Orders by PRR descending so highest-signal drugs process first.
    If the batch run is interrupted, the most important signals
    have already been processed and written to Snowflake.

    Returns:
        List of signal dicts — one per signals_flagged row
        for the 10 golden drugs.
    """
    conn = get_conn()
    cur  = conn.cursor()

    
    conditions = " OR ".join(["drug_key = %s"] * len(GOLDEN_DRUGS))
    params     = GOLDEN_DRUGS

    cur.execute(f"""
        SELECT
            drug_key,
            pt,
            prr,
            drug_reaction_count,
            death_count,
            hosp_count,
            lt_count,
            stat_score
        FROM signals_flagged
        WHERE {conditions}
        ORDER BY prr DESC
    """, params)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    signals = [
        {
            "drug_key"   : str(row[0]),
            "pt"         : str(row[1]),
            "prr"        : float(row[2]),
            "case_count" : int(row[3]),
            "death_count": int(row[4] or 0),
            "hosp_count" : int(row[5] or 0),
            "lt_count"   : int(row[6] or 0),
            "stat_score" : float(row[7]) if row[7] is not None else None,
        }
        for row in rows
    ]

    log.info(
        "load_golden_signals — %d signals found for %d golden drugs",
        len(signals), len(GOLDEN_DRUGS),
    )

    return signals
def _sanitize_state(state: dict) -> dict:
    """
    Recursively convert numpy types to plain Python types.
    Called before pipeline.invoke() to prevent MemorySaver
    msgpack serialization failures.
    """
    import numpy as np

    def convert(obj):
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, dict):
            return {k: convert(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [convert(i) for i in obj]
        return obj

    return convert(state)

def load_single_signal(drug_key: str, pt: str) -> dict:
    """
    Load one specific signal from Snowflake signals_flagged.

    Called by on-demand workflow. Raises ValueError if the signal
    does not exist in signals_flagged — this means either Branch 2
    has not run yet or the signal did not clear the PRR thresholds.

    Args:
        drug_key : canonical drug name e.g. "dupilumab"
        pt       : MedDRA preferred term e.g. "conjunctivitis"

    Returns:
        Signal dict with all fields needed to build SignalState.

    Raises:
        ValueError : if signal not found in signals_flagged
    """
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("""
        SELECT
            drug_key,
            pt,
            prr,
            drug_reaction_count,
            death_count,
            hosp_count,
            lt_count,
            stat_score
        FROM signals_flagged
        WHERE drug_key = %s AND pt = %s
    """, (drug_key, pt))

    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        raise ValueError(
            f"Signal not found in signals_flagged: {drug_key} x {pt}. "
            f"Either Branch 2 has not run yet or this pair did not "
            f"clear the PRR thresholds (A>=50, C>=200, PRR>=2.0)."
        )

    return {
        "drug_key"   : row[0],
        "pt"         : row[1],
        "prr"        : float(row[2]),
        "case_count" : int(row[3]),
        "death_count": int(row[4] or 0),
        "hosp_count" : int(row[5] or 0),
        "lt_count"   : int(row[6] or 0),
        "stat_score" : float(row[7]) if row[7] is not None else None,
    }


# ── State builder ─────────────────────────────────────────────────────────────

def build_initial_state(signal: dict) -> SignalState:
    """
    Build the SignalState dict for one signal at Stage 0.

    All Stage 0 fields come from signals_flagged.
    All Agent output fields start as None — each agent fills only
    its own fields. LangGraph merges agent return dicts into state
    automatically after each node completes.

    stat_score is included at Stage 0 because it was computed by
    Branch 2 and stored in signals_flagged. Agent 1 reads it from
    state and confirms it — does not recompute.

    Returns:
        SignalState TypedDict with Stage 0 populated,
        all agent output fields set to None.
    """
    return {
        # Stage 0 — from signals_flagged
        "drug_key"   : signal["drug_key"],
        "pt"         : signal["pt"],
        "prr"        : float(signal["prr"]),
        "case_count" : int(signal["case_count"]),
        "death_count": int(signal["death_count"]),
        "hosp_count" : int(signal["hosp_count"]),
        "lt_count"   : int(signal["lt_count"]),
        "stat_score" : float(signal["stat_score"]) if signal["stat_score"] is not None else None,

        # Stage 1 — Agent 1 outputs
        "search_queries": None,

        # Stage 2 — Agent 2 outputs
        "abstracts" : None,
        "lit_score" : None,

        # Stage 3 — Agent 3 outputs
        "priority"  : None,
        "brief"     : None,
    }


# ── Core pipeline runner ──────────────────────────────────────────────────────

def run_pipeline_for_signal(signal: dict) -> dict:
    """
    Run the full agent pipeline for one signal.

    thread_id is unique per drug-reaction pair — ensures MemorySaver
    checkpoints are isolated. One signal's state never bleeds into
    another even if they run in the same process.

    Args:
        signal : dict with all Stage 0 fields loaded from Snowflake

    Returns:
        Final state dict after all three agents complete.
        Key fields: priority, brief, stat_score, lit_score, error
    """
    drug_key = signal["drug_key"]
    pt       = signal["pt"]

    initial_state = build_initial_state(signal)
    initial_state = _sanitize_state(initial_state)

    # Replace spaces in pt with underscores for a valid thread_id
    thread_id = f"{drug_key}__{pt.replace(' ', '_').replace('/', '_')}"

    config = {"configurable": {"thread_id": thread_id}}

    log.info(
        "pipeline_start drug=%s pt=%s prr=%.2f stat_score=%s",
        drug_key, pt, signal["prr"], signal["stat_score"],
    )

    import time
    _start = time.perf_counter()
    result = pipeline.invoke(initial_state, config)
    try:
        from app.observability.metrics import PIPELINE_DURATION
        PIPELINE_DURATION.observe(time.perf_counter() - _start)
    except Exception:
        pass
    return result


# ── Workflow 1 — Batch ────────────────────────────────────────────────────────

def run_all_golden_signals():
    """
    Workflow 1 — Batch pipeline for all 10 golden drug signals.

    Run this after every Branch 2 completion to generate SafetyBriefs
    for all golden drug-reaction pairs. Expected runtime under 2 minutes
    for all signals from Q1 data.

    Each signal is processed independently:
        - One failure does not stop the remaining signals
        - generation_error signals are still written to Snowflake
          so HITL queue sees them with a flag indicating brief failed

    Redis invalidation:
        - Brief cache cleared per signal immediately after Agent 3 writes
        - Signal cache cleared once after all signals complete
          so Streamlit Signal Feed shows updated priority tiers

    Logs a summary at the end:
        Success       — SafetyBrief generated and written
        Generation err — Agent 3 failed both attempts, generation_error=True
        Exceptions    — unexpected error, signal skipped entirely
    """
    signals = load_golden_signals()
    # signals=load_golden_signals()[:15] # Temporary limit for testing — remove slicing for full run
    if not signals:
        log.error(
            "No golden signals found in signals_flagged. "
            "Run Branch 2 first and confirm all 10 golden drugs are present."
        )
        return

    log.info("=" * 60)
    log.info("MedSignal — Batch Pipeline Starting")
    log.info("Signals : %d", len(signals))
    log.info("=" * 60)

    success       = 0
    gen_error     = 0
    exceptions    = 0

    for i, signal in enumerate(signals, 1):
        drug_key = signal["drug_key"]
        pt       = signal["pt"]

        log.info("[%d/%d] %s x %s", i, len(signals), drug_key, pt)

        try:
            result = run_pipeline_for_signal(signal)

            if result.get("error"):
                log.warning(
                    "generation_error drug=%s pt=%s",
                    drug_key, pt,
                )
                gen_error += 1
            else:
                log.info(
                    "success drug=%s pt=%s priority=%s",
                    drug_key, pt, result.get("priority"),
                )
                success += 1

            # Invalidate brief cache so Signal Detail shows latest output
            invalidate_brief(drug_key, pt)

        except Exception as exc:
            import traceback
            log.error(
                 "exception drug=%s pt=%s error=%s\n%s",
                     drug_key, pt, exc,
                 traceback.format_exc()  # ← add this
            )
            exceptions += 1
            continue

    # Invalidate signal cache so Signal Feed shows updated priority tiers
    invalidate_signals()

    log.info("=" * 60)
    log.info("MedSignal — Batch Pipeline Complete")
    log.info("Success        : %d", success)
    log.info("Generation err : %d", gen_error)
    log.info("Exceptions     : %d", exceptions)
    log.info("Total          : %d", len(signals))
    log.info("=" * 60)


# ── Workflow 2 — On Demand ────────────────────────────────────────────────────

def run_single_signal(drug_key: str, pt: str) -> dict:
    """
    Workflow 2 — On-demand pipeline for one specific signal.

    Called by FastAPI POST /signals/{drug_key}/{pt}/investigate
    when an analyst clicks Investigate on the Streamlit Signal Feed.

    Steps:
        1. Load signal from signals_flagged — raises ValueError if not found
        2. Ensure ChromaDB has abstracts for this drug
           — fetches from PubMed automatically if missing (~30-60 seconds)
           — subsequent investigations of the same drug are instant
        3. Run A1 → A2 → A3
        4. Invalidate Redis brief cache so Signal Detail shows new output

    ChromaDB auto-population means any signal in signals_flagged
    can be investigated — not just the 10 golden drugs.
    Agent 2 will have literature regardless of whether the drug
    was pre-loaded at system startup.

    Args:
        drug_key : canonical drug name e.g. "finasteride"
        pt       : MedDRA preferred term e.g. "depression"

    Returns:
        dict with keys: priority, brief, stat_score, lit_score, error

    Raises:
        ValueError : if signal not found in signals_flagged
    """
    log.info("on_demand_start drug=%s pt=%s", drug_key, pt)

    # Step 1 — load signal
    signal = load_single_signal(drug_key, pt)

    # Step 2 — ensure ChromaDB has abstracts for this drug
    # This is the key difference from batch mode — on-demand auto-populates
    # ChromaDB for any drug the analyst investigates, not just golden drugs
    abstract_count = ensure_drug_loaded(drug_key)
    log.info(
        "chromadb_ready drug=%s abstracts=%d",
        drug_key, abstract_count,
    )

    # Step 3 — run pipeline
    result = run_pipeline_for_signal(signal)

    # Step 4 — invalidate Redis brief cache
    invalidate_brief(drug_key, pt)

    log.info(
        "on_demand_complete drug=%s pt=%s priority=%s error=%s",
        drug_key, pt,
        result.get("priority"),
        result.get("error"),
    )

    return result


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level  =logging.INFO,
        format ="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )
    run_all_golden_signals()