"""
state.py — LangGraph shared state for MedSignal agent pipeline.

Every field is typed. Agents read from state and add their outputs.
No agent modifies another agent's fields.

Field ownership:
    Input (from signals_flagged) : drug_key, pt, prr, case_count,
                                   death_count, hosp_count, lt_count,
                                   stat_score
    Agent 1 adds                 : search_queries
    Agent 2 adds                 : abstracts, lit_score
    Agent 3 adds                 : priority, brief

Note on stat_score:
    StatScore is computed deterministically in Branch 2 (branch2_prr.py)
    from PRR, case_count, and outcome flags. It is written to
    signals_flagged and loaded into state by pipeline.py before the
    agent pipeline runs. Agent 1 does NOT recompute it — it reads
    stat_score from state and passes it through unchanged.
"""

from typing import TypedDict, List, Optional


class Abstract(TypedDict):
    """One PubMed abstract returned by Agent 2 from ChromaDB."""
    pmid      : str
    text      : str
    similarity: float   # cosine similarity score — 0.0 to 1.0


class SafetyBrief(TypedDict):
    """Structured SafetyBrief produced by Agent 3 and validated by Pydantic."""
    brief_text        : str
    key_findings      : List[str]
    pmids_cited       : List[str]
    recommended_action: str


class SignalState(TypedDict):
    """
    Shared state passed between all three LangGraph agents.

    Populated in stages:
        Stage 0 — loaded from signals_flagged before pipeline runs
        Stage 1 — Agent 1 (Signal Detector) fills search_queries
        Stage 2 — Agent 2 (Literature Retriever) fills abstracts + lit_score
        Stage 3 — Agent 3 (Assessor) fills priority + brief
    """

    # ── Stage 0: input from signals_flagged ──────────────────────────────
    drug_key   : str    # canonical drug name e.g. "dupilumab"
    pt         : str    # MedDRA preferred term e.g. "conjunctivitis"
    prr        : float  # Proportional Reporting Ratio
    case_count : int    # A in PRR formula — cases with drug X + reaction Y
    death_count: int    # cases with death outcome flag
    hosp_count : int    # cases with hospitalisation outcome flag
    lt_count   : int    # cases with life-threatening outcome flag
    stat_score : float  # StatScore ∈ [0, 1] — computed by Branch 2,
                        # not recomputed by Agent 1

    # ── Stage 1: Agent 1 output ──────────────────────────────────────────
    search_queries: Optional[List[str]]  # 3 GPT-4o generated PubMed queries

    # ── Stage 2: Agent 2 output ──────────────────────────────────────────
    abstracts : Optional[List[Abstract]] # top-5 PubMed abstracts from ChromaDB
    lit_score : Optional[float]          # LitScore ∈ [0, 1]

    # ── Stage 3: Agent 3 output ──────────────────────────────────────────
    priority: Optional[str]         # P1 / P2 / P3 / P4
    brief   : Optional[SafetyBrief] # Pydantic-validated SafetyBrief