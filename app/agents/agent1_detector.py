"""
agent1_detector.py — MedSignal Agent 1: Signal Detector

Role in pipeline:
    Receives a flagged signal from state (loaded from signals_flagged by pipeline.py).
    stat_score is already in state — computed by Branch 2 and stored in
    signals_flagged. pipeline.py loads it into state at Stage 0.
    Agent 1 does not recompute it.

    Calls GPT-4o (via LLMRouter) to generate 3 PubMed search queries.
    Falls back to Claude Haiku automatically via LLMRouter MODEL_CHAIN.
    Falls back to clean generic template queries if all LLM calls fail.

Fallback chain (handled by LLMRouter — no extra code needed here):
    1. GPT-4o / gpt-4o-mini  (PRIMARY_MODEL in .env)
    2. Claude Haiku           (FALLBACK_MODEL in .env)
    3. Template queries       (caught in generate_queries except block)

Why GPT-4o for query generation:
    Template queries are too generic — same structure regardless of drug class.
    GPT-4o's biomedical pretraining generates domain-specific terminology
    including pharmacological class terms that significantly improve ChromaDB
    retrieval. This works for any drug — not just the 10 golden signal drugs.

    Example: empagliflozin + diabetic ketoacidosis
        Template:  "empagliflozin diabetic ketoacidosis mechanism pharmacology"
        GPT-4o:    "empagliflozin SGLT2 inhibitor euglycemic ketoacidosis mechanism"
                   "empagliflozin diabetic ketoacidosis incidence risk factors"
                   "empagliflozin ketoacidosis outcomes hospitalisation management"

Template fallback:
    Clean generic queries — no drug class map, no hardcoded terms.
    Works for any drug. Honest about being a generic fallback.
    GPT-4o handles class specificity — template does not try to replicate it.

Token config (all owned by LLMRouter TASK_CONFIG — not duplicated here):
    task="query_generation" → temperature=0, max_tokens=200
"""

import json
import logging
import os
from typing import Optional

from dotenv import load_dotenv
from app.core.llm_router import LLMRouter

from app.agents.state import SignalState

load_dotenv()

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

# Number of search queries to generate per signal
NUM_QUERIES = 3

# Minimum words per query — single or two-word queries return irrelevant results
MIN_QUERY_WORDS = 3

# ── System prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a pharmacovigilance literature search specialist.

Given a drug name, adverse reaction, and signal statistics, generate exactly 3
PubMed search queries to retrieve the most relevant clinical and safety literature.

Rules:
- Use ONLY plain keyword strings — no MeSH tags, no boolean operators, no field qualifiers
- Each query must be 4-8 words long
- If you know the drug's pharmacological class (e.g. SGLT2 inhibitor, GLP-1 agonist,
  IL-4 receptor antagonist), include it in at least one query
- If you do not know the drug's class, use the drug name only — do not invent a class
- Each query must approach the signal from a different angle:
    Query 1 — mechanistic: why does this drug cause this reaction biologically?
    Query 2 — epidemiological: how common is this reaction? incidence, risk factors
    Query 3 — clinical outcomes: how serious? hospitalisation, mortality, management
- Return ONLY a JSON array of exactly 3 strings. No explanation. No markdown.

Example input:
Drug: dupilumab
Reaction: conjunctivitis
PRR: 8.43 based on 412 cases
Severity: 12 hospitalisations, 3 life-threatening

Example output:
["dupilumab IL-4 receptor conjunctivitis ocular mechanism",
 "dupilumab conjunctivitis incidence risk factors biologic",
 "dupilumab conjunctivitis outcomes severity management treatment"]"""


# ── Template fallback ─────────────────────────────────────────────────────────

def _template_queries(drug_key: str, pt: str) -> list[str]:
    """
    Generic fallback queries used when all LLM calls fail.
    No drug class map — works for any drug.
    Always returns exactly 3 queries covering mechanistic,
    epidemiological, and clinical outcomes angles.
    """
    return [
        f"{drug_key} {pt} mechanism pharmacology adverse reaction",
        f"{drug_key} {pt} incidence risk factors clinical trial safety",
        f"{drug_key} {pt} outcomes severity hospitalisation mortality",
    ]


# ── Severity context builder ──────────────────────────────────────────────────

def _build_severity_str(state: dict) -> str:
    """
    Builds a human-readable severity string from outcome flags in state.
    Included in the GPT-4o user message to calibrate query specificity.
    Returns "no serious outcomes reported" when all flags are zero.
    """
    parts = []
    if state.get("death_count", 0) > 0:
        parts.append(f"{state['death_count']} deaths")
    if state.get("lt_count", 0) > 0:
        parts.append(f"{state['lt_count']} life-threatening")
    if state.get("hosp_count", 0) > 0:
        parts.append(f"{state['hosp_count']} hospitalisations")
    return ", ".join(parts) if parts else "no serious outcomes reported"


# ── Query validation ──────────────────────────────────────────────────────────

def _validate_queries(queries: object) -> bool:
    """
    Returns True if queries is a list of exactly NUM_QUERIES non-empty
    strings each with at least MIN_QUERY_WORDS words, all unique.
    """
    return (
        isinstance(queries, list)
        and len(queries) == NUM_QUERIES
        and all(isinstance(q, str) and q.strip() for q in queries)
        and all(len(q.strip().split()) >= MIN_QUERY_WORDS for q in queries)
        and len(set(q.strip() for q in queries)) == NUM_QUERIES  # all unique
    )


# ── GPT-4o / Claude query generation ─────────────────────────────────────────

def generate_queries(
    drug_key  : str,
    pt        : str,
    prr       : float,
    case_count: int,
    state     : dict = None,
) -> list[str]:
    """
    Generate 3 PubMed search queries via LLMRouter (GPT-4o → Claude → template).

    LLMRouter handles the fallback chain automatically:
        1. PRIMARY_MODEL  (gpt-4o-mini default, or gpt-4o via OPENAI_MODEL in .env)
        2. FALLBACK_MODEL (claude-haiku via FALLBACK_MODEL in .env)
        3. Raises RuntimeError if both fail → caught here → template fallback

    Args:
        drug_key   : canonical drug name e.g. "dupilumab"
        pt         : MedDRA preferred term e.g. "conjunctivitis"
        prr        : Proportional Reporting Ratio
        case_count : number of cases (A in PRR formula)
        state      : full SignalState dict — used to extract severity flags
                     and the shared LLMRouter instance. Optional for tests.

    Returns:
        List of exactly 3 search query strings.
        Falls back to template queries on any LLM error.
    """
    severity_str = _build_severity_str(state) if state else "no serious outcomes reported"

    user_message = (
        f"Drug: {drug_key}\n"
        f"Reaction: {pt}\n"
        f"PRR: {prr:.2f} based on {case_count} cases\n"
        f"Severity: {severity_str}"
    )

    log.info(
        "agent1_call drug=%s pt=%s prr=%.2f cases=%d severity=%s",
        drug_key, pt, prr, case_count, severity_str,
    )

    try:
        # Use shared router from state if available; create a fresh one for
        # direct test calls. Budget tracking only works with the shared instance.
        router = (state.get("router") if state else None) or LLMRouter()

        response = router.complete(
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": user_message},
            ],
            task="query_generation",
        )

        # Log which model actually responded (primary or fallback)
        model_used  = getattr(response, "model", "unknown")
        tokens_used = response.usage.total_tokens

        log.info(
            "agent1_response drug=%s model_used=%s tokens=%d",
            drug_key, model_used, tokens_used,
        )

        # Emit Prometheus metrics if available
        try:
            from app.observability.metrics import LLM_TOKENS_USED
            LLM_TOKENS_USED.labels(agent="agent1", type="input").inc(
                response.usage.prompt_tokens
            )
            LLM_TOKENS_USED.labels(agent="agent1", type="output").inc(
                response.usage.completion_tokens
            )
        except Exception:
            pass

        raw_text = response.choices[0].message.content.strip()

        # Strip markdown fences if model added them despite instructions
        clean = raw_text
        if clean.startswith("```"):
            clean = clean.split("```")[1]
            if clean.startswith("json"):
                clean = clean[4:]
            clean = clean.strip()

        queries = json.loads(clean)

        if _validate_queries(queries):
            log.info(
                "agent1_queries_ok drug=%s model=%s\n  Q1: %s\n  Q2: %s\n  Q3: %s",
                drug_key, model_used, queries[0], queries[1], queries[2],
            )
            return [q.strip() for q in queries]

        log.warning(
            "agent1_invalid_format drug=%s model=%s raw=%r — template fallback",
            drug_key, model_used, raw_text,
        )
        return _template_queries(drug_key, pt)

    except json.JSONDecodeError as e:
        log.warning(
            "agent1_json_error drug=%s error=%s — template fallback",
            drug_key, e,
        )
        return _template_queries(drug_key, pt)

    except RuntimeError as e:
        # LLMRouter raises RuntimeError when ALL models in MODEL_CHAIN fail
        # (budget exceeded or both primary + fallback unavailable)
        log.error(
            "agent1_all_models_failed drug=%s error=%s — template fallback",
            drug_key, e,
        )
        return _template_queries(drug_key, pt)

    except Exception as e:
        log.error(
            "agent1_unexpected_error drug=%s error=%s — template fallback",
            drug_key, e,
        )
        return _template_queries(drug_key, pt)


# ── LangGraph node ────────────────────────────────────────────────────────────

def agent1_node(state: SignalState) -> dict:
    """
    LangGraph node for Agent 1.

    Reads from state (all loaded from signals_flagged by pipeline.py):
        drug_key    — canonical drug name
        pt          — MedDRA preferred term
        prr         — Proportional Reporting Ratio
        case_count  — number of cases (A in PRR formula)
        stat_score  — already in state, not recomputed here
        death_count, hosp_count, lt_count — severity context for prompt

    Adds to state:
        search_queries — 3 PubMed search queries for Agent 2

    Returns dict with search_queries only — LangGraph merges into state.
    """
    drug_key   = state["drug_key"]
    pt         = state["pt"]
    prr        = state["prr"]
    case_count = state["case_count"]
    stat_score = state["stat_score"]

    log.info(
        "agent1_start drug=%s pt=%s prr=%.2f cases=%d stat_score=%.4f",
        drug_key, pt, prr, case_count, stat_score,
    )

    search_queries = generate_queries(
        drug_key   = drug_key,
        pt         = pt,
        prr        = prr,
        case_count = case_count,
        state      = state,
    )

    log.info(
        "agent1_complete drug=%s pt=%s queries=%s",
        drug_key, pt, search_queries,
    )

    return {"search_queries": search_queries}