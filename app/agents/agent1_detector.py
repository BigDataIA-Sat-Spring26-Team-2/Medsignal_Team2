"""
agent1_detector.py — MedSignal Agent 1: Signal Detector

Role in pipeline:
    Receives a flagged signal from state (loaded from signals_flagged by pipeline.py).
    stat_score is already in state — computed by Branch 2 and stored in
    signals_flagged. pipeline.py loads it into state at Stage 0.
    Agent 1 does not recompute it.

    Calls GPT-4o to generate 3 PubMed search queries for the signal.
    Falls back to angle-specific template queries if GPT-4o fails.
    Writes search_queries to state for Agent 2.

Why GPT-4o for query generation:
    Template queries are too generic — same structure regardless of drug class.
    GPT-4o's biomedical pretraining generates domain-specific terminology
    that makes ChromaDB retrieval significantly more effective.

    Example: dupilumab + conjunctivitis
        Template:  "dupilumab conjunctivitis adverse drug reaction"  (generic)
        GPT-4o:    "dupilumab conjunctivitis ocular adverse effects mechanism"
                   "dupilumab eye inflammation incidence epidemiology"
                   "dupilumab conjunctivitis clinical outcomes risk factors"

    The three queries approach the signal from different angles:
        Query 1 — mechanistic   (why does this drug cause this reaction?)
        Query 2 — epidemiological (how common is it?)
        Query 3 — clinical outcomes (how serious is it?)

GPT-4o config:
    temperature=0      — reproducible outputs across runs
    max_tokens=200     — only needs 3 short queries, ~80 tokens response
    default model      — gpt-4o-mini (development and testing)
                         Set OPENAI_MODEL=gpt-4o in .env for production.
                         With 10 signals and multiple debug runs, gpt-4o-mini
                         stays well under the $10 hard limit.
"""

import json
import logging
import os
from typing import Optional

from dotenv import load_dotenv
from openai import OpenAI

from app.agents.state import SignalState

load_dotenv()

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

# Default is gpt-4o-mini — cost-effective for development and debugging.
# Set OPENAI_MODEL=gpt-4o in .env only for production.
MODEL_NAME = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# Temperature 0 for reproducibility — same signal always produces same queries
TEMPERATURE = 0

# Max tokens for query generation — 3 short queries need ~80 tokens response
MAX_TOKENS = 200

# Number of search queries to generate per signal
NUM_QUERIES = 3

# System prompt — instructs GPT-4o to generate queries covering three angles:
# mechanistic, epidemiological, and clinical outcomes.
SYSTEM_PROMPT = """You are a pharmacovigilance expert. Given a drug name and adverse \
reaction, generate exactly 3 PubMed search queries that would retrieve the most \
relevant clinical and safety literature.

Each query should approach the topic from a different angle:
1. Mechanistic — why does this drug cause this reaction?
2. Epidemiological — how common is this reaction with this drug?
3. Clinical outcomes — how serious are the outcomes?

Return only the 3 queries as a JSON array. No explanation. No markdown. Example:
["dupilumab conjunctivitis ocular adverse effects mechanism",
 "dupilumab eye inflammation incidence epidemiology",
 "dupilumab conjunctivitis clinical outcomes risk factors"]"""


# ── Lazy initialization ───────────────────────────────────────────────────────
# OpenAI client loads on first call, not at import time.
# Allows unit tests to import pure logic functions without OPENAI_API_KEY set.

_CLIENT: Optional[OpenAI] = None


def _get_client() -> OpenAI:
    """
    Lazy loader for the OpenAI client.
    Loads once on first call, reuses on subsequent calls.
    Fails with a clear error if OPENAI_API_KEY is not set.
    """
    global _CLIENT
    if _CLIENT is None:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError(
                "OPENAI_API_KEY not set in .env — "
                "Agent 1 cannot call GPT-4o without it."
            )
        _CLIENT = OpenAI(api_key=api_key)
        log.info("OpenAI client initialised model=%s", MODEL_NAME)
    return _CLIENT


# ── Template fallback ─────────────────────────────────────────────────────────

def _template_queries(drug_key: str, pt: str) -> list[str]:
    """
    Angle-specific fallback queries used when GPT-4o fails or returns
    malformed JSON.

    Each query covers a different retrieval angle — mechanistic,
    epidemiological, and clinical outcomes — mirroring the GPT-4o
    instruction structure so Agent 2 gets diverse retrieval coverage
    even without a live API call.

    Always returns exactly 3 queries so Agent 2 always has something
    to query ChromaDB with.
    """
    return [
        f"{drug_key} {pt} adverse drug reaction mechanism pharmacology",
        f"{drug_key} {pt} incidence epidemiology clinical trial safety",
        f"{drug_key} {pt} outcomes hospitalisation mortality risk factors",
    ]


# ── GPT-4o query generation ───────────────────────────────────────────────────

def generate_queries(
    drug_key: str,
    pt: str,
    prr: float,
    case_count: int,
) -> list[str]:
    """
    Call GPT-4o to generate 3 PubMed search queries for a drug-reaction signal.

    Prompt includes PRR and case_count so GPT-4o understands the statistical
    context — a signal with PRR=15 and 200 cases warrants different queries
    than PRR=2.1 with 50 cases.

    Args:
        drug_key   : canonical drug name e.g. "dupilumab"
        pt         : MedDRA preferred term e.g. "conjunctivitis"
        prr        : Proportional Reporting Ratio
        case_count : number of cases (A in PRR formula)

    Returns:
        List of exactly 3 search query strings.
        Falls back to angle-specific template queries on any error.
    """
    user_message = (
        f"Drug: {drug_key}\n"
        f"Reaction: {pt}\n"
        f"PRR: {prr:.2f} based on {case_count} cases"
    )

    log.info(
        "agent1_gpt4o_call drug=%s pt=%s prr=%.2f cases=%d model=%s",
        drug_key, pt, prr, case_count, MODEL_NAME,
    )

    try:
        client   = _get_client()
        response = client.chat.completions.create(
            model      = MODEL_NAME,
            temperature= TEMPERATURE,
            max_tokens = MAX_TOKENS,
            messages   = [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": user_message},
            ],
        )

        raw_text = response.choices[0].message.content.strip()

        log.info(
            "agent1_gpt4o_response drug=%s tokens_used=%d",
            drug_key,
            response.usage.total_tokens,
        )
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

        # Strip markdown fences if model added them despite instructions
        clean = raw_text.strip()
        if clean.startswith("```"):
            clean = clean.split("```")[1]
            if clean.startswith("json"):
                clean = clean[4:]
            clean = clean.strip()

        queries = json.loads(clean)

        # Validate — must be a list of exactly 3 non-empty strings
        if (
            isinstance(queries, list)
            and len(queries) == NUM_QUERIES
            and all(isinstance(q, str) and q.strip() for q in queries)
        ):
            log.info(
                "agent1_queries_generated drug=%s queries=%s",
                drug_key, queries,
            )
            return [q.strip() for q in queries]

        log.warning(
            "agent1_gpt4o_invalid_format drug=%s raw=%s — using template fallback",
            drug_key, raw_text,
        )
        return _template_queries(drug_key, pt)

    except json.JSONDecodeError as e:
        log.warning(
            "agent1_json_parse_error drug=%s error=%s — using template fallback",
            drug_key, e,
        )
        return _template_queries(drug_key, pt)

    except Exception as e:
        log.error(
            "agent1_gpt4o_failed drug=%s error=%s — using template fallback",
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
        stat_score  — already in state, computed by Branch 2 and stored
                      in signals_flagged. Not recomputed here.

    Adds to state:
        search_queries — 3 PubMed search queries for Agent 2

    Agent 1 does NOT write to any database table.
    Agent 2 uses search_queries to query ChromaDB.
    Agent 3 uses stat_score (already in state) for priority tier and SafetyBrief.

    Returns:
        Dict with search_queries only — LangGraph merges this into state.
    """
    drug_key   = state["drug_key"]
    pt         = state["pt"]
    prr        = state["prr"]
    case_count = state["case_count"]
    stat_score = state["stat_score"]   # loaded from signals_flagged, not recomputed

    log.info(
        "agent1_start drug=%s pt=%s prr=%.2f cases=%d stat_score=%.4f",
        drug_key, pt, prr, case_count, stat_score,
    )

    search_queries = generate_queries(drug_key, pt, prr, case_count)

    log.info(
        "agent1_complete drug=%s pt=%s queries=%s",
        drug_key, pt, search_queries,
    )

    return {"search_queries": search_queries}