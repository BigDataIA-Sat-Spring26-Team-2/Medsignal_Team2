"""
agent1_detector.py — MedSignal Agent 1: Signal Detector

Role in pipeline:
    Reads a flagged signal from state (loaded from signals_flagged by pipeline.py).
    Reads stat_score from state — already computed by Branch 2, not recomputed here.
    Calls GPT-4o to generate 3 PubMed search queries for the signal.
    Falls back to templates if GPT-4o fails or returns malformed JSON.
    Passes search_queries to Agent 2 for ChromaDB hybrid retrieval.

Why GPT-4o for query generation:
    Template queries are too generic — same structure regardless of drug class.
    GPT-4o's biomedical pretraining generates domain-specific terminology
    that makes ChromaDB retrieval significantly more effective.

    Example: dupilumab + conjunctivitis
        Template:  "dupilumab conjunctivitis adverse effects"  (generic)
        GPT-4o:    "dupilumab conjunctivitis ocular adverse effects mechanism"
                   "dupilumab eye inflammation incidence epidemiology"
                   "dupilumab conjunctivitis clinical outcomes risk factors"

    The three queries approach the signal from different angles:
        Query 1 — mechanistic   (why does this drug cause this reaction?)
        Query 2 — epidemiological (how common is it?)
        Query 3 — clinical outcomes (how serious is it?)

Why no LLM for StatScore:
    StatScore is a deterministic formula computed in Branch 2 from FAERS-derived
    features. It does not require language understanding. Agent 1 reads it
    from state and passes it through unchanged.

GPT-4o config:
    temperature=0  — reproducible outputs across runs
    max_tokens=200 — only needs 3 short queries, ~80 tokens response
    model=gpt-4o   — final demo; use gpt-4o-mini during development
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

MODEL_NAME = os.getenv("OPENAI_MODEL", "gpt-4o")

# Temperature 0 for reproducibility — same signal always produces same queries
TEMPERATURE = 0

# Max tokens for query generation — 3 short queries need ~80 tokens response
MAX_TOKENS = 200

# Number of search queries to generate per signal
NUM_QUERIES = 3

# System prompt — instructs GPT-4o to act as a pharmacovigilance expert
# and generate queries covering three angles: mechanistic, epidemiological,
# and clinical outcomes.
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
    Deterministic fallback queries used when GPT-4o fails or returns
    malformed JSON.

    Always returns exactly 3 queries so Agent 2 always has something
    to query ChromaDB with — even if the signal is not in ChromaDB,
    returning empty abstracts is handled gracefully by Agent 2.
    """
    return [
        f"{drug_key} {pt} adverse effects safety",
        f"{drug_key} {pt} mechanism clinical",
        f"{drug_key} {pt} risk factors outcomes",
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
        Falls back to template queries on any error.
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

        # Parse JSON array from response
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

    Reads from state:
        drug_key    — canonical drug name
        pt          — MedDRA preferred term
        prr         — Proportional Reporting Ratio
        case_count  — number of cases (A in PRR formula)
        stat_score  — already computed by Branch 2, passed through unchanged

    Adds to state:
        search_queries — 3 PubMed search queries for Agent 2

    Agent 1 does NOT write to any database table.
    It only populates search_queries and returns the updated state fragment.
    Agent 2 uses search_queries to query ChromaDB.

    Returns:
        Dict with search_queries key only — LangGraph merges this into state.
    """
    drug_key   = state["drug_key"]
    pt         = state["pt"]
    prr        = state["prr"]
    case_count = state["case_count"]
    stat_score = state.get("stat_score")

    log.info(
        "agent1_start drug=%s pt=%s prr=%.2f cases=%d stat_score=%s",
        drug_key, pt, prr, case_count, stat_score,
    )

    search_queries = generate_queries(drug_key, pt, prr, case_count)

    log.info(
        "agent1_complete drug=%s pt=%s queries=%s",
        drug_key, pt, search_queries,
    )

    return {"search_queries": search_queries}