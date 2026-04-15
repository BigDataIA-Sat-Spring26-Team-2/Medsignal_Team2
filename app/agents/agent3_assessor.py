"""
agent3_assessor.py — Agent 3: Priority Tier Assignment + SafetyBrief Generation

Receives state populated by Agent 1 (stat_score) and Agent 2 (abstracts, lit_score).
Assigns a P1-P4 priority tier, calls GPT-4o to synthesise a SafetyBrief,
validates output with Pydantic, strips fabricated PMIDs, and writes to Snowflake.

Retry logic: one retry on Pydantic failure with the validation error in prompt.
On second failure: writes generation_error=True so HITL still sees the signal.

Owner: Siddharth
"""

import json
import logging
import math
import os
from datetime import datetime, timezone
from typing import List, Literal, Optional

import snowflake.connector
from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field, ValidationError

from app.agents.state import SignalState

load_dotenv()

log    = logging.getLogger(__name__)
client = OpenAI()
MODEL  = "gpt-4o"


# ── Pydantic schema ───────────────────────────────────────────────────────────
# Every GPT-4o response is validated against this before being written.
# ValidationError on attempt 1 → retry. On attempt 2 → generation_error.

class SafetyBriefOutput(BaseModel):
    brief_text        : str
    key_findings      : List[str]
    pmids_cited       : List[str]
    recommended_action: Literal["MONITOR", "LABEL_UPDATE", "RESTRICT", "WITHDRAW"]
    drug_key          : str
    pt                : str
    stat_score        : float = Field(ge=0.0, le=1.0)
    lit_score         : float = Field(ge=0.0, le=1.0)
    priority          : Literal["P1", "P2", "P3", "P4"]
    generated_at      : str


# ── Snowflake ─────────────────────────────────────────────────────────────────

def _get_conn() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account  =os.getenv("SNOWFLAKE_ACCOUNT"),
        user     =os.getenv("SNOWFLAKE_USER"),
        password =os.getenv("SNOWFLAKE_PASSWORD"),
        database =os.getenv("SNOWFLAKE_DATABASE"),
        schema   =os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )


# ── Priority tier ─────────────────────────────────────────────────────────────
# Proposal p28. StatScore and LitScore are never combined —
# each threshold is evaluated independently.

def assign_priority(stat_score: float, lit_score: float) -> str:
    if stat_score >= 0.7 and lit_score >= 0.5:
        return "P1"
    if stat_score >= 0.7:
        return "P2"
    if lit_score >= 0.5:
        return "P3"
    return "P4"


# ── StatScore fallback ────────────────────────────────────────────────────────
# Used when Agent 1 has not run yet (still mocked in pipeline).
# Identical formula to Branch 2 compute_stat_score so values are consistent.

def _compute_stat_score(
    prr: float, case_count: int,
    death: int, lt: int, hosp: int,
) -> float:
    prr_s = min(prr / 4.0, 1.0)
    vol_s = min(math.log10(max(case_count, 1)) / math.log10(50), 1.0)
    sev_s = 1.0 if death else 0.75 if lt else 0.50 if hosp else 0.0
    return round(prr_s * 0.50 + vol_s * 0.30 + sev_s * 0.20, 4)


# ── Citation guard ────────────────────────────────────────────────────────────
# GPT-4o sometimes cites PMIDs it was not given.
# Strip anything not in the set Agent 2 actually retrieved.

def _validate_citations(
    brief: SafetyBriefOutput,
    retrieved_pmids: List[str],
) -> SafetyBriefOutput:
    retrieved  = set(retrieved_pmids)
    cleaned    = [p for p in brief.pmids_cited if p in retrieved]
    fabricated = set(brief.pmids_cited) - retrieved

    if fabricated:
        log.warning(
            "Stripped %d fabricated PMID(s) from %s x %s: %s",
            len(fabricated), brief.drug_key, brief.pt, fabricated,
        )

    return brief.model_copy(update={"pmids_cited": cleaned})


# ── Prompt construction ───────────────────────────────────────────────────────

def _format_abstracts(abstracts: list) -> str:
    """Format Agent 2's retrieved abstracts for inclusion in the GPT-4o prompt."""
    if not abstracts:
        return "No abstracts retrieved above similarity threshold."

    lines = []
    for i, a in enumerate(abstracts, 1):
        # Agent 2 stores similarity as 1 - cosine_distance (higher = more relevant)
        lines.append(
            f"[{i}] PMID:{a['pmid']} | similarity={a['similarity']:.3f}\n"
            f"{a['text'][:600]}"
        )
    return "\n\n".join(lines)


def _build_prompt(state: SignalState, priority: str) -> str:
    return f"""Drug: {state["drug_key"]}
Reaction (MedDRA PT): {state["pt"]}
PRR: {state["prr"]:.2f} | Cases: {state["case_count"]}
Deaths: {state["death_count"]} | Hospitalizations: {state["hosp_count"]} | Life-threatening: {state["lt_count"]}
StatScore: {state["stat_score"]:.4f} | LitScore: {state["lit_score"]:.4f}
Priority: {priority}

Retrieved abstracts — cite using [PMID:xxxxxxxx] inline in brief_text:
{_format_abstracts(state.get("abstracts") or [])}

Return ONLY a JSON object. No markdown, no explanation, no extra text.
{{
    "brief_text": "2-3 paragraph clinical narrative. Cite PMIDs inline.",
    "key_findings": ["finding 1", "finding 2", "finding 3"],
    "pmids_cited": ["pmid1", "pmid2"],
    "recommended_action": "MONITOR or LABEL_UPDATE or RESTRICT or WITHDRAW",
    "drug_key": "{state["drug_key"]}",
    "pt": "{state["pt"]}",
    "stat_score": {state["stat_score"]:.4f},
    "lit_score": {state["lit_score"]:.4f},
    "priority": "{priority}",
    "generated_at": "{datetime.now(timezone.utc).isoformat()}"
}}"""


def _build_retry_prompt(state: SignalState, priority: str, error: str) -> str:
    """Retry prompt includes the exact Pydantic error so GPT-4o knows what to fix."""
    return f"""Your previous response failed schema validation with this error:
{error}

Allowed values for recommended_action: MONITOR, LABEL_UPDATE, RESTRICT, WITHDRAW
pmids_cited must only contain PMIDs from the abstracts provided below.
key_findings must be a non-empty list of strings.
stat_score and lit_score must be floats between 0.0 and 1.0.

{_build_prompt(state, priority)}"""


# ── GPT-4o call ───────────────────────────────────────────────────────────────

def _call_gpt4o(prompt: str) -> tuple[dict, int, int]:
    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a pharmacovigilance medical writer. "
                    "Write safety briefs grounded only in the evidence provided. "
                    "Do not introduce claims beyond what the abstracts support. "
                    "Return only valid JSON. No markdown, no preamble."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
    )

    raw        = response.choices[0].message.content.strip()
    input_tok  = response.usage.prompt_tokens
    output_tok = response.usage.completion_tokens

    # GPT-4o occasionally wraps JSON in ```json``` blocks despite instructions.
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    try:
        return json.loads(raw), input_tok, output_tok
    except json.JSONDecodeError as e:
        raise ValueError(f"GPT-4o did not return valid JSON: {e}\nOutput: {raw[:300]}")


# ── Snowflake writer ──────────────────────────────────────────────────────────
# MERGE so re-running the pipeline on the same signal updates rather than
# failing on the (drug_key, pt) uniqueness constraint.
# key_findings and pmids_cited are VARIANT columns — PARSE_JSON() converts
# the JSON string into Snowflake's native semi-structured type.

def _write_to_snowflake(
    state     : SignalState,
    priority  : str,
    brief     : Optional[SafetyBriefOutput],
    input_tok : int,
    output_tok: int,
    gen_error : bool,
) -> None:
    conn = _get_conn()
    cur  = conn.cursor()

    cur.execute(
        """
        MERGE INTO safety_briefs AS target
        USING (
            SELECT
                %s                AS drug_key,
                %s                AS pt,
                %s                AS stat_score,
                %s                AS lit_score,
                %s                AS priority,
                %s                AS brief_text,
                PARSE_JSON(%s)    AS key_findings,
                PARSE_JSON(%s)    AS pmids_cited,
                %s                AS recommended_action,
                %s                AS model_used,
                %s                AS input_tokens,
                %s                AS output_tokens,
                %s                AS generation_error,
                CURRENT_TIMESTAMP() AS generated_at
        ) AS source
        ON  target.drug_key = source.drug_key
        AND target.pt       = source.pt
        WHEN MATCHED THEN UPDATE SET
            stat_score         = source.stat_score,
            lit_score          = source.lit_score,
            priority           = source.priority,
            brief_text         = source.brief_text,
            key_findings       = source.key_findings,
            pmids_cited        = source.pmids_cited,
            recommended_action = source.recommended_action,
            model_used         = source.model_used,
            input_tokens       = source.input_tokens,
            output_tokens      = source.output_tokens,
            generation_error   = source.generation_error,
            generated_at       = source.generated_at
        WHEN NOT MATCHED THEN INSERT (
            drug_key, pt, stat_score, lit_score, priority,
            brief_text, key_findings, pmids_cited, recommended_action,
            model_used, input_tokens, output_tokens,
            generation_error, generated_at
        ) VALUES (
            source.drug_key,    source.pt,         source.stat_score,
            source.lit_score,   source.priority,   source.brief_text,
            source.key_findings, source.pmids_cited, source.recommended_action,
            source.model_used,  source.input_tokens, source.output_tokens,
            source.generation_error, source.generated_at
        )
        """,
        (
            state["drug_key"],
            state["pt"],
            state.get("stat_score"),
            state.get("lit_score"),
            priority,
            brief.brief_text                       if brief else None,
            json.dumps(brief.key_findings)         if brief else json.dumps([]),
            json.dumps(brief.pmids_cited)          if brief else json.dumps([]),
            brief.recommended_action               if brief else None,
            MODEL,
            input_tok,
            output_tok,
            gen_error,
        ),
    )

    conn.commit()
    cur.close()
    conn.close()

    log.info(
        "safety_briefs — %s x %s | priority=%s | gen_error=%s | tokens=%d",
        state["drug_key"], state["pt"], priority,
        gen_error, input_tok + output_tok,
    )


# ── LangGraph node ────────────────────────────────────────────────────────────

def agent3_node(state: SignalState) -> dict:
    """
    LangGraph node for Agent 3.

    stat_score comes from Agent 1. If Agent 1 is still mocked and has not
    put a value in state, we compute it here from the raw signal columns
    using the same formula as Branch 2 so values are consistent end to end.

    Returns a dict of new state fields — LangGraph merges this into
    the existing state automatically.
    """
    drug_key = state["drug_key"]
    pt       = state["pt"]

    # Resolve stat_score — Agent 1's value takes precedence
    stat_score = state.get("stat_score")
    if stat_score is None:
        stat_score = _compute_stat_score(
            state["prr"], state["case_count"],
            state["death_count"], state["lt_count"], state["hosp_count"],
        )
        log.info(
            "stat_score not in state for %s x %s — computed locally: %.4f",
            drug_key, pt, stat_score,
        )

    lit_score = state.get("lit_score") or 0.0
    abstracts = state.get("abstracts") or []

    # Write resolved scores back into state so prompt and Pydantic see same values
    state = {**state, "stat_score": stat_score, "lit_score": lit_score}

    log.info(
        "agent3_start — %s x %s | stat=%.4f | lit=%.4f | abstracts=%d",
        drug_key, pt, stat_score, lit_score, len(abstracts),
    )

    priority        = assign_priority(stat_score, lit_score)
    retrieved_pmids = [a.get("pmid", "") for a in abstracts]
    input_tok       = 0
    output_tok      = 0
    brief           = None
    gen_error       = False
    last_error      = ""

    for attempt in range(2):
        try:
            if attempt == 0:
                prompt = _build_prompt(state, priority)
            else:
                log.warning(
                    "Attempt 1 failed for %s x %s — retrying with error context",
                    drug_key, pt,
                )
                prompt = _build_retry_prompt(state, priority, last_error)

            raw, i_tok, o_tok = _call_gpt4o(prompt)
            input_tok  += i_tok
            output_tok += o_tok

            # Enforce fields GPT-4o must not change — overwrite before validation
            raw["drug_key"]     = drug_key
            raw["pt"]           = pt
            raw["stat_score"]   = stat_score
            raw["lit_score"]    = lit_score
            raw["priority"]     = priority
            raw["generated_at"] = datetime.now(timezone.utc).isoformat()

            brief = SafetyBriefOutput(**raw)
            brief = _validate_citations(brief, retrieved_pmids)

            log.info(
                "agent3_success — %s x %s | attempt=%d | pmids=%d | action=%s",
                drug_key, pt, attempt + 1,
                len(brief.pmids_cited), brief.recommended_action,
            )
            break

        except (ValidationError, ValueError, Exception) as exc:
            last_error = str(exc)
            log.warning("agent3 attempt %d failed — %s", attempt + 1, last_error)

            if attempt == 1:
                log.error(
                    "Both attempts failed for %s x %s — writing generation_error",
                    drug_key, pt,
                )
                gen_error = True
                brief     = None

    _write_to_snowflake(
        state, priority, brief,
        input_tok, output_tok, gen_error,
    )

    return {
        "priority": priority,
        "brief"   : brief.model_dump() if brief else None,
        "error"   : last_error if gen_error else None,
    }