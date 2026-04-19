"""
agent3_assessor.py — Agent 3: Priority Tier Assignment + SafetyBrief Generation

Receives state from the pipeline. stat_score is computed by Branch 2 and
stored in signals_flagged — Agent 1 reads it from there and passes it through
state. abstracts and lit_score come from Agent 2.

The local _compute_stat_score() fallback only fires if Agent 1 has not run.
Retry logic: one retry on Pydantic failure with the validation error in prompt.
On second failure: writes generation_error=True so HITL still sees the signal.
"""

import json
import logging
import math
import os
from datetime import datetime, timezone
from typing import List, Optional
from app.utils.redis_client import invalidate_brief
from app.utils.snowflake_client import get_conn

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import ValidationError

from app.agents.state import SignalState
from app.models.brief import SafetyBriefOutput

load_dotenv()

log    = logging.getLogger(__name__)
client = OpenAI()

# Use gpt-4o-mini during development — switch to gpt-4o for the final
# evaluation run by setting OPENAI_MODEL=gpt-4o in .env.
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# Valid recommended_action values — used for normalization before Pydantic.
_VALID_ACTIONS = {"MONITOR", "LABEL_UPDATE", "RESTRICT", "WITHDRAW"}


# ── Priority tier ─────────────────────────────────────────────────────────────
# Proposal p28. StatScore and LitScore evaluated independently — never combined.

def assign_priority(stat_score: float, lit_score: float) -> str:
    if stat_score >= 0.7 and lit_score >= 0.5:
        return "P1"
    if stat_score >= 0.7:
        return "P2"
    if lit_score >= 0.5:
        return "P3"
    return "P4"


# ── StatScore fallback ────────────────────────────────────────────────────────
# Only called when Agent 1 has not populated stat_score in state.
# Uses the same formula as Branch 2 so values are consistent.

def _compute_stat_score(
    prr: float, case_count: int,
    death: int, lt: int, hosp: int,
) -> float:
    prr_s = min(prr / 4.0, 1.0)
    vol_s = min(math.log10(max(case_count, 1)) / math.log10(50), 1.0)
    sev_s = 1.0 if death else 0.75 if lt else 0.50 if hosp else 0.0
    return round(prr_s * 0.50 + vol_s * 0.30 + sev_s * 0.20, 4)


# ── Action normalization ──────────────────────────────────────────────────────
# GPT-4o sometimes returns prose like "Escalate for pharmacovigilance review"
# instead of one of the four exact Literal values. This maps common variants
# to valid values before Pydantic validation runs, preventing generation_error
# on every signal.

def _normalize_action(raw: dict) -> dict:
    action = str(raw.get("recommended_action", "")).upper().strip()

    # Hard guard — empty string or whitespace-only must not reach Pydantic.
    # This happens when GPT-4o returns the JSON template placeholder text
    # ("WITHDRAW or RESTRICT or LABEL_UPDATE or MONITOR") or an empty field.
    if not action:
        raw["recommended_action"] = "MONITOR"
        log.warning(
            "recommended_action was empty — defaulting to MONITOR"
        )
        return raw

    # Template placeholder — GPT-4o occasionally returns the example string
    # from the prompt verbatim. Treat as no decision made → MONITOR.
    if "OR" in action and len(action) > 20:
        raw["recommended_action"] = "MONITOR"
        log.warning(
            "recommended_action looked like a template placeholder '%s' "
            "— defaulting to MONITOR",
            action[:60],
        )
        return raw

    if action in _VALID_ACTIONS:
        return raw

    if "WITHDRAW" in action or "REMOVE" in action:
        normalized = "WITHDRAW"
    elif "RESTRICT" in action or "LIMIT" in action:
        normalized = "RESTRICT"
    elif "LABEL" in action or "UPDATE" in action or "REVISE" in action:
        normalized = "LABEL_UPDATE"
    else:
        # Default to MONITOR — least disruptive safe fallback.
        # Logged as warning so the reviewer knows normalization happened.
        normalized = "MONITOR"
        log.warning(
            "recommended_action '%s' did not match any known value — "
            "defaulting to MONITOR",
            action,
        )

    raw["recommended_action"] = normalized
    return raw


# ── Citation guard ────────────────────────────────────────────────────────────
# Every PMID in pmids_cited must appear in the set Agent 2 actually retrieved.
# Anything else is a hallucination — strip it before writing to Snowflake.

def _validate_citations(
    brief: SafetyBriefOutput,
    retrieved_pmids: List[str],
) -> SafetyBriefOutput:
    def normalize(p: str) -> str:
        return p.strip().lstrip("PMID:").strip()
    retrieved  = {normalize(p) for p in retrieved_pmids}
    cleaned    = [p for p in brief.pmids_cited if normalize(p) in retrieved]
    fabricated = set(brief.pmids_cited) - set(cleaned)

    if fabricated:
        log.warning(
            "Stripped %d fabricated PMID(s) from %s x %s: %s",
            len(fabricated), brief.drug_key, brief.pt, fabricated,
        )
    try:
        from app.observability.metrics import AGENT3_CITATIONS_REMOVED
        removed = pre_count - len(brief.pmids_cited)
        if removed > 0:
            AGENT3_CITATIONS_REMOVED.inc(removed)
    except Exception:
        pass
    return brief.model_copy(update={"pmids_cited": cleaned})


# ── Prompt construction ───────────────────────────────────────────────────────

def _format_abstracts(abstracts: list) -> str:
    if not abstracts:
        return "No abstracts retrieved above similarity threshold."

    lines = []
    for i, a in enumerate(abstracts, 1):
        lines.append(
            f"[{i}] PMID:{a['pmid']} | similarity={a['similarity']:.3f}\n"
            f"{a['text'][:600]}"
        )
    return "\n\n".join(lines)


def _build_prompt(state: dict, priority: str) -> str:
    retrieved_pmids = [a.get("pmid", "") for a in (state.get("abstracts") or [])]

    # Build a prominent outcome context line so GPT-4o cannot miss deaths
    outcome_parts = []
    if state["death_count"] > 0:
        outcome_parts.append(f"{state['death_count']} deaths")
    if state["lt_count"] > 0:
        outcome_parts.append(f"{state['lt_count']} life-threatening events")
    if state["hosp_count"] > 0:
        outcome_parts.append(f"{state['hosp_count']} hospitalisations")
    outcome_line = (
        f"Serious outcomes: {', '.join(outcome_parts)}"
        if outcome_parts
        else "Serious outcomes: none reported"
    )

    return f"""Drug: {state["drug_key"]}
Reaction (MedDRA PT): {state["pt"]}
PRR: {state["prr"]:.2f} | Cases: {state["case_count"]}
{outcome_line}
StatScore: {state["stat_score"]:.4f} | LitScore: {state["lit_score"]:.4f}
Priority: {priority}

You MUST cite only these PMIDs: {retrieved_pmids}
Do not cite any PMID not in this list. If no PMID is relevant, leave pmids_cited empty.

Retrieved abstracts — cite using [PMID:xxxxxxxx] inline in brief_text:
{_format_abstracts(state.get("abstracts") or [])}

Select recommended_action using this clinical decision framework.
Apply it strictly — do not default to MONITOR unless none of the other criteria are met:

WITHDRAW     : Deaths reported AND PRR > 10 AND literature confirms direct
               causal mechanism with no adequate risk mitigation possible.
               Reserve for signals where continued use poses immediate risk
               that outweighs all clinical benefit.

RESTRICT     : Serious outcomes present (deaths OR life-threatening events)
               AND PRR > 5 AND literature supports limiting use to specific
               populations or with mandatory risk mitigation
               (e.g. contraindicated in renal failure, pregnancy, or
               with specific co-medications).

LABEL_UPDATE : PRR > 2 AND reaction is not adequately described in current
               labeling, OR new epidemiological evidence strengthens a known
               but under-documented risk. Applies even when outcomes are not
               life-threatening if signal frequency and PRR justify updated
               prescriber guidance. This is the most common appropriate action
               for a statistically significant P1 or P2 signal.

MONITOR      : Reaction is already adequately labeled, OR evidence is
               insufficient for regulatory action, OR reaction is mild and
               self-limiting with no serious outcomes and PRR below 3.

Return ONLY a JSON object. No markdown, no explanation, no extra text.
{{
    "brief_text": "2-3 paragraph clinical narrative justifying the recommended action. Cite PMIDs inline.",
    "key_findings": ["finding 1", "finding 2", "finding 3"],
    "pmids_cited": ["pmid1", "pmid2"],
    "recommended_action": "LABEL_UPDATE",
    "drug_key": "{state["drug_key"]}",
    "pt": "{state["pt"]}",
    "stat_score": {state["stat_score"]:.4f},
    "lit_score": {state["lit_score"]:.4f},
    "priority": "{priority}",
    "generated_at": "{datetime.now(timezone.utc).isoformat()}"
}}"""


def _build_retry_prompt(state: dict, priority: str, error: str) -> str:
    return f"""Your previous response failed schema validation with this error:
{error}

recommended_action must be exactly one of: MONITOR, LABEL_UPDATE, RESTRICT, WITHDRAW
pmids_cited must only contain PMIDs from the abstracts provided below.
key_findings must be a non-empty list of strings.
stat_score and lit_score must be floats between 0.0 and 1.0.

{_build_prompt(state, priority)}"""
try:
    from app.observability.metrics import AGENT3_PYDANTIC_RETRIES
    AGENT3_PYDANTIC_RETRIES.inc()
except Exception:
    pass


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
        temperature=0,
    )

    raw        = response.choices[0].message.content.strip()
    input_tok  = response.usage.prompt_tokens
    output_tok = response.usage.completion_tokens

    # Strip markdown fences — GPT-4o occasionally wraps JSON in ```json```
    # blocks despite explicit instructions not to.
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()
    try:
        from app.observability.metrics import LLM_TOKENS_USED
        LLM_TOKENS_USED.labels(agent="agent3", type="input").inc(input_tok)
        LLM_TOKENS_USED.labels(agent="agent3", type="output").inc(output_tok)
    except Exception:
        pass
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
    state     : dict,
    priority  : str,
    brief     : Optional[SafetyBriefOutput],
    input_tok : int,
    output_tok: int,
    gen_error : bool,
) -> None:
    conn = get_conn()
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
            source.drug_key,     source.pt,          source.stat_score,
            source.lit_score,    source.priority,    source.brief_text,
            source.key_findings, source.pmids_cited, source.recommended_action,
            source.model_used,   source.input_tokens, source.output_tokens,
            source.generation_error, source.generated_at
        )
        """,
        (
            state["drug_key"],
            state["pt"],
            state.get("stat_score"),
            state.get("lit_score"),
            priority,
            brief.brief_text               if brief else None,
            json.dumps(brief.key_findings) if brief else json.dumps([]),
            json.dumps(brief.pmids_cited)  if brief else json.dumps([]),
            brief.recommended_action       if brief else None,
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

    Reads stat_score from state (set by Agent 1). If Agent 1 has not run,
    logs a warning and computes stat_score locally as a fallback so the
    pipeline continues rather than producing a misleading 0.0 score.

    Returns new state fields only — LangGraph merges them automatically.
    """
    drug_key = state["drug_key"]
    pt       = state["pt"]

    # Agent 1 sets stat_score. Log a warning if it is missing so integration
    # failures are visible in logs rather than silently producing wrong output.
    stat_score = state.get("stat_score")
    if stat_score is None:
        log.warning(
            "stat_score missing for %s x %s — Agent 1 may not have run. "
            "Computing locally as fallback.",
            drug_key, pt,
        )
        stat_score = _compute_stat_score(
            state["prr"], state["case_count"],
            state["death_count"], state["lt_count"], state["hosp_count"],
        )

    lit_score = state.get("lit_score") or 0.0
    abstracts = state.get("abstracts") or []

    # Use local variables so state is not mutated inside the node.
    # LangGraph merges the return dict into state after the node completes.
    resolved_state = {
        **state,
        "stat_score": stat_score,
        "lit_score" : lit_score,
    }

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
                prompt = _build_prompt(resolved_state, priority)
            else:
                log.warning(
                    "Attempt 1 failed for %s x %s — retrying with error context",
                    drug_key, pt,
                )
                prompt = _build_retry_prompt(resolved_state, priority, last_error)

            raw, i_tok, o_tok = _call_gpt4o(prompt)
            input_tok  += i_tok
            output_tok += o_tok

            # Normalize recommended_action before Pydantic sees it.
            # GPT-4o often returns prose variants that do not exactly match
            # the four Literal values — this prevents generation_error on
            # every signal due to a trivial formatting difference.
            raw = _normalize_action(raw)

            # Overwrite fields GPT-4o must not control.
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
        resolved_state, priority, brief,
        input_tok, output_tok, gen_error,
    )
    invalidate_brief(state["drug_key"], state["pt"])
    return {
        "priority"  : priority,
        "brief"     : brief.model_dump() if brief else None,
        "stat_score": stat_score,
        "lit_score" : lit_score,
        "error"     : last_error if gen_error else None,
    }