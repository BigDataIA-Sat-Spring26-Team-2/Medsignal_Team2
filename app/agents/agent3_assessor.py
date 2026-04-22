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
from pydantic import ValidationError

from app.core.llm_router import LLMRouter

from app.agents.state import SignalState
from app.models.brief import SafetyBriefOutput

load_dotenv()

log   = logging.getLogger(__name__)
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
_VALID_ACTIONS = {"MONITOR", "LABEL_UPDATE", "RESTRICT", "WITHDRAW"}


# ── Priority tier ─────────────────────────────────────────────────────────────

def assign_priority(stat_score: float, lit_score: float) -> str:
    if stat_score >= 0.7 and lit_score >= 0.5:
        return "P1"
    if stat_score >= 0.7:
        return "P2"
    if lit_score >= 0.5:
        return "P3"
    return "P4"


# ── StatScore fallback ────────────────────────────────────────────────────────

def _compute_stat_score(
    prr: float, case_count: int,
    death: int, lt: int, hosp: int,
) -> float:
    prr_s = min(prr / 4.0, 1.0)
    vol_s = min(math.log10(max(case_count, 1)) / math.log10(50), 1.0)
    sev_s = 1.0 if death else 0.75 if lt else 0.50 if hosp else 0.0
    return round(prr_s * 0.50 + vol_s * 0.30 + sev_s * 0.20, 4)


# ── Action normalization ──────────────────────────────────────────────────────

def _normalize_action(raw: dict) -> dict:
    action = str(raw.get("recommended_action", "")).upper().strip()

    if not action:
        raw["recommended_action"] = "MONITOR"
        log.warning("recommended_action was empty — defaulting to MONITOR")
        return raw

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
        normalized = "MONITOR"
        log.warning(
            "recommended_action '%s' did not match any known value — "
            "defaulting to MONITOR",
            action,
        )

    raw["recommended_action"] = normalized
    return raw


# ── Citation guard ────────────────────────────────────────────────────────────

def _validate_citations(
    brief: SafetyBriefOutput,
    retrieved_pmids: List[str],
) -> SafetyBriefOutput:
    def normalize(p: str) -> str:
        return p.strip().lstrip("PMID:").strip()

    retrieved  = {normalize(p) for p in retrieved_pmids}
    pre_count  = len(brief.pmids_cited)
    cleaned    = [p for p in brief.pmids_cited if normalize(p) in retrieved]
    fabricated = set(brief.pmids_cited) - set(cleaned)

    if fabricated:
        log.warning(
            "Stripped %d fabricated PMID(s) from %s x %s: %s",
            len(fabricated), brief.drug_key, brief.pt, fabricated,
        )

    updated = brief.model_copy(update={"pmids_cited": cleaned})

    # Observability — inside try/except so metrics failure never breaks pipeline
    try:
        from app.observability.metrics import AGENT3_CITATIONS_REMOVED
        removed = pre_count - len(updated.pmids_cited)
        if removed > 0:
            AGENT3_CITATIONS_REMOVED.inc(removed)
    except Exception:
        pass

    return updated


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

    death = int(state.get("death_count") or 0)
    lt    = int(state.get("lt_count")    or 0)
    hosp  = int(state.get("hosp_count")  or 0)
    prr   = float(state.get("prr") or 0)

    outcome_parts = []
    if death > 0:
        outcome_parts.append(f"{death} deaths")
    if lt > 0:
        outcome_parts.append(f"{lt} life-threatening events")
    if hosp > 0:
        outcome_parts.append(f"{hosp} hospitalisations")
    outcome_line = (
        f"Serious outcomes: {', '.join(outcome_parts)}"
        if outcome_parts
        else "Serious outcomes: none reported"
    )

    hints = []
    if death > 0 and prr > 10:
        hints.append(
            f"⚠ WITHDRAW criteria may be met: {death} deaths reported with PRR {prr:.2f} > 10"
        )
    elif death > 0 and prr > 2:
        hints.append(
            f"⚠ RESTRICT criteria may be met: {death} deaths reported with PRR {prr:.2f} > 2 — "
            f"consider whether use should be limited to specific populations"
        )
    elif lt > 0 and prr > 2:
        hints.append(
            f"⚠ RESTRICT criteria may be met: {lt} life-threatening events with PRR {prr:.2f}"
        )
    elif death == 0 and lt == 0 and hosp == 0 and prr < 5:
        hints.append(
            "ℹ No serious outcomes reported and PRR below 5 — "
            "consider whether MONITOR is appropriate if reaction is mild and self-limiting"
        )
    hint_block = "\n".join(hints) if hints else ""

    return f"""Drug: {state["drug_key"]}
Reaction (MedDRA PT): {state["pt"]}
PRR: {prr:.2f} | Cases: {state["case_count"]}
{outcome_line}
StatScore: {state["stat_score"]:.4f} | LitScore: {state["lit_score"]:.4f}
Priority: {priority}
{hint_block}

You MUST cite only these PMIDs: {retrieved_pmids}
Do not cite any PMID not in this list. If no PMID is relevant, leave pmids_cited empty.

Retrieved abstracts — cite using [PMID:xxxxxxxx] inline in brief_text:
{_format_abstracts(state.get("abstracts") or [])}

Select recommended_action using this clinical decision framework.
Read the serious outcomes line above carefully before deciding.

WITHDRAW : Deaths reported AND PRR > 10 AND literature confirms direct
           causal mechanism with no adequate risk mitigation possible.
           Reserve for signals where continued use poses immediate serious
           risk that outweighs all clinical benefit.

RESTRICT : Any of the following:
           — Deaths reported AND PRR > 2 AND literature supports limiting
             use to specific populations (e.g. renal failure, pregnancy,
             elderly) or adding mandatory contraindications.
           — Life-threatening events AND PRR > 5 AND literature supports
             restriction to specific conditions or co-medication warnings.

LABEL_UPDATE : PRR > 2 AND the reaction is statistically significant AND
               one of:
               — Reaction is not adequately described in current labeling
               — New evidence strengthens a known but under-documented risk
               — Reaction has serious outcomes (hospitalisation) but does
                 not meet RESTRICT threshold
               This is the most common action for P1/P2 signals without deaths.

MONITOR : Use MONITOR when ALL of the following are true:
          — Zero deaths, zero life-threatening events, zero hospitalisations
          — Reaction is mild and self-limiting (e.g. injection site pain,
            nausea, flatulence, minor skin reactions, eructation)
          — PRR < 5 OR the reaction is already well-documented in labeling
          If deaths are present, MONITOR is never appropriate.

Worked examples to calibrate your decision:
Drug A x reaction  | deaths=90, PRR=17.8  → RESTRICT
Drug B x reaction  | deaths=33, PRR=2.98  → RESTRICT
Drug C x reaction  | deaths present, PRR=4.2 → RESTRICT
Drug D x reaction  | hosp=41, PRR=30.8 → LABEL_UPDATE
Drug E x reaction  | 0 deaths, PRR=9.0 → LABEL_UPDATE
Drug F x reaction  | 0 deaths, PRR=4.0, known risk → LABEL_UPDATE
Drug G x reaction  | 0 deaths, 0 LT, mild local → MONITOR
Drug H x reaction  | 0 deaths, 0 LT, GI symptom → MONITOR

Return ONLY a JSON object. No markdown, no explanation, no extra text.
{{
    "brief_text": "2-3 paragraph clinical narrative justifying the recommended action. Cite PMIDs inline.",
    "key_findings": ["finding 1", "finding 2", "finding 3"],
    "pmids_cited": ["pmid1", "pmid2"],
    "search_queries": {json.dumps(state.get("search_queries", []))},
    "recommended_action": "LABEL_UPDATE",
    "drug_key": "{state["drug_key"]}",
    "pt": "{state["pt"]}",
    "stat_score": {state["stat_score"]:.4f},
    "lit_score": {state["lit_score"]:.4f},
    "priority": "{priority}",
    "generated_at": "{datetime.now(timezone.utc).isoformat()}"
}}"""


def _build_retry_prompt(state: dict, priority: str, error: str) -> str:
    # Observability — must be inside the function, not at module level
    try:
        from app.observability.metrics import AGENT3_PYDANTIC_RETRIES
        AGENT3_PYDANTIC_RETRIES.inc()
    except Exception:
        pass

    return f"""Your previous response failed schema validation with this error:
{error}

recommended_action must be exactly one of: MONITOR, LABEL_UPDATE, RESTRICT, WITHDRAW
pmids_cited must only contain PMIDs from the abstracts provided below.
key_findings must be a non-empty list of strings.
stat_score and lit_score must be floats between 0.0 and 1.0.

{_build_prompt(state, priority)}"""


# ── LLM call via router ───────────────────────────────────────────────────────

def _call_llm(prompt: str, router: LLMRouter) -> tuple[dict, int, int]:
    response = router.complete(
        task="brief_generation",
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
    )

    raw        = response.choices[0].message.content.strip()
    input_tok  = response.usage.prompt_tokens
    output_tok = response.usage.completion_tokens

    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    # Observability — inside try/except so metrics failure never breaks pipeline
    try:
        from app.observability.metrics import LLM_TOKENS_USED
        LLM_TOKENS_USED.labels(agent="agent3", type="input").inc(input_tok)
        LLM_TOKENS_USED.labels(agent="agent3", type="output").inc(output_tok)
    except Exception:
        pass

    try:
        return json.loads(raw), input_tok, output_tok
    except json.JSONDecodeError as e:
        raise ValueError(f"LLM did not return valid JSON: {e}\nOutput: {raw[:300]}")


# ── Snowflake writer ──────────────────────────────────────────────────────────

def _write_to_snowflake(
    state     : dict,
    priority  : str,
    brief     : Optional[SafetyBriefOutput],
    input_tok : int,
    output_tok: int,
    gen_error : bool,
    hallucination_report: Optional[dict] = None,
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
                PARSE_JSON(%s)    AS search_queries,
                %s                AS recommended_action,
                %s                AS model_used,
                %s                AS input_tokens,
                %s                AS output_tokens,
                %s                AS generation_error,
                %s                AS hallucination_score,
                %s                AS hallucination_pass,
                PARSE_JSON(%s)    AS hallucination_flags,
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
            search_queries     = source.search_queries,
            recommended_action = source.recommended_action,
            model_used         = source.model_used,
            input_tokens       = source.input_tokens,
            output_tokens      = source.output_tokens,
            generation_error   = source.generation_error,
            hallucination_score = source.hallucination_score,
            hallucination_pass  = source.hallucination_pass,
            hallucination_flags = source.hallucination_flags,
            generated_at       = source.generated_at
        WHEN NOT MATCHED THEN INSERT (
            drug_key, pt, stat_score, lit_score, priority,
            brief_text, key_findings, pmids_cited, search_queries,
            recommended_action, model_used, input_tokens, output_tokens,
            generation_error, hallucination_score, hallucination_pass,
            hallucination_flags, generated_at
        ) VALUES (
            source.drug_key,     source.pt,          source.stat_score,
            source.lit_score,    source.priority,    source.brief_text,
            source.key_findings, source.pmids_cited, source.search_queries,
            source.recommended_action, source.model_used, source.input_tokens,
            source.output_tokens, source.generation_error, source.hallucination_score,
            source.hallucination_pass, source.hallucination_flags,
            source.generated_at
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
            json.dumps(state.get("search_queries", [])),
            brief.recommended_action       if brief else None,
            MODEL,
            input_tok,
            output_tok,
            gen_error,
            hallucination_report["hallucination_score"] if hallucination_report else None,
            hallucination_report["pass"]                if hallucination_report else None,
            json.dumps(hallucination_report["flags"])   if hallucination_report else json.dumps([]),
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
    from evaluation.hallucination_check import validate_brief

    drug_key = state["drug_key"]
    pt       = state["pt"]
    router   = state.get("router") or LLMRouter()

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
    hallucination_report = None

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

            raw, i_tok, o_tok = _call_llm(prompt, router)
            input_tok  += i_tok
            output_tok += o_tok

            raw = _normalize_action(raw)

            raw["drug_key"]     = drug_key
            raw["pt"]           = pt
            raw["stat_score"]   = stat_score
            raw["lit_score"]    = lit_score
            raw["priority"]     = priority
            raw["generated_at"] = datetime.now(timezone.utc).isoformat()

            brief = SafetyBriefOutput(**raw)
            brief = _validate_citations(brief, retrieved_pmids)

            # Run hallucination detection
            hallucination_report = validate_brief(
                brief=brief,
                state=resolved_state,
                abstracts=abstracts
            )

            log.info(
                "agent3_success — %s x %s | attempt=%d | pmids=%d | action=%s | "
                "hallucination_score=%.3f | pass=%s",
                drug_key, pt, attempt + 1,
                len(brief.pmids_cited), brief.recommended_action,
                hallucination_report["hallucination_score"],
                hallucination_report["pass"]
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
        hallucination_report,
    )
    invalidate_brief(state["drug_key"], state["pt"])
    return {
        "priority"  : priority,
        "brief"     : brief.model_dump() if brief else None,
        "stat_score": stat_score,
        "lit_score" : lit_score,
        "error"     : last_error if gen_error else None,
        "hallucination_report": hallucination_report,
    }