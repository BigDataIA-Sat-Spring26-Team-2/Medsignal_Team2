"""
evaluation/hallucination_check.py — Hallucination detection for SafetyBriefs

Validates GPT-4o generated SafetyBriefs for:
  1. Numerical accuracy — PRR, case counts, severity data
  2. Priority-action consistency — recommended_action matches priority tier
  3. Citation grounding — claims are supported by retrieved abstracts

Returns hallucination score [0.0, 1.0] where:
  0.0 = no hallucinations detected
  1.0 = severe hallucinations across multiple dimensions

Usage:
    from evaluation.hallucination_check import validate_brief

    report = validate_brief(
        brief=brief,
        state=state,
        abstracts=abstracts
    )

    # report = {
    #     "hallucination_score": 0.15,
    #     "pass": True,
    #     "checks": {...},
    #     "flags": [...]
    # }
"""

import logging
import re
from typing import Any, Dict, List, Optional

from sentence_transformers import SentenceTransformer

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

# Hallucination score threshold — briefs above this should be flagged for review
HALLUCINATION_THRESHOLD = 0.20

# Weights for composite hallucination score
WEIGHTS = {
    "numerical_accuracy": 0.40,      # High weight — objective, verifiable
    "priority_consistency": 0.30,    # Medium-high — enforces decision framework
    "citation_grounding": 0.30,      # Medium-high — prevents unsupported claims
}

# Semantic similarity threshold for citation grounding
CITATION_SIMILARITY_THRESHOLD = 0.50

# Lazy-load embedding model (same as Agent 2 uses)
_EMBEDDING_MODEL: Optional[SentenceTransformer] = None


def _get_embedding_model() -> SentenceTransformer:
    """Lazy loader for sentence transformer model."""
    global _EMBEDDING_MODEL
    if _EMBEDDING_MODEL is None:
        _EMBEDDING_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
        log.info("Loaded all-MiniLM-L6-v2 for hallucination checks")
    return _EMBEDDING_MODEL


# ── 1. Numerical Accuracy Check ──────────────────────────────────────────────

def validate_numerical_accuracy(brief: Any, state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Check if numerical values in brief_text match source data.

    Detects:
      - Fabricated PRR values
      - Wrong case counts
      - Incorrect death/hospitalization/life-threatening counts
      - Invalid percentages (> 100%)
      - Out-of-range dates

    Args:
        brief: SafetyBriefOutput Pydantic model
        state: SignalState dict with actual values

    Returns:
        {
            "errors": List[str],
            "hallucination_rate": float (0.0-1.0),
            "details": {...}
        }
    """
    errors = []
    brief_text = brief.brief_text.lower()

    # Extract all numbers with surrounding context
    number_pattern = r'(\d+(?:\.\d+)?)\s*([a-z\s]{0,30})'
    matches = re.findall(number_pattern, brief_text)

    # Check PRR mentions
    # NOTE: Only match "prr" or "proportional reporting ratio" specifically
    # to avoid false positives on "proportion of patients", "proportion of reports", etc.
    prr_actual = state.get("prr", 0)
    prr_mentions = [
        float(num) for num, ctx in matches
        if "prr" in ctx or "proportional reporting" in ctx
    ]

    for prr_claimed in prr_mentions:
        # Allow 10% tolerance for rounding
        if abs(prr_claimed - prr_actual) > prr_actual * 0.1:
            errors.append(
                f"Hallucinated PRR: claimed {prr_claimed:.2f}, actual {prr_actual:.2f}"
            )

    # Check case count mentions
    case_actual = state.get("case_count", 0)
    case_mentions = [
        int(float(num)) for num, ctx in matches
        if any(keyword in ctx for keyword in ["case", "report", "event"])
        and float(num) > 10  # Skip small numbers (likely percentages or other stats)
    ]

    for case_claimed in case_mentions:
        # Allow 20% tolerance (GPT may say "over 100 cases" when actual is 120)
        if abs(case_claimed - case_actual) > case_actual * 0.2:
            errors.append(
                f"Hallucinated case count: claimed {case_claimed}, actual {case_actual}"
            )

    # Check death count mentions
    death_actual = state.get("death_count", 0)
    death_keywords = ["death", "fatal", "fatality", "mortality"]

    # Flag if brief claims "no deaths" but deaths exist
    if death_actual > 0:
        if any(phrase in brief_text for phrase in ["no death", "zero death", "no fatal"]):
            errors.append(f"Claims no deaths but {death_actual} deaths reported")

    # Flag if brief claims deaths when none exist
    if death_actual == 0:
        death_claim_patterns = [
            r'\d+\s+death', r'fatal\s+outcome', r'resulted\s+in\s+death',
            r'mortality\s+of\s+\d+'
        ]
        if any(re.search(pattern, brief_text) for pattern in death_claim_patterns):
            errors.append("Claims deaths occurred but data shows 0 deaths")

    # Check for percentage hallucinations (> 100%)
    percentage_pattern = r'(\d+(?:\.\d+)?)\s*%'
    percentages = [float(p) for p in re.findall(percentage_pattern, brief_text)]
    for pct in percentages:
        if pct > 100:
            errors.append(f"Invalid percentage: {pct}% (exceeds 100%)")

    # Check for date hallucinations (should be 2023 for FAERS data)
    year_pattern = r'\b(19\d{2}|20\d{2})\b'
    years = [int(y) for y in re.findall(year_pattern, brief_text)]
    for year in years:
        if year < 2023 or year > 2024:
            errors.append(
                f"Suspicious date: {year} (FAERS data is 2023, FDA comm may be 2024)"
            )

    # Compute hallucination rate
    # 0 errors = 0.0, 1 error = 0.33, 2 errors = 0.67, 3+ = 1.0
    hallucination_rate = min(len(errors) / 3.0, 1.0)

    return {
        "errors": errors,
        "hallucination_rate": hallucination_rate,
        "details": {
            "prr_actual": prr_actual,
            "prr_mentions": prr_mentions,
            "case_actual": case_actual,
            "case_mentions": case_mentions,
            "death_actual": death_actual,
        }
    }


# ── 2. Priority-Action Consistency Check ──────────────────────────────────────

def validate_priority_action_consistency(brief: Any, state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Verify recommended_action is consistent with priority tier and data.

    Enforces decision framework from agent3_assessor.py lines 196-220:
      WITHDRAW     : deaths + PRR > 10
      RESTRICT     : serious outcomes + PRR > 5
      LABEL_UPDATE : PRR > 2
      MONITOR      : otherwise

    Args:
        brief: SafetyBriefOutput with priority and recommended_action
        state: SignalState with PRR and outcome counts

    Returns:
        {
            "errors": List[str],
            "hallucination_rate": float (0.0-1.0),
            "details": {...}
        }
    """
    errors = []

    priority = brief.priority
    action = brief.recommended_action
    prr = state.get("prr", 0)
    deaths = state.get("death_count", 0)
    lt = state.get("lt_count", 0)
    hosp = state.get("hosp_count", 0)

    # Define expected actions per priority tier
    expected_actions = {
        "P1": ["LABEL_UPDATE", "RESTRICT", "WITHDRAW"],  # High priority — strong action
        "P2": ["LABEL_UPDATE", "RESTRICT", "MONITOR"],   # Good stat, weak lit
        "P3": ["LABEL_UPDATE", "MONITOR"],               # Weak stat, good lit
        "P4": ["MONITOR", "LABEL_UPDATE"]                # Weak both — monitor unless PRR justifies label
    }

    # Check 1: Action matches priority tier
    if action not in expected_actions.get(priority, []):
        errors.append(
            f"Priority-action mismatch: {priority} should use {expected_actions[priority]}, "
            f"not {action}"
        )

    # Check 2: WITHDRAW justification
    # NOTE: This only validates deaths + PRR. The full WITHDRAW criteria requires
    # "literature confirms direct causal mechanism with no adequate risk mitigation possible"
    # which cannot be easily automated. Reviewers should manually assess literature causality.
    if action == "WITHDRAW":
        if deaths == 0:
            errors.append("WITHDRAW without deaths — should be RESTRICT or LABEL_UPDATE")
        if prr < 10:
            errors.append(
                f"WITHDRAW with low PRR ({prr:.2f}) — threshold is PRR > 10"
            )

    # Check 3: RESTRICT justification
    # Updated to match agent3_assessor.py decision framework:
    #   - Deaths + PRR > 2  OR
    #   - Life-threatening events + PRR > 5
    if action == "RESTRICT":
        if deaths == 0 and lt == 0:
            errors.append(
                "RESTRICT without deaths or life-threatening events — "
                "should be LABEL_UPDATE or MONITOR"
            )
        elif deaths > 0 and prr < 2:
            errors.append(
                f"RESTRICT with deaths but PRR < 2 ({prr:.2f}) — "
                "threshold is PRR > 2 when deaths present"
            )
        elif deaths == 0 and lt > 0 and prr < 5:
            errors.append(
                f"RESTRICT with LT events but PRR < 5 ({prr:.2f}) — "
                "threshold is PRR > 5 when only LT events (no deaths)"
            )

    # Check 4: LABEL_UPDATE justification
    if action == "LABEL_UPDATE":
        if prr < 2:
            errors.append(
                f"LABEL_UPDATE with PRR < 2 ({prr:.2f}) — signal may not meet threshold"
            )

    # Check 5: MONITOR over-use
    # Only flag MONITOR as suspicious if serious outcomes exist.
    # Mild reactions (injection site pain, nausea) should get MONITOR even with high PRR.
    # Example: tirzepatide x injection site pain (P1, PRR ~4, 0 deaths/LT/hosp) → MONITOR correct
    if action == "MONITOR" and priority in ["P1", "P2"]:
        # Only flag if there are serious outcomes that should trigger stronger action
        if (deaths > 0 or lt > 0 or hosp > 0) and prr > 3:
            errors.append(
                f"MONITOR for {priority} signal with serious outcomes "
                f"(deaths={deaths}, lt={lt}, hosp={hosp}) and PRR {prr:.2f} — "
                "consider LABEL_UPDATE or RESTRICT"
            )

    # Hallucination rate: 0 errors = 0.0, 1 error = 0.5, 2+ = 1.0
    hallucination_rate = min(len(errors) / 2.0, 1.0)

    return {
        "errors": errors,
        "hallucination_rate": hallucination_rate,
        "details": {
            "priority": priority,
            "action": action,
            "prr": prr,
            "deaths": deaths,
            "lt": lt,
            "hosp": hosp,
            "expected_actions": expected_actions.get(priority, [])
        }
    }


# ── 3. Citation Grounding Check ───────────────────────────────────────────────

def validate_citation_grounding(brief: Any, abstracts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Verify claims in brief_text are semantically grounded in retrieved abstracts.

    Extracts sentences with citations from brief_text and checks if the claim
    has high semantic similarity with the cited abstract.

    Args:
        brief: SafetyBriefOutput with brief_text and pmids_cited
        abstracts: List of retrieved abstracts from Agent 2

    Returns:
        {
            "errors": List[str],
            "hallucination_rate": float (0.0-1.0),
            "details": {...}
        }
    """
    errors = []

    if not abstracts:
        # No abstracts retrieved — cannot validate grounding
        # Not a hallucination per se, but flag for low confidence
        return {
            "errors": ["No abstracts available for grounding check"],
            "hallucination_rate": 0.0,  # Don't penalize — this is a data issue
            "details": {"abstracts_count": 0, "grounded_claims": 0}
        }

    # Build PMID → abstract text mapping
    pmid_to_abstract = {
        str(a.get("pmid", "")).strip(): a.get("text", "")
        for a in abstracts
    }

    # Extract claims with citations from brief_text
    # Pattern: [PMID:12345678] or similar citation formats
    citation_pattern = r'\[PMID[:\s]*(\d+)\]([^.]*\.)'
    matches = re.findall(citation_pattern, brief.brief_text, re.IGNORECASE)

    if not matches:
        # No inline citations found — check if brief mentions PMIDs at all
        if len(brief.pmids_cited) > 0:
            # Brief has PMIDs but doesn't cite them inline — acceptable
            log.debug("Brief has PMIDs but no inline citations — acceptable pattern")
        return {
            "errors": [],
            "hallucination_rate": 0.0,
            "details": {"inline_citations": 0, "grounded_claims": 0}
        }

    model = _get_embedding_model()
    grounded_count = 0
    ungrounded_claims = []

    for pmid, claim in matches:
        pmid = pmid.strip()
        claim = claim.strip()

        # Skip very short claims (likely just labels)
        if len(claim.split()) < 5:
            continue

        # Check if PMID exists in retrieved abstracts
        if pmid not in pmid_to_abstract:
            errors.append(f"Cited PMID {pmid} not in retrieved set")
            continue

        abstract_text = pmid_to_abstract[pmid]

        # Compute semantic similarity
        claim_embedding = model.encode(claim, convert_to_tensor=False)
        abstract_embedding = model.encode(abstract_text[:500], convert_to_tensor=False)

        # Cosine similarity
        from numpy import dot
        from numpy.linalg import norm

        similarity = dot(claim_embedding, abstract_embedding) / (
            norm(claim_embedding) * norm(abstract_embedding)
        )

        if similarity < CITATION_SIMILARITY_THRESHOLD:
            ungrounded_claims.append({
                "pmid": pmid,
                "claim": claim[:100],
                "similarity": float(similarity)
            })
        else:
            grounded_count += 1

    # Report ungrounded claims
    for item in ungrounded_claims:
        errors.append(
            f"Claim not grounded in PMID {item['pmid']} "
            f"(similarity {item['similarity']:.2f} < {CITATION_SIMILARITY_THRESHOLD}): "
            f"{item['claim']}"
        )

    # Hallucination rate based on ungrounded proportion
    total_claims = len(matches)
    if total_claims > 0:
        hallucination_rate = len(ungrounded_claims) / total_claims
    else:
        hallucination_rate = 0.0

    return {
        "errors": errors,
        "hallucination_rate": hallucination_rate,
        "details": {
            "total_claims": total_claims,
            "grounded_count": grounded_count,
            "ungrounded_count": len(ungrounded_claims),
            "ungrounded_claims": ungrounded_claims
        }
    }


# ── Main Validation Function ──────────────────────────────────────────────────

def validate_brief(
    brief: Any,
    state: Dict[str, Any],
    abstracts: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Run all hallucination checks and return composite report.

    Args:
        brief: SafetyBriefOutput Pydantic model
        state: SignalState dict with actual values
        abstracts: Retrieved abstracts from Agent 2

    Returns:
        {
            "hallucination_score": float (0.0-1.0),
            "pass": bool (True if score < threshold),
            "checks": {
                "numerical_accuracy": {...},
                "priority_consistency": {...},
                "citation_grounding": {...}
            },
            "flags": List[str] (all errors across checks)
        }
    """
    log.info(
        "hallucination_check_start — %s x %s",
        brief.drug_key, brief.pt
    )

    # Run all checks
    checks = {
        "numerical_accuracy": validate_numerical_accuracy(brief, state),
        "priority_consistency": validate_priority_action_consistency(brief, state),
        "citation_grounding": validate_citation_grounding(brief, abstracts),
    }

    # Compute weighted hallucination score
    hallucination_score = sum(
        checks[name]["hallucination_rate"] * WEIGHTS[name]
        for name in WEIGHTS
    )

    # Collect all error flags
    all_flags = []
    for check_name, result in checks.items():
        for error in result.get("errors", []):
            all_flags.append(f"[{check_name}] {error}")

    # Determine pass/fail
    passed = hallucination_score < HALLUCINATION_THRESHOLD

    log.info(
        "hallucination_check_complete — %s x %s | score=%.3f | pass=%s | flags=%d",
        brief.drug_key, brief.pt, hallucination_score, passed, len(all_flags)
    )

    if not passed:
        log.warning(
            "HALLUCINATION DETECTED — %s x %s | score=%.3f | flags: %s",
            brief.drug_key, brief.pt, hallucination_score, all_flags[:3]
        )

    return {
        "hallucination_score": round(hallucination_score, 3),
        "pass": passed,
        "checks": checks,
        "flags": all_flags,
        "drug_key": brief.drug_key,
        "pt": brief.pt,
    }
