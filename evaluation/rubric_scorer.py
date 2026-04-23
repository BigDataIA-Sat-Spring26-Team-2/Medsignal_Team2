"""
evaluation/rubric_scorer.py — SafetyBrief Quality Rubric

Applies the four-criteria rubric defined in the proposal (section 5.7)
to all 10 golden drug-reaction SafetyBriefs.

Criteria (pass/fail per brief):
    1. Signal identification  — brief_text contains drug_key AND pt
    2. Literature grounding   — at least 1 PMID cited in brief_text
    3. Citation accuracy      — every PMID in brief_text appears in pmids_cited
    4. Tier consistency       — recommended_action is consistent with priority tier

A brief passes if ALL four criteria pass.
Pass rate = passing briefs / 10 golden signals.
Target: >= 7/10 (per proposal KPIs).

Run:
    poetry run python evaluation/rubric_scorer.py

Output:
    Prints per-signal results and overall pass rate to stdout.
    Saves results to evaluation/rubric_results.json for reporting.
"""

import json
import os
import re
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
from app.utils.snowflake_client import get_conn

load_dotenv()

# ── Golden signals — must match evaluation/lead_time.py exactly ──────────────

GOLDEN_SIGNALS = [
    {"drug_key": "dupilumab",    "pt": "conjunctivitis"},
    {"drug_key": "pregabalin",     "pt": "coma"},
    {"drug_key": "gabapentin",     "pt": "cardio-respiratory arrest"},
    {"drug_key": "levetiracetam",  "pt": "seizure"},
    {"drug_key": "tirzepatide",  "pt": "injection site pain"},
    {"drug_key": "metformin",      "pt": "lactic acidosis"},
    {"drug_key": "bupropion",      "pt": "seizure"},
    {"drug_key": "semaglutide",    "pt": "increased appetite"},
    {"drug_key": "empagliflozin",    "pt": "diabetic ketoacidosis"},
    {"drug_key": "dapagliflozin",   "pt": "glomerular filtration rate decreased"},
]

# Tier → valid recommended actions (from agent3_assessor.py decision framework)
VALID_ACTIONS_BY_TIER = {
    "P1": {"WITHDRAW", "RESTRICT", "LABEL_UPDATE", "MONITOR"},
    "P2": {"RESTRICT", "LABEL_UPDATE", "MONITOR"},
    "P3": {"LABEL_UPDATE", "MONITOR"},
    "P4": {"MONITOR"},
}

# Stricter consistency check — P1 with deaths should not use MONITOR
# This is a soft flag, not a hard fail, to avoid over-penalising


# ── Criteria functions ────────────────────────────────────────────────────────

def check_signal_identification(brief: dict) -> tuple[bool, str]:
    """
    Criterion 1: brief_text contains both drug_key and pt.
    Case-insensitive substring match.
    """
    text     = (brief.get("brief_text") or "").lower()
    drug_key = (brief.get("drug_key")   or "").lower()
    pt       = (brief.get("pt")         or "").lower()

    drug_present = drug_key in text
    pt_present   = pt in text

    if drug_present and pt_present:
        return True, "pass"
    missing = []
    if not drug_present: missing.append(f"drug '{drug_key}'")
    if not pt_present:   missing.append(f"reaction '{pt}'")
    return False, f"missing: {', '.join(missing)}"


def check_literature_grounding(brief: dict) -> tuple[bool, str]:
    """
    Criterion 2: brief_text contains at least one PMID citation.
    Looks for PMID: or [PMID:NNNNN] or bare 8-digit numbers in pmids_cited.
    """
    text       = brief.get("brief_text") or ""
    pmids_cited = brief.get("pmids_cited") or []

    # Check if any cited PMID appears in the text
    for pmid in pmids_cited:
        if str(pmid) in text:
            return True, f"pass ({len(pmids_cited)} PMIDs cited)"

    # Also check for PMID: pattern even if pmids_cited is empty
    if re.search(r'PMID[:\s]\d+', text, re.IGNORECASE):
        return True, "pass (PMID pattern found in text)"

    if not pmids_cited:
        return False, "no PMIDs cited"
    return False, "PMIDs cited but none appear in brief_text"


def check_citation_accuracy(brief: dict) -> tuple[bool, str]:
    """
    Criterion 3: every PMID mentioned in brief_text appears in pmids_cited.
    Extracts all numeric IDs from text and checks against pmids_cited list.
    """
    text        = brief.get("brief_text") or ""
    pmids_cited = set(str(p) for p in (brief.get("pmids_cited") or []))

    # Extract PMIDs from text — look for 7-9 digit numbers near PMID keyword
    pmid_pattern = re.compile(r'PMID[:\s]?(\d{7,9})', re.IGNORECASE)
    mentioned    = set(pmid_pattern.findall(text))

    if not mentioned:
        return True, "pass (no PMIDs mentioned in text)"

    fabricated = mentioned - pmids_cited
    if not fabricated:
        return True, f"pass (all {len(mentioned)} cited PMIDs verified)"
    return False, f"fabricated PMIDs: {fabricated}"


def check_tier_consistency(brief: dict) -> tuple[bool, str]:
    """
    Criterion 4: recommended_action is consistent with priority tier.
    P4 → MONITOR only
    P3 → LABEL_UPDATE or MONITOR
    P2 → RESTRICT, LABEL_UPDATE, or MONITOR
    P1 → any action
    """
    priority = (brief.get("priority") or "").upper()
    action   = (brief.get("recommended_action") or "").upper()

    if not priority or not action:
        return False, f"missing priority='{priority}' or action='{action}'"

    valid = VALID_ACTIONS_BY_TIER.get(priority, set())
    if action in valid:
        return True, f"pass ({priority} → {action})"
    return False, f"{priority} cannot use {action} (valid: {valid})"


# ── Fetch briefs from Snowflake ───────────────────────────────────────────────

def fetch_golden_briefs() -> list[dict]:
    """
    Fetch SafetyBriefs for all 10 golden signals from Snowflake.
    Returns list of brief dicts — None entry if brief not found for a signal.
    """
    conn = get_conn()
    cur  = conn.cursor()

    results = []
    for signal in GOLDEN_SIGNALS:
        cur.execute("""
            SELECT
                sb.drug_key,
                sb.pt,
                sb.priority,
                sb.brief_text,
                sb.pmids_cited,
                sb.recommended_action,
                sb.generation_error,
                sb.stat_score,
                sb.lit_score
            FROM safety_briefs sb
            WHERE sb.drug_key = %s AND sb.pt = %s
            LIMIT 1
        """, (signal["drug_key"], signal["pt"]))

        row = cur.fetchone()
        if row is None:
            results.append({
                "drug_key": signal["drug_key"],
                "pt":       signal["pt"],
                "_found":   False,
            })
        else:
            cols  = [desc[0].lower() for desc in cur.description]
            brief = dict(zip(cols, row))

            # pmids_cited comes back as JSON string from Snowflake VARIANT
            if isinstance(brief.get("pmids_cited"), str):
                try:
                    brief["pmids_cited"] = json.loads(brief["pmids_cited"])
                except Exception:
                    brief["pmids_cited"] = []

            brief["_found"] = True
            results.append(brief)

    cur.close()
    conn.close()
    return results


# ── Score one brief ───────────────────────────────────────────────────────────

def score_brief(brief: dict) -> dict:
    """
    Apply all 4 criteria to one brief.
    Returns per-criterion pass/fail + overall pass.
    """
    if not brief.get("_found"):
        return {
            "drug_key":            brief["drug_key"],
            "pt":                  brief["pt"],
            "found":               False,
            "overall_pass":        False,
            "signal_identification": (False, "brief not found in Snowflake"),
            "literature_grounding":  (False, "brief not found in Snowflake"),
            "citation_accuracy":     (False, "brief not found in Snowflake"),
            "tier_consistency":      (False, "brief not found in Snowflake"),
        }

    if brief.get("generation_error"):
        return {
            "drug_key":            brief["drug_key"],
            "pt":                  brief["pt"],
            "found":               True,
            "overall_pass":        False,
            "signal_identification": (False, "generation_error=True"),
            "literature_grounding":  (False, "generation_error=True"),
            "citation_accuracy":     (False, "generation_error=True"),
            "tier_consistency":      (False, "generation_error=True"),
        }

    c1 = check_signal_identification(brief)
    c2 = check_literature_grounding(brief)
    c3 = check_citation_accuracy(brief)
    c4 = check_tier_consistency(brief)

    overall = all([c1[0], c2[0], c3[0], c4[0]])

    return {
        "drug_key":              brief["drug_key"],
        "pt":                    brief["pt"],
        "priority":              brief.get("priority"),
        "recommended_action":    brief.get("recommended_action"),
        "found":                 True,
        "overall_pass":          overall,
        "signal_identification": c1,
        "literature_grounding":  c2,
        "citation_accuracy":     c3,
        "tier_consistency":      c4,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "="*70)
    print("MedSignal — SafetyBrief Quality Rubric")
    print("="*70)

    briefs  = fetch_golden_briefs()
    results = [score_brief(b) for b in briefs]

    print(f"\n{'Drug':<20} {'PT':<35} {'C1':<4} {'C2':<4} {'C3':<4} {'C4':<4} {'PASS'}")
    print("-"*80)

    passes = 0
    for r in results:
        c1 = "✓" if r["signal_identification"][0] else "✗"
        c2 = "✓" if r["literature_grounding"][0]  else "✗"
        c3 = "✓" if r["citation_accuracy"][0]      else "✗"
        c4 = "✓" if r["tier_consistency"][0]       else "✗"
        ok = "PASS" if r["overall_pass"] else "FAIL"

        if r["overall_pass"]:
            passes += 1

        print(f"{r['drug_key']:<20} {r['pt']:<35} {c1:<4} {c2:<4} {c3:<4} {c4:<4} {ok}")

        # Print failure reasons
        for label, result in [
            ("  Signal ID",   r["signal_identification"]),
            ("  Lit ground",  r["literature_grounding"]),
            ("  Citation",    r["citation_accuracy"]),
            ("  Tier",        r["tier_consistency"]),
        ]:
            if not result[0]:
                print(f"    {label}: {result[1]}")

    total      = len(results)
    pass_rate  = passes / total
    target_met = pass_rate >= 0.7

    print("\n" + "="*70)
    print(f"Results: {passes}/{total} briefs passed all 4 criteria")
    print(f"Pass rate: {pass_rate:.0%}  (target: ≥70%)")
    print(f"Target met: {'YES ✓' if target_met else 'NO ✗'}")
    print("="*70)

    # Column legend
    print("\nC1=Signal identification  C2=Literature grounding")
    print("C3=Citation accuracy      C4=Tier consistency")

    # Save to JSON for reporting
    output = {
        "pass_rate":     pass_rate,
        "passes":        passes,
        "total":         total,
        "target_met":    target_met,
        "target":        0.7,
        "results":       [
            {
                "drug_key":            r["drug_key"],
                "pt":                  r["pt"],
                "priority":            r.get("priority"),
                "overall_pass":        r["overall_pass"],
                "signal_identification": r["signal_identification"][0],
                "literature_grounding":  r["literature_grounding"][0],
                "citation_accuracy":     r["citation_accuracy"][0],
                "tier_consistency":      r["tier_consistency"][0],
                "failure_reasons": {
                    k: v[1] for k, v in [
                        ("signal_identification", r["signal_identification"]),
                        ("literature_grounding",  r["literature_grounding"]),
                        ("citation_accuracy",      r["citation_accuracy"]),
                        ("tier_consistency",       r["tier_consistency"]),
                    ] if not v[0]
                }
            }
            for r in results
        ]
    }

    out_path = Path(__file__).parent / "rubric_results.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved to {out_path}")

    return 0 if target_met else 1


if __name__ == "__main__":
    sys.exit(main())