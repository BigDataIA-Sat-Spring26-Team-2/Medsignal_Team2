"""
test_agent1.py — Unit tests for Agent 1: Signal Detector

Design under test:
    - stat_score is loaded from signals_flagged by pipeline.py at Stage 0
    - Agent 1 only generates search_queries and returns them
    - Agent 1 does NOT compute or return stat_score
    - Mock state includes stat_score as a Stage 0 input (as pipeline.py would load)

Tests cover:
    1. Normal GPT-4o call — golden signal drug (dupilumab + conjunctivitis)
    2. High severity signal — death flag set (gabapentin + cardio-respiratory arrest)
    3. Borderline PRR signal — PRR just above 2.0 threshold
    4. High volume signal — large case count (semaglutide + nausea)
    5. No outcome flags — all death/hosp/lt = 0
    6. Template fallback — direct function call, no API needed
    7. Query uniqueness — all 3 queries must be distinct
    8. All 10 golden signal drugs — template fallback, no API cost

Run from project root:
    poetry run python tests/unit/test_agent1.py

Live GPT-4o tests only run when OPENAI_API_KEY is set in .env.
Template fallback tests (6, 7, 8) always run — no API cost.
"""

import sys
import os

# Point to project root (two levels up from tests/unit/)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

from app.agents.agent1_detector import agent1_node, generate_queries, _template_queries

# ── Helpers ───────────────────────────────────────────────────────────────────

PASS = "PASS"
FAIL = "FAIL"


def check(label: str, condition: bool) -> bool:
    print(f"  {PASS if condition else FAIL}  {label}")
    return condition


def make_state(drug_key: str, pt: str, prr: float, case_count: int,
               death_count: int = 0, hosp_count: int = 0,
               lt_count: int = 0, stat_score: float = 0.5) -> dict:
    """
    Build a mock SignalState as pipeline.py would load from signals_flagged.
    stat_score is a Stage 0 input — always present in state before Agent 1 runs.
    """
    return {
        "drug_key"   : drug_key,
        "pt"         : pt,
        "prr"        : prr,
        "case_count" : case_count,
        "death_count": death_count,
        "hosp_count" : hosp_count,
        "lt_count"   : lt_count,
        "stat_score" : stat_score,
    }


def run_test(name: str, state: dict) -> dict:
    print(f"\n{'─'*60}")
    print(f"TEST: {name}")
    print(f"  drug={state['drug_key']}  pt={state['pt']}  "
          f"prr={state['prr']}  cases={state['case_count']}  "
          f"stat_score={state['stat_score']}")
    result = agent1_node(state)
    return result


def assert_valid_result(result: dict, drug_key: str) -> bool:
    """
    Common assertions for every Agent 1 result.
    Agent 1 returns ONLY search_queries — not stat_score.
    stat_score stays in state from Stage 0, Agent 1 doesn't touch it.
    """
    queries = result.get("search_queries", [])
    return all([
        check("returns search_queries key",        "search_queries" in result),
        check("does NOT return stat_score",        "stat_score" not in result),
        check("exactly 3 queries returned",        len(queries) == 3),
        check("all queries are non-empty strings", all(isinstance(q, str) and q.strip() for q in queries)),
        check("drug name in at least 1 query",     any(drug_key.lower() in q.lower() for q in queries)),
    ])


# ── Test cases ────────────────────────────────────────────────────────────────

def test_golden_signal_dupilumab():
    """
    Test 1 — Normal GPT-4o call with golden signal drug.
    dupilumab + conjunctivitis is the proposal's example signal.
    stat_score=0.74 is a Stage 0 value — Agent 1 must not overwrite it.
    """
    state  = make_state("dupilumab", "conjunctivitis", prr=8.43,
                        case_count=412, hosp_count=12, lt_count=3,
                        stat_score=0.74)
    result = run_test("Golden signal — dupilumab + conjunctivitis", state)
    return assert_valid_result(result, "dupilumab")


def test_high_severity_signal():
    """
    Test 2 — High severity signal with death_count=67.
    gabapentin + cardio-respiratory arrest — pipeline validation checkpoint signal.
    """
    state  = make_state("gabapentin", "cardio-respiratory arrest", prr=5.21,
                        case_count=189, death_count=67, hosp_count=45,
                        lt_count=23, stat_score=0.91)
    result = run_test("High severity — gabapentin + cardio-respiratory arrest", state)
    return assert_valid_result(result, "gabapentin")


def test_borderline_prr_signal():
    """
    Test 3 — Borderline PRR just above threshold (2.0).
    Low statistical strength — barely cleared Branch 2 filters.
    Agent 1 should still generate 3 valid queries.
    """
    state  = make_state("metformin", "lactic acidosis", prr=2.05,
                        case_count=52, death_count=3, hosp_count=18,
                        lt_count=7, stat_score=0.31)
    result = run_test("Borderline PRR — metformin + lactic acidosis", state)
    return assert_valid_result(result, "metformin")


def test_high_volume_signal():
    """
    Test 4 — High volume signal with large case count.
    semaglutide + nausea — GLP-1 drugs heavily reported in FAERS 2023.
    """
    state  = make_state("semaglutide", "nausea", prr=3.87,
                        case_count=4821, hosp_count=89, lt_count=12,
                        stat_score=0.82)
    result = run_test("High volume — semaglutide + nausea", state)
    return assert_valid_result(result, "semaglutide")


def test_no_outcome_flags():
    """
    Test 5 — Signal with all outcome flags zero.
    No deaths, no hospitalisations, no life-threatening events.
    Agent 1 should still generate 3 valid queries — outcome flags
    don't affect query generation.
    """
    state  = make_state("pregabalin", "dizziness", prr=4.12,
                        case_count=334, stat_score=0.48)
    result = run_test("No outcome flags — pregabalin + dizziness", state)
    return assert_valid_result(result, "pregabalin")


def test_template_fallback():
    """
    Test 6 — Template fallback when GPT-4o is unavailable.
    Tests _template_queries() directly — no API call.
    Verifies three distinct angles: mechanistic, epidemiological, clinical.
    """
    print(f"\n{'─'*60}")
    print("TEST: Template fallback — direct function call (no API)")

    drug_key = "empagliflozin"
    pt       = "diabetic ketoacidosis"
    queries  = _template_queries(drug_key, pt)

    passed = all([
        check("exactly 3 template queries",       len(queries) == 3),
        check("all queries non-empty strings",    all(isinstance(q, str) and q.strip() for q in queries)),
        check("drug name in all queries",         all(drug_key in q for q in queries)),
        check("reaction term in all queries",     all(pt in q for q in queries)),
        check("all 3 queries are unique",         len(set(queries)) == 3),
        check("mechanistic angle covered",        any("mechanism" in q or "pharmacology" in q for q in queries)),
        check("epidemiological angle covered",    any("epidemiology" in q or "incidence" in q for q in queries)),
        check("clinical outcomes angle covered",  any("outcomes" in q or "mortality" in q for q in queries)),
    ])

    print("  Template queries:")
    for i, q in enumerate(queries, 1):
        print(f"    Query {i}: {q}")

    return passed


def test_query_uniqueness():
    """
    Test 7 — All 3 queries must be distinct.
    GPT-4o should not return the same query three times.
    """
    state  = make_state("levetiracetam", "suicidal ideation", prr=6.88,
                        case_count=201, death_count=8, hosp_count=34,
                        lt_count=15, stat_score=0.87)
    result = run_test("Query uniqueness — levetiracetam + suicidal ideation", state)
    queries = result.get("search_queries", [])

    return all([
        check("exactly 3 queries",          len(queries) == 3),
        check("all 3 queries are unique",   len(set(queries)) == 3),
        check("all queries non-empty",      all(isinstance(q, str) and q.strip() for q in queries)),
        check("stat_score not in result",   "stat_score" not in result),
    ])


def test_all_golden_drugs():
    """
    Test 8 — All 10 golden signal drugs produce valid queries.
    Uses template fallback — no API call, no cost.
    Confirms Agent 1 handles every golden drug without crashing.
    stat_score is included in mock state as it would be from signals_flagged.
    """
    print(f"\n{'─'*60}")
    print("TEST: All 10 golden signal drugs (template fallback)")

    golden_signals = [
        ("dupilumab",     "conjunctivitis",          0.74),
        ("gabapentin",    "cardio-respiratory arrest",0.91),
        ("pregabalin",    "dizziness",                0.48),
        ("levetiracetam", "suicidal ideation",        0.87),
        ("tirzepatide",   "pancreatitis",             0.65),
        ("semaglutide",   "nausea",                   0.82),
        ("empagliflozin", "diabetic ketoacidosis",    0.71),
        ("bupropion",     "seizure",                  0.78),
        ("dapagliflozin", "urinary tract infection",  0.53),
        ("metformin",     "lactic acidosis",           0.31),
    ]

    all_passed = True
    for drug_key, pt, stat_score in golden_signals:
        queries = _template_queries(drug_key, pt)
        ok = (
            len(queries) == 3
            and all(isinstance(q, str) and q.strip() for q in queries)
            and all(drug_key in q for q in queries)
            and len(set(queries)) == 3
        )
        print(f"  {PASS if ok else FAIL}  {drug_key} + {pt}  (stat_score={stat_score})")
        all_passed = all_passed and ok

    return all_passed


# ── Runner ────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("MedSignal — Agent 1 Unit Tests")
    print("=" * 60)
    print("Design: stat_score loaded from signals_flagged at Stage 0")
    print("        Agent 1 returns search_queries only")

    using_live_api = bool(os.getenv("OPENAI_API_KEY"))
    print(f"Mode  : {'Live GPT-4o calls (' + os.getenv('OPENAI_MODEL', 'gpt-4o-mini') + ')' if using_live_api else 'Template fallback only (no API key)'}")
    if not using_live_api:
        print("Tip   : Set OPENAI_API_KEY in .env to run live GPT-4o tests")

    results = {}

    if using_live_api:
        results["dupilumab + conjunctivitis"]            = test_golden_signal_dupilumab()
        results["gabapentin + cardio-respiratory arrest"]= test_high_severity_signal()
        results["metformin + lactic acidosis (low PRR)"] = test_borderline_prr_signal()
        results["semaglutide + nausea (high volume)"]    = test_high_volume_signal()
        results["pregabalin + dizziness (no outcomes)"]  = test_no_outcome_flags()
        results["levetiracetam + suicidal (uniqueness)"] = test_query_uniqueness()

    # Always run — no API call needed
    results["template fallback (3 angles)"] = test_template_fallback()
    results["all 10 golden drugs"]          = test_all_golden_drugs()

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    passed = sum(results.values())
    total  = len(results)
    for name, ok in results.items():
        print(f"  {PASS if ok else FAIL}  {name}")
    print(f"\n{passed}/{total} tests passed")

    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()