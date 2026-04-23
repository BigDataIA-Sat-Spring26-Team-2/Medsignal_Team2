"""
test_agent1.py — Unit tests for Agent 1: Signal Detector

Tests cover:
    ── Logic correctness ──────────────────────────────────────────────────────
    1.  Golden signal dupilumab — normal GPT-4o call
    2.  High severity signal — death flag (gabapentin + cardio-respiratory)
    3.  Borderline PRR — just above 2.0 threshold (metformin + lactic acidosis)
    4.  High volume — large case count (semaglutide + nausea)
    5.  No outcome flags — all death/hosp/lt = 0
    6.  stat_score not mutated by Agent 1
    7.  All 10 golden signal drugs — template fallback, no API cost

    ── Query strength verification ────────────────────────────────────────────
    8.  Query uniqueness — all 3 must be distinct
    9.  Minimum word count — all queries >= 3 words
    10. GPT-4o adds pharmacological class vs template (specificity check)
    11. All 3 queries cover different angles (no duplicate intent)
    12. Drug name present in at least 1 query

    ── Fallback chain ─────────────────────────────────────────────────────────
    13. Template fallback — direct call, no API needed
    14. GPT-4o returns malformed JSON → template fallback
    15. GPT-4o returns wrong number of queries → template fallback
    16. GPT-4o returns empty string in query list → template fallback
    17. GPT-4o returns duplicate queries → template fallback
    18. GPT-4o fails → Claude Haiku responds → valid queries returned
    19. GPT-4o AND Claude both fail → template fallback (no RuntimeError leak)
    20. Missing OPENAI_API_KEY → auth error → template fallback

Run from project root:
    poetry run python tests/unit/test_agent1.py

Live LLM tests (1-12, 18) only run when OPENAI_API_KEY is set in .env.
Tests 13-17, 19-20 always run — no API cost.
"""

import sys
import os
import pytest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

from app.agents.agent1_detector import agent1_node, generate_queries, _template_queries

# Helpers

PASS = "PASS"
FAIL = "FAIL"


def check(label: str, condition: bool) -> bool:
    status = PASS if condition else FAIL
    print(f"  {status}  {label}")
    return condition


def make_state(drug_key, pt, prr, case_count,
               death_count=0, hosp_count=0, lt_count=0, stat_score=0.5):
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


def assert_valid_queries(queries, drug_key, label="") -> bool:
    """Core assertions applied to every query result."""
    return all([
        check(f"{label}exactly 3 queries",            len(queries) == 3),
        check(f"{label}all non-empty strings",        all(isinstance(q, str) and q.strip() for q in queries)),
        check(f"{label}all >= 3 words",               all(len(q.strip().split()) >= 3 for q in queries)),
        check(f"{label}all unique",                   len(set(q.strip() for q in queries)) == 3),
        check(f"{label}drug name in >= 1 query",      any(drug_key.lower() in q.lower() for q in queries)),
    ])


def assert_valid_result(result: dict, drug_key: str) -> bool:
    """Agent1 node result assertions — stat_score must not be returned."""
    queries = result.get("search_queries", [])
    return all([
        check("returns search_queries key",   "search_queries" in result),
        check("does NOT return stat_score",   "stat_score" not in result),
        *[check(f"query {i+1} valid", isinstance(q, str) and len(q.strip().split()) >= 3)
          for i, q in enumerate(queries)],
    ]) and assert_valid_queries(queries, drug_key)


GOLDEN_DRUGS = [
    ("dupilumab",      "conjunctivitis",            8.43,  412, 0,  12, 3,  0.74),
    ("gabapentin",     "cardio-respiratory arrest", 5.21,  189, 67, 45, 23, 0.91),
    ("metformin",      "lactic acidosis",           30.77, 59,  1,  41, 16, 1.00),
    ("semaglutide",    "nausea",                    3.12,  1823,2,  89, 12, 0.62),
    ("empagliflozin",  "diabetic ketoacidosis",     30.77, 59,  1,  41, 16, 1.00),
    ("finasteride",    "depression",                3.14,  67,  0,  8,  2,  0.48),
    ("bupropion",      "seizure",                   4.20,  89,  3,  22, 7,  0.71),
    ("dapagliflozin",  "decreased glomerular filtration rate", 6.1, 203, 0, 78, 11, 0.83),
    ("dupilumab",      "eczema herpeticum",         12.4,  156, 2,  34, 8,  0.88),
    ("pregabalin",     "dizziness",                 2.31,  445, 0,  18, 4,  0.39),
]


# ── Tests 1-5: Logic correctness (live LLM) ───────────────────────────────────

def test_golden_signal_dupilumab():
    """Test 1 — Normal GPT-4o call, golden signal."""
    print(f"\n{'─'*60}")
    print("TEST 1: Golden signal — dupilumab + conjunctivitis")
    state  = make_state("dupilumab", "conjunctivitis", 8.43, 412,
                        hosp_count=12, lt_count=3, stat_score=0.74)
    result = agent1_node(state)
    print(f"  Queries: {result.get('search_queries', [])}")
    return assert_valid_result(result, "dupilumab")


def test_high_severity_signal():
    """Test 2 — High death count calibrates query angle."""
    print(f"\n{'─'*60}")
    print("TEST 2: High severity — gabapentin + cardio-respiratory arrest")
    state  = make_state("gabapentin", "cardio-respiratory arrest", 5.21, 189,
                        death_count=67, hosp_count=45, lt_count=23, stat_score=0.91)
    result = agent1_node(state)
    print(f"  Queries: {result.get('search_queries', [])}")
    return assert_valid_result(result, "gabapentin")


def test_borderline_prr():
    """Test 3 — Low PRR signal, minimal severity."""
    print(f"\n{'─'*60}")
    print("TEST 3: Borderline PRR — metformin + lactic acidosis")
    state  = make_state("metformin", "lactic acidosis", 2.05, 52,
                        death_count=3, hosp_count=18, lt_count=7, stat_score=0.31)
    result = agent1_node(state)
    print(f"  Queries: {result.get('search_queries', [])}")
    return assert_valid_result(result, "metformin")


def test_high_volume_signal():
    """Test 4 — Large case count."""
    print(f"\n{'─'*60}")
    print("TEST 4: High volume — semaglutide + nausea")
    state  = make_state("semaglutide", "nausea", 3.12, 1823,
                        death_count=2, hosp_count=89, lt_count=12, stat_score=0.62)
    result = agent1_node(state)
    print(f"  Queries: {result.get('search_queries', [])}")
    return assert_valid_result(result, "semaglutide")


def test_no_outcome_flags():
    """Test 5 — All outcome flags zero — 'no serious outcomes reported' in prompt."""
    print(f"\n{'─'*60}")
    print("TEST 5: No outcomes — pregabalin + dizziness")
    state  = make_state("pregabalin", "dizziness", 2.31, 445, stat_score=0.39)
    result = agent1_node(state)
    print(f"  Queries: {result.get('search_queries', [])}")
    return assert_valid_result(result, "pregabalin")


def test_stat_score_not_mutated():
    """Test 6 — Agent 1 must not overwrite stat_score in its return dict."""
    print(f"\n{'─'*60}")
    print("TEST 6: stat_score not mutated")
    state  = make_state("finasteride", "depression", 3.14, 67, stat_score=0.48)
    result = agent1_node(state)
    passed = check("stat_score absent from return dict", "stat_score" not in result)
    passed = passed and check("stat_score unchanged in state", state["stat_score"] == 0.48)
    return passed


def test_all_golden_drugs():
    """Test 7 — Template fallback for all 10 golden signal drugs (no API cost)."""
    print(f"\n{'─'*60}")
    print("TEST 7: All 10 golden drugs via template fallback")
    all_passed = True
    for drug, pt, prr, cases, d, h, lt, ss in GOLDEN_DRUGS:
        queries = _template_queries(drug, pt)
        ok = assert_valid_queries(queries, drug, label=f"{drug}: ")
        all_passed = all_passed and ok
    return all_passed


# ── Tests 8-12: Query strength ────────────────────────────────────────────────

def test_query_uniqueness():
    """Test 8 — All 3 queries must be distinct strings."""
    print(f"\n{'─'*60}")
    print("TEST 8: Query uniqueness — levetiracetam + suicidal ideation")
    state  = make_state("levetiracetam", "suicidal ideation", 4.1, 132,
                        death_count=5, stat_score=0.65)
    result = agent1_node(state)
    queries = result.get("search_queries", [])
    print(f"  Queries: {queries}")
    return check("all 3 queries are unique", len(set(q.strip() for q in queries)) == 3)


def test_minimum_word_count():
    """Test 9 — Every query must have >= 3 words."""
    print(f"\n{'─'*60}")
    print("TEST 9: Minimum word count >= 3 for all queries")
    all_passed = True
    for drug, pt, prr, cases, d, h, lt, ss in GOLDEN_DRUGS[:3]:
        queries = _template_queries(drug, pt)
        for i, q in enumerate(queries):
            ok = check(f"{drug} Q{i+1} has >= 3 words ({len(q.split())} words)",
                       len(q.strip().split()) >= 3)
            all_passed = all_passed and ok
    return all_passed


def test_gpt4o_adds_class_specificity():
    """
    Test 10 — GPT-4o queries must contain at least one term not in template.
    Confirms GPT-4o is injecting pharmacological class vocabulary.
    Tests 3 well-known golden drugs with clear class identifiers.
    """
    print(f"\n{'─'*60}")
    print("TEST 10: GPT-4o adds pharmacological class vs template")
    all_passed = True

    test_signals = [
        ("empagliflozin", "diabetic ketoacidosis", 30.77, 59, 1, 41, 16, 1.0),
        ("semaglutide",   "nausea",                3.12, 1823, 2, 89, 12, 0.62),
        ("dupilumab",     "conjunctivitis",        8.43,  412, 0, 12,  3, 0.74),
    ]

    for drug, pt, prr, cases, d, h, lt, ss in test_signals:
        state    = make_state(drug, pt, prr, cases, d, h, lt, ss)
        result   = agent1_node(state)
        gpt_qs   = result.get("search_queries", [])
        tmpl_qs  = _template_queries(drug, pt)

        gpt_words  = set(" ".join(gpt_qs).lower().split())
        tmpl_words = set(" ".join(tmpl_qs).lower().split())
        new_words  = gpt_words - tmpl_words

        ok = check(
            f"{drug}: GPT-4o added new terms ({', '.join(list(new_words)[:4])})",
            len(new_words) > 0
        )
        all_passed = all_passed and ok
        print(f"    GPT-4o: {gpt_qs}")
        print(f"    Template: {tmpl_qs}")

    return all_passed


def test_queries_cover_different_angles():
    """
    Test 11 — 3 queries should not all be about the same aspect.
    Heuristic: no two queries should share more than 60% of their words
    (excluding the drug name and reaction term which appear in all).
    """
    print(f"\n{'─'*60}")
    print("TEST 11: Queries cover different angles")
    state  = make_state("bupropion", "seizure", 4.2, 89,
                        death_count=3, hosp_count=22, lt_count=7, stat_score=0.71)
    result = agent1_node(state)
    queries = result.get("search_queries", [])

    stop = {"bupropion", "seizure", "the", "of", "and", "in", "a", "an"}
    word_sets = [
        set(q.lower().split()) - stop for q in queries
    ]

    all_passed = True
    pairs = [(0,1), (0,2), (1,2)]
    for i, j in pairs:
        if not word_sets[i] or not word_sets[j]:
            continue
        overlap = len(word_sets[i] & word_sets[j]) / max(len(word_sets[i]), len(word_sets[j]))
        ok = check(f"Q{i+1} vs Q{j+1} overlap {overlap:.0%} < 60%", overlap < 0.60)
        all_passed = all_passed and ok

    print(f"  Queries: {queries}")
    return all_passed


def test_drug_name_in_queries():
    """Test 12 — Drug name must appear in at least 1 query."""
    print(f"\n{'─'*60}")
    print("TEST 12: Drug name in at least 1 query")
    all_passed = True
    for drug, pt, prr, cases, d, h, lt, ss in GOLDEN_DRUGS:
        queries = _template_queries(drug, pt)
        ok = check(
            f"{drug}: name in >= 1 query",
            any(drug.lower() in q.lower() for q in queries)
        )
        all_passed = all_passed and ok
    return all_passed


# ── Tests 13-20: Fallback chain ───────────────────────────────────────────────

def test_template_fallback():
    """Test 13 — Template fallback direct call, no API needed."""
    print(f"\n{'─'*60}")
    print("TEST 13: Template fallback — direct call")
    queries = _template_queries("metformin", "lactic acidosis")
    print(f"  Queries: {queries}")
    return assert_valid_queries(queries, "metformin")


def test_malformed_json_fallback():
    """Test 14 — GPT-4o returns malformed JSON → template fallback."""
    print(f"\n{'─'*60}")
    print("TEST 14: Malformed JSON → template fallback")

    mock_response = MagicMock()
    mock_response.choices[0].message.content = "not valid json at all {{{"
    mock_response.usage.total_tokens = 50
    mock_response.usage.prompt_tokens = 40
    mock_response.usage.completion_tokens = 10
    mock_response.model = "gpt-4o-mini"

    with patch("app.core.llm_router.completion", return_value=mock_response):
        queries = generate_queries("bupropion", "seizure", prr=4.2, case_count=89)

    template = _template_queries("bupropion", "seizure")
    passed = check("fell back to template", queries == template)
    print(f"  Queries: {queries}")
    return passed


def test_wrong_query_count_fallback():
    """Test 15 — GPT-4o returns 2 or 4 queries → template fallback."""
    print(f"\n{'─'*60}")
    print("TEST 15: Wrong query count → template fallback")

    for bad_count, label in [(2, "too few (2)"), (4, "too many (4)")]:
        bad_queries = [f"query {i}" for i in range(bad_count)]
        mock_response = MagicMock()
        mock_response.choices[0].message.content = str(bad_queries).replace("'", '"')
        mock_response.usage.total_tokens = 40
        mock_response.usage.prompt_tokens = 30
        mock_response.usage.completion_tokens = 10
        mock_response.model = "gpt-4o-mini"

        with patch("app.core.llm_router.completion", return_value=mock_response):
            queries = generate_queries("metformin", "lactic acidosis", prr=2.05, case_count=52)

        template = _template_queries("metformin", "lactic acidosis")
        ok = check(f"{label} → template fallback", queries == template)
        print(f"  Queries ({label}): {queries}")

    return ok


def test_empty_string_in_queries_fallback():
    """Test 16 — GPT-4o returns empty string in list → template fallback."""
    print(f"\n{'─'*60}")
    print("TEST 16: Empty string in queries → template fallback")

    bad = '["empagliflozin SGLT2 ketoacidosis mechanism", "", "empagliflozin outcomes management"]'
    mock_response = MagicMock()
    mock_response.choices[0].message.content = bad
    mock_response.usage.total_tokens = 40
    mock_response.usage.prompt_tokens = 30
    mock_response.usage.completion_tokens = 10
    mock_response.model = "gpt-4o-mini"

    with patch("app.core.llm_router.completion", return_value=mock_response):
        queries = generate_queries("empagliflozin", "diabetic ketoacidosis", prr=30.77, case_count=59)

    template = _template_queries("empagliflozin", "diabetic ketoacidosis")
    passed = check("fell back to template on empty string", queries == template)
    print(f"  Queries: {queries}")
    return passed


def test_duplicate_queries_fallback():
    """Test 17 — GPT-4o returns duplicate queries → template fallback."""
    print(f"\n{'─'*60}")
    print("TEST 17: Duplicate queries → template fallback")

    dupe = '["dupilumab conjunctivitis mechanism", "dupilumab conjunctivitis mechanism", "dupilumab outcomes severity"]'
    mock_response = MagicMock()
    mock_response.choices[0].message.content = dupe
    mock_response.usage.total_tokens = 40
    mock_response.usage.prompt_tokens = 30
    mock_response.usage.completion_tokens = 10
    mock_response.model = "gpt-4o-mini"

    with patch("app.core.llm_router.completion", return_value=mock_response):
        queries = generate_queries("dupilumab", "conjunctivitis", prr=8.43, case_count=412)

    template = _template_queries("dupilumab", "conjunctivitis")
    passed = check("fell back to template on duplicate queries", queries == template)
    print(f"  Queries: {queries}")
    return passed


def test_gpt4o_fails_claude_responds():
    """
    Test 18 — GPT-4o fails, Claude Haiku responds with valid queries.
    Simulates the LLMRouter MODEL_CHAIN fallback:
        Call 1 (gpt-4o-mini) → raises Exception
        Call 2 (claude-haiku) → returns valid JSON
    """
    print(f"\n{'─'*60}")
    print("TEST 18: GPT-4o fails → Claude Haiku responds")

    claude_response = MagicMock()
    claude_response.choices[0].message.content = (
        '["semaglutide GLP-1 agonist nausea mechanism",'
        ' "semaglutide nausea incidence clinical trials",'
        ' "semaglutide nausea management dose reduction"]'
    )
    claude_response.usage.total_tokens = 65
    claude_response.usage.prompt_tokens = 50
    claude_response.usage.completion_tokens = 15
    claude_response.model = "claude-haiku-4-5-20251001"

    call_count = {"n": 0}

    def side_effect(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise Exception("RateLimitError: GPT-4o unavailable")
        return claude_response

    with patch("app.core.llm_router.completion", side_effect=side_effect):
        queries = generate_queries("semaglutide", "nausea", prr=3.12, case_count=1823)

    print(f"  Model used: {claude_response.model}")
    print(f"  Queries: {queries}")

    template = _template_queries("semaglutide", "nausea")
    return all([
        check("did NOT fall back to template (Claude succeeded)", queries != template),
        check("exactly 3 queries", len(queries) == 3),
        check("all unique",        len(set(queries)) == 3),
        check("all non-empty",     all(q.strip() for q in queries)),
        check("semaglutide present", any("semaglutide" in q.lower() for q in queries)),
    ])


def test_both_models_fail_template_fallback():
    """
    Test 19 — GPT-4o AND Claude both fail → template fallback, no crash.
    LLMRouter raises RuntimeError after MODEL_CHAIN exhausted.
    agent1_detector catches RuntimeError and returns template.
    """
    print(f"\n{'─'*60}")
    print("TEST 19: Both GPT-4o AND Claude fail → template fallback (no crash)")

    with patch(
        "app.core.llm_router.completion",
        side_effect=Exception("ServiceUnavailableError: All models down"),
    ):
        queries = generate_queries("gabapentin", "cardio-respiratory arrest",
                                   prr=5.21, case_count=189)

    template = _template_queries("gabapentin", "cardio-respiratory arrest")
    print(f"  Queries: {queries}")
    return all([
        check("fell back to template (no crash)", queries == template),
        check("exactly 3 queries",                len(queries) == 3),
        check("all non-empty",                    all(q.strip() for q in queries)),
    ])


def test_missing_api_key_template_fallback():
    """
    Test 20 — Missing OPENAI_API_KEY → auth error → template fallback (no crash).
    """
    print(f"\n{'─'*60}")
    print("TEST 20: Missing API key → template fallback")

    original_key = os.environ.pop("OPENAI_API_KEY", None)
    try:
        with patch(
            "app.core.llm_router.completion",
            side_effect=Exception("AuthenticationError: No API key provided"),
        ):
            queries = generate_queries("bupropion", "seizure", prr=4.2, case_count=89)

        template = _template_queries("bupropion", "seizure")
        print(f"  Queries: {queries}")
        return all([
            check("fell back to template on auth error", queries == template),
            check("exactly 3 queries returned",          len(queries) == 3),
            check("all queries non-empty",               all(q.strip() for q in queries)),
        ])
    finally:
        if original_key:
            os.environ["OPENAI_API_KEY"] = original_key


# ── Runner ────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("MedSignal — Agent 1 Unit Tests")
    print("=" * 60)
    print("Fallback chain: GPT-4o → Claude Haiku → Template")

    using_live_api = bool(os.getenv("OPENAI_API_KEY"))
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    print(f"Mode: {'Live API — ' + model if using_live_api else 'No API key — mock/template tests only'}")
    if not using_live_api:
        print("Tip : Set OPENAI_API_KEY in .env to run live LLM tests (1-12, 18)")

    results = {}

    # Live LLM tests — only run when API key present
    if using_live_api:
        results["1. dupilumab + conjunctivitis (golden)"]    = test_golden_signal_dupilumab()
        results["2. gabapentin + cardio-resp (high sev)"]    = test_high_severity_signal()
        results["3. metformin + lactic acidosis (low PRR)"]  = test_borderline_prr()
        results["4. semaglutide + nausea (high volume)"]     = test_high_volume_signal()
        results["5. pregabalin + dizziness (no outcomes)"]   = test_no_outcome_flags()
        results["6. stat_score not mutated"]                 = test_stat_score_not_mutated()
        results["7. all 10 golden drugs (template)"]         = test_all_golden_drugs()
        results["8. query uniqueness"]                       = test_query_uniqueness()
        results["9. minimum word count"]                     = test_minimum_word_count()
        results["10. GPT-4o adds class specificity"]         = test_gpt4o_adds_class_specificity()
        results["11. queries cover different angles"]        = test_queries_cover_different_angles()
        results["12. drug name in queries"]                  = test_drug_name_in_queries()
        results["18. GPT-4o fails → Claude responds"]        = test_gpt4o_fails_claude_responds()
    else:
        results["7. all 10 golden drugs (template)"]         = test_all_golden_drugs()
        results["9. minimum word count"]                     = test_minimum_word_count()
        results["12. drug name in queries"]                  = test_drug_name_in_queries()

    # Always run — no API cost
    results["13. template fallback direct call"]             = test_template_fallback()
    results["14. malformed JSON → template"]                 = test_malformed_json_fallback()
    results["15. wrong query count → template"]              = test_wrong_query_count_fallback()
    results["16. empty string in queries → template"]        = test_empty_string_in_queries_fallback()
    results["17. duplicate queries → template"]              = test_duplicate_queries_fallback()
    results["19. both models fail → template (no crash)"]    = test_both_models_fail_template_fallback()
    results["20. missing API key → template"]                = test_missing_api_key_template_fallback()

    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    passed = sum(1 for v in results.values() if v)
    total  = len(results)
    for name, ok in results.items():
        print(f"  {PASS if ok else FAIL}  {name}")
    print(f"\n{passed}/{total} tests passed")
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()


# ── pytest-discoverable unit tests ───────────────────────────────────────────

@pytest.mark.unit
def test_validate_queries_rejects_short_queries():
    """
    _validate_queries must reject queries with fewer than MIN_QUERY_WORDS=6 words.
    A 5-word query is insufficient; exactly 6 words passes.
    """
    from app.agents.agent1_detector import _validate_queries

    # 5-word queries — below minimum
    short = [
        "bupropion seizure mechanism adverse",          # 4 words
        "bupropion seizure clinical outcomes",           # 4 words
        "bupropion seizure risk factors",               # 4 words
    ]
    assert not _validate_queries(short), "Queries with < 6 words must fail validation"

    # exactly 6 words — meets minimum
    exact_six = [
        "bupropion seizure mechanism pharmacology adverse reaction",
        "bupropion seizure clinical outcomes incidence risk",
        "bupropion seizure CNS lowering threshold evidence",
    ]
    assert _validate_queries(exact_six), "Queries with exactly 6 words must pass validation"