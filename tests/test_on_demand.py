"""
test_on_demand.py — On-demand pipeline workflow tests

Tests Workflow 2: run_single_signal(drug_key, pt)

Run: .venv\Scripts\python.exe -m pytest tests/test_on_demand.py -v -s

Tests:
    1. Known golden signal — dupilumab x conjunctivitis
       Already in safety_briefs — pipeline reruns and updates
    2. Non-existent signal — aspirin x headache
       Not in signals_flagged — must raise ValueError
    3. Priority tier is valid — one of P1/P2/P3/P4
    4. Brief is generated — not None
    5. No generation error
"""

import logging
import pytest

logging.basicConfig(
    level  =logging.INFO,
    format ="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def dupilumab_result():
    """
    Run on-demand pipeline for dupilumab x conjunctivitis once.
    Shared across all tests in this module — avoids running pipeline
    multiple times and wasting OpenAI tokens.
    scope=module means fixture runs once for all tests in this file.
    """
    from app.agents.pipeline import run_single_signal
    return run_single_signal("dupilumab", "conjunctivitis")


# ── Test 1: Known signal runs without error ───────────────────────────────────

def test_known_signal_completes(dupilumab_result):
    """
    Pipeline must complete for a known golden signal.
    No exception, no generation_error.
    """
    result = dupilumab_result

    print(f"\nPriority  : {result.get('priority')}")
    print(f"Stat score: {result.get('stat_score')}")
    print(f"Lit score : {result.get('lit_score')}")
    print(f"Error     : {result.get('error')}")

    assert result is not None
    assert result.get("error") is None


# ── Test 2: Priority tier is valid ────────────────────────────────────────────

def test_priority_tier_valid(dupilumab_result):
    """
    Priority must be one of P1/P2/P3/P4.
    dupilumab x conjunctivitis is a known P1 signal.
    """
    priority = dupilumab_result.get("priority")
    print(f"\nPriority: {priority}")

    assert priority in ("P1", "P2", "P3", "P4")
    assert priority == "P1", (
        f"dupilumab x conjunctivitis expected P1, got {priority}. "
        f"stat_score={dupilumab_result.get('stat_score')} "
        f"lit_score={dupilumab_result.get('lit_score')}"
    )


# ── Test 3: SafetyBrief is generated ─────────────────────────────────────────

def test_brief_generated(dupilumab_result):
    """
    SafetyBrief must be generated — not None.
    Confirms Agent 3 ran successfully and Pydantic validation passed.
    """
    brief = dupilumab_result.get("brief")
    print(f"\nBrief keys: {list(brief.keys()) if brief else 'None'}")

    assert brief is not None
    assert "brief_text" in brief
    assert "key_findings" in brief
    assert "pmids_cited" in brief
    assert "recommended_action" in brief


# ── Test 4: Brief content is valid ────────────────────────────────────────────

def test_brief_content_valid(dupilumab_result):
    """
    SafetyBrief content must be clinically meaningful.
    Drug and reaction must appear in brief_text.
    key_findings must be a non-empty list.
    recommended_action must be one of four valid values.
    """
    brief = dupilumab_result.get("brief")

    # Drug and reaction named correctly
    assert "dupilumab" in brief["brief_text"].lower(), \
        "brief_text does not mention dupilumab"
    assert "conjunctivitis" in brief["brief_text"].lower(), \
        "brief_text does not mention conjunctivitis"

    # Key findings is a non-empty list
    assert isinstance(brief["key_findings"], list)
    assert len(brief["key_findings"]) > 0

    # Recommended action is valid
    assert brief["recommended_action"] in (
        "MONITOR", "LABEL_UPDATE", "RESTRICT", "WITHDRAW"
    ), f"Invalid recommended_action: {brief['recommended_action']}"

    print(f"\nBrief preview : {brief['brief_text'][:150]}")
    print(f"Key findings  : {brief['key_findings']}")
    print(f"Action        : {brief['recommended_action']}")
    print(f"PMIDs cited   : {brief['pmids_cited']}")


# ── Test 5: Scores are in valid range ─────────────────────────────────────────

def test_scores_in_range(dupilumab_result):
    """
    stat_score and lit_score must be in [0.0, 1.0].
    Confirms StatScore and LitScore formulas are correct.
    """
    stat = dupilumab_result.get("stat_score")
    lit  = dupilumab_result.get("lit_score")

    print(f"\nstat_score: {stat}")
    print(f"lit_score : {lit}")

    assert stat is not None
    assert lit  is not None
    assert 0.0 <= stat <= 1.0, f"stat_score {stat} out of range"
    assert 0.0 <= lit  <= 1.0, f"lit_score {lit} out of range"


# ── Test 6: Snowflake was updated ─────────────────────────────────────────────

def test_snowflake_updated():
    """
    After on-demand run, Snowflake safety_briefs must have a row
    for dupilumab x conjunctivitis.
    Confirms Agent 3 wrote to Snowflake successfully.
    """
    import os
    import snowflake.connector
    from dotenv import load_dotenv
    load_dotenv()

    conn = snowflake.connector.connect(
        account  =os.getenv("SNOWFLAKE_ACCOUNT"),
        user     =os.getenv("SNOWFLAKE_USER"),
        password =os.getenv("SNOWFLAKE_PASSWORD"),
        database =os.getenv("SNOWFLAKE_DATABASE"),
        schema   =os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT drug_key, pt, priority, stat_score, lit_score,
               generation_error
        FROM safety_briefs
        WHERE drug_key = 'dupilumab'
        AND   pt       = 'conjunctivitis'
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()

    print(f"\nSnowflake row: {row}")

    assert row is not None, "No row found in safety_briefs"
    assert row[2] in ("P1", "P2", "P3", "P4")  # priority
    assert row[5] == False                       # generation_error


# ── Test 7: Non-existent signal raises ValueError ─────────────────────────────

def test_nonexistent_signal_raises():
    """
    Signal not in signals_flagged must raise ValueError with clear message.
    Confirms load_single_signal() error handling works correctly.
    """
    from app.agents.pipeline import run_single_signal

    with pytest.raises(ValueError) as exc:
        run_single_signal("aspirin", "headache")

    print(f"\nCorrectly raised: {exc.value}")
    assert "not found" in str(exc.value).lower()

@pytest.fixture(scope="module")
def tenofovir_result():
    """
    Non-golden signal — tenofovir disoproxil x renal injury.
    Tests ensure_drug_loaded() — fetches PubMed abstracts on demand
    since tenofovir is not in the pre-loaded ChromaDB corpus.
    Takes ~30-60 seconds on first run.
    """
    from app.agents.pipeline import run_single_signal
    return run_single_signal("tenofovir disoproxil", "renal injury")


def test_nongolden_signal_completes(tenofovir_result):
    """Non-golden signal must complete without error."""
    result = tenofovir_result
    print(f"\nPriority  : {result.get('priority')}")
    print(f"Stat score: {result.get('stat_score')}")
    print(f"Lit score : {result.get('lit_score')}")
    print(f"Error     : {result.get('error')}")

    assert result.get("error") is None
    assert result.get("priority") in ("P1", "P2", "P3", "P4")


def test_nongolden_brief_generated(tenofovir_result):
    """SafetyBrief must be generated for non-golden signal."""
    brief = tenofovir_result.get("brief")
    print(f"\nBrief preview : {str(brief.get('brief_text', ''))[:200]}")
    print(f"PMIDs cited   : {brief.get('pmids_cited')}")
    print(f"Action        : {brief.get('recommended_action')}")

    assert brief is not None
    assert "tenofovir" in brief["brief_text"].lower()
    assert brief["recommended_action"] in (
        "MONITOR", "LABEL_UPDATE", "RESTRICT", "WITHDRAW"
    )