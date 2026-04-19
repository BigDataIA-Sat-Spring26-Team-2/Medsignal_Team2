"""
tests/test_hallucination_check.py — Unit tests for hallucination detection

Run with:
    pytest tests/test_hallucination_check.py -v
    or
    python tests/test_hallucination_check.py
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.models.brief import SafetyBriefOutput
from evaluation.hallucination_check import (
    validate_numerical_accuracy,
    validate_priority_action_consistency,
    validate_citation_grounding,
    validate_brief,
)


def test_numerical_accuracy_clean():
    """Test clean brief with correct numbers."""
    brief = SafetyBriefOutput(
        drug_key="warfarin",
        pt="skin necrosis",
        brief_text="This signal has a PRR of 3.50 with 120 cases reported, including 5 deaths.",
        key_findings=["High PRR", "Deaths reported"],
        pmids_cited=["12345678"],
        recommended_action="LABEL_UPDATE",
        stat_score=0.75,
        lit_score=0.60,
        priority="P1",
        generated_at="2024-01-01T00:00:00Z"
    )

    state = {
        "drug_key": "warfarin",
        "pt": "skin necrosis",
        "prr": 3.50,
        "case_count": 120,
        "death_count": 5,
        "lt_count": 2,
        "hosp_count": 10
    }

    result = validate_numerical_accuracy(brief, state)
    print(f"✓ Clean brief: {result}")
    assert result["hallucination_rate"] == 0.0
    assert len(result["errors"]) == 0


def test_numerical_accuracy_hallucinated_prr():
    """Test brief with incorrect PRR."""
    brief = SafetyBriefOutput(
        drug_key="warfarin",
        pt="skin necrosis",
        brief_text="This signal has a PRR of 10.5 with 120 cases.",  # Wrong PRR
        key_findings=["High PRR"],
        pmids_cited=["12345678"],
        recommended_action="LABEL_UPDATE",
        stat_score=0.75,
        lit_score=0.60,
        priority="P1",
        generated_at="2024-01-01T00:00:00Z"
    )

    state = {
        "prr": 3.50,  # Actual PRR
        "case_count": 120,
        "death_count": 0,
        "lt_count": 0,
        "hosp_count": 0
    }

    result = validate_numerical_accuracy(brief, state)
    print(f"✓ Hallucinated PRR detected: {result}")
    assert result["hallucination_rate"] > 0.0
    assert len(result["errors"]) > 0


def test_numerical_accuracy_false_death_claim():
    """Test brief claiming no deaths when deaths exist."""
    brief = SafetyBriefOutput(
        drug_key="warfarin",
        pt="skin necrosis",
        brief_text="This signal has PRR 3.5 with 120 cases and no deaths reported.",
        key_findings=["No fatalities"],
        pmids_cited=["12345678"],
        recommended_action="MONITOR",
        stat_score=0.65,
        lit_score=0.40,
        priority="P3",
        generated_at="2024-01-01T00:00:00Z"
    )

    state = {
        "prr": 3.50,
        "case_count": 120,
        "death_count": 5,  # Deaths exist but brief denies them
        "lt_count": 0,
        "hosp_count": 0
    }

    result = validate_numerical_accuracy(brief, state)
    print(f"✓ False death claim detected: {result}")
    assert result["hallucination_rate"] > 0.0
    assert any("no death" in err.lower() for err in result["errors"])


def test_priority_action_consistency_valid():
    """Test valid priority-action pairing."""
    brief = SafetyBriefOutput(
        drug_key="gabapentin",
        pt="respiratory depression",
        brief_text="High priority signal requires label update.",
        key_findings=["Strong statistical signal"],
        pmids_cited=["12345678"],
        recommended_action="LABEL_UPDATE",
        stat_score=0.85,
        lit_score=0.65,
        priority="P1",
        generated_at="2024-01-01T00:00:00Z"
    )

    state = {
        "prr": 6.5,
        "death_count": 3,
        "lt_count": 5,
        "hosp_count": 10
    }

    result = validate_priority_action_consistency(brief, state)
    print(f"✓ Valid P1 + LABEL_UPDATE: {result}")
    assert result["hallucination_rate"] == 0.0


def test_priority_action_consistency_invalid_withdraw():
    """Test WITHDRAW without justification."""
    brief = SafetyBriefOutput(
        drug_key="metformin",
        pt="mild nausea",
        brief_text="This drug should be withdrawn.",
        key_findings=["Common adverse event"],
        pmids_cited=["12345678"],
        recommended_action="WITHDRAW",  # Too aggressive
        stat_score=0.45,
        lit_score=0.30,
        priority="P4",
        generated_at="2024-01-01T00:00:00Z"
    )

    state = {
        "prr": 2.1,
        "death_count": 0,  # No deaths
        "lt_count": 0,
        "hosp_count": 0
    }

    result = validate_priority_action_consistency(brief, state)
    print(f"✓ Invalid WITHDRAW detected: {result}")
    assert result["hallucination_rate"] > 0.0
    assert any("WITHDRAW" in err for err in result["errors"])


def test_citation_grounding_no_abstracts():
    """Test when no abstracts are available."""
    brief = SafetyBriefOutput(
        drug_key="warfarin",
        pt="skin necrosis",
        brief_text="This is a known signal [PMID:12345678].",
        key_findings=["Well documented"],
        pmids_cited=["12345678"],
        recommended_action="MONITOR",
        stat_score=0.60,
        lit_score=0.0,  # No abstracts
        priority="P4",
        generated_at="2024-01-01T00:00:00Z"
    )

    abstracts = []  # No abstracts retrieved

    result = validate_citation_grounding(brief, abstracts)
    print(f"✓ No abstracts case: {result}")
    # Should not penalize when no abstracts available
    assert result["hallucination_rate"] == 0.0


def test_citation_grounding_with_valid_citations():
    """Test brief with properly grounded citations."""
    brief = SafetyBriefOutput(
        drug_key="warfarin",
        pt="skin necrosis",
        brief_text=(
            "Warfarin-induced skin necrosis is a rare but serious adverse effect "
            "[PMID:12345678]. The mechanism involves protein C deficiency."
        ),
        key_findings=["Rare but serious", "Protein C mechanism"],
        pmids_cited=["12345678"],
        recommended_action="LABEL_UPDATE",
        stat_score=0.75,
        lit_score=0.70,
        priority="P1",
        generated_at="2024-01-01T00:00:00Z"
    )

    abstracts = [
        {
            "pmid": "12345678",
            "text": (
                "Warfarin-induced skin necrosis is a rare complication occurring in "
                "patients with protein C or protein S deficiency. The mechanism "
                "involves microvascular thrombosis leading to skin necrosis."
            ),
            "similarity": 0.85
        }
    ]

    result = validate_citation_grounding(brief, abstracts)
    print(f"✓ Valid citation grounding: {result}")
    # Should pass with high similarity
    assert result["hallucination_rate"] < 0.5  # May have some variation


def test_composite_validation_clean_brief():
    """Test full validation on a clean brief."""
    brief = SafetyBriefOutput(
        drug_key="dupilumab",
        pt="conjunctivitis",
        brief_text=(
            "This signal shows a PRR of 4.2 with 150 cases including 0 deaths. "
            "Literature supports the association [PMID:12345678]. "
            "Recommend label update to inform prescribers."
        ),
        key_findings=["High PRR", "Literature support", "No fatalities"],
        pmids_cited=["12345678"],
        recommended_action="LABEL_UPDATE",
        stat_score=0.82,
        lit_score=0.68,
        priority="P1",
        generated_at="2024-01-01T00:00:00Z"
    )

    state = {
        "drug_key": "dupilumab",
        "pt": "conjunctivitis",
        "prr": 4.2,
        "case_count": 150,
        "death_count": 0,
        "lt_count": 0,
        "hosp_count": 5
    }

    abstracts = [
        {
            "pmid": "12345678",
            "text": (
                "Dupilumab-associated conjunctivitis is a frequently reported adverse "
                "event in clinical trials. The mechanism involves conjunctival inflammation."
            ),
            "similarity": 0.75
        }
    ]

    result = validate_brief(brief, state, abstracts)
    print(f"\n✓ COMPOSITE VALIDATION - Clean brief:")
    print(f"  Hallucination score: {result['hallucination_score']:.3f}")
    print(f"  Pass: {result['pass']}")
    print(f"  Flags: {len(result['flags'])}")

    assert result["pass"] is True
    assert result["hallucination_score"] < 0.20


def test_composite_validation_problematic_brief():
    """Test full validation on a brief with multiple issues."""
    brief = SafetyBriefOutput(
        drug_key="aspirin",
        pt="headache",
        brief_text=(
            "This signal has a PRR of 15.0 with 500 cases and 20 deaths. "  # Wrong PRR
            "Recommend immediate withdrawal from market."  # Wrong action for P4
        ),
        key_findings=["Extremely high risk"],
        pmids_cited=["99999999"],  # Fabricated PMID
        recommended_action="WITHDRAW",
        stat_score=0.35,  # Low score
        lit_score=0.20,   # Low score
        priority="P4",    # Low priority
        generated_at="2024-01-01T00:00:00Z"
    )

    state = {
        "drug_key": "aspirin",
        "pt": "headache",
        "prr": 2.1,  # Actual PRR much lower
        "case_count": 50,  # Actual count much lower
        "death_count": 0,  # No deaths
        "lt_count": 0,
        "hosp_count": 0
    }

    abstracts = [
        {
            "pmid": "12345678",  # Different PMID
            "text": "Aspirin commonly causes mild headache.",
            "similarity": 0.80
        }
    ]

    result = validate_brief(brief, state, abstracts)
    print(f"\n✓ COMPOSITE VALIDATION - Problematic brief:")
    print(f"  Hallucination score: {result['hallucination_score']:.3f}")
    print(f"  Pass: {result['pass']}")
    print(f"  Flags: {len(result['flags'])}")
    for flag in result['flags'][:5]:  # Show first 5 flags
        print(f"    - {flag}")

    assert result["pass"] is False
    assert result["hallucination_score"] > 0.20
    assert len(result["flags"]) > 0


if __name__ == "__main__":
    print("=" * 80)
    print("HALLUCINATION DETECTION TEST SUITE")
    print("=" * 80)

    print("\n[1] Numerical Accuracy Tests")
    print("-" * 80)
    test_numerical_accuracy_clean()
    test_numerical_accuracy_hallucinated_prr()
    test_numerical_accuracy_false_death_claim()

    print("\n[2] Priority-Action Consistency Tests")
    print("-" * 80)
    test_priority_action_consistency_valid()
    test_priority_action_consistency_invalid_withdraw()

    print("\n[3] Citation Grounding Tests")
    print("-" * 80)
    test_citation_grounding_no_abstracts()
    test_citation_grounding_with_valid_citations()

    print("\n[4] Composite Validation Tests")
    print("-" * 80)
    test_composite_validation_clean_brief()
    test_composite_validation_problematic_brief()

    print("\n" + "=" * 80)
    print("✓ ALL TESTS PASSED")
    print("=" * 80)
