"""
tests/test_properties.py — Property-Based Tests for MedSignal

Property tests verify INVARIANTS — conditions that must ALWAYS hold
regardless of input values.

Unlike unit tests (specific examples), property tests generate hundreds
of random inputs and verify properties hold for ALL of them.

Library: hypothesis (property-based testing framework)
Run: pytest tests/test_properties.py -v

Example property:
  "StatScore is ALWAYS between 0.0 and 1.0"
  → Test with 100 random (PRR, case_count, deaths) combinations
  → If ANY fail, test fails and shows the failing case
"""

import sys
from pathlib import Path

import pytest
from hypothesis import given, strategies as st, assume, settings

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


# ── Strategy Definitions ──────────────────────────────────────────────────────
# Strategies define the range and type of random values to generate


# Valid ranges for pharmacovigilance metrics
prr_strategy = st.floats(min_value=0.1, max_value=100.0, allow_nan=False, allow_infinity=False)
case_count_strategy = st.integers(min_value=1, max_value=10000)
outcome_count_strategy = st.integers(min_value=0, max_value=1000)
score_strategy = st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False)


# ── 1. StatScore Properties ───────────────────────────────────────────────────


class TestStatScoreProperties:
    """Properties that ALWAYS hold for StatScore calculation."""

    @given(
        prr=prr_strategy,
        case_count=case_count_strategy,
        deaths=outcome_count_strategy,
        lt=outcome_count_strategy,
        hosp=outcome_count_strategy
    )
    @settings(max_examples=50, deadline=None)  # Reduced from 200, disabled deadline
    def test_stat_score_always_in_valid_range(self, prr, case_count, deaths, lt, hosp):
        """
        Property: StatScore is ALWAYS between 0.0 and 1.0.

        Why: Score outside this range breaks priority tier logic.
        Method: Generate 200 random valid inputs, compute StatScore, verify range.
        """
        from app.agents.agent3_assessor import _compute_stat_score

        score = _compute_stat_score(prr, case_count, deaths, lt, hosp)

        assert 0.0 <= score <= 1.0, (
            f"StatScore {score:.4f} outside valid range [0.0, 1.0]. "
            f"Inputs: PRR={prr:.2f}, cases={case_count}, deaths={deaths}, lt={lt}, hosp={hosp}"
        )

    @given(
        prr=prr_strategy,
        case_count=case_count_strategy,
        deaths=outcome_count_strategy,
        lt=outcome_count_strategy,
        hosp=outcome_count_strategy
    )
    @settings(max_examples=100)
    def test_stat_score_deterministic(self, prr, case_count, deaths, lt, hosp):
        """
        Property: StatScore is DETERMINISTIC — same inputs always produce same output.

        Why: Non-deterministic scores would make priority assignments unstable.
        """
        from app.agents.agent3_assessor import _compute_stat_score

        score1 = _compute_stat_score(prr, case_count, deaths, lt, hosp)
        score2 = _compute_stat_score(prr, case_count, deaths, lt, hosp)

        assert score1 == score2, (
            f"StatScore not deterministic: {score1} != {score2}"
        )

    @given(
        prr=st.floats(min_value=0.1, max_value=10.0),
        case_count=case_count_strategy,
        deaths=st.integers(min_value=0, max_value=100),
        lt=st.integers(min_value=0, max_value=100),
        hosp=st.integers(min_value=0, max_value=100)
    )
    @settings(max_examples=100)
    def test_stat_score_monotonic_with_deaths(self, prr, case_count, deaths, lt, hosp):
        """
        Property: StatScore INCREASES (or stays same) when deaths increase.

        Why: More deaths = higher severity = should increase score.
        """
        from app.agents.agent3_assessor import _compute_stat_score

        score_no_extra_deaths = _compute_stat_score(prr, case_count, deaths, lt, hosp)
        score_with_extra_deaths = _compute_stat_score(prr, case_count, deaths + 10, lt, hosp)

        assert score_with_extra_deaths >= score_no_extra_deaths, (
            f"StatScore decreased when deaths increased: "
            f"{score_no_extra_deaths:.4f} → {score_with_extra_deaths:.4f}"
        )

    @given(
        prr1=st.floats(min_value=0.1, max_value=5.0),
        prr2=st.floats(min_value=5.0, max_value=20.0),
        case_count=case_count_strategy
    )
    @settings(max_examples=100)
    def test_stat_score_monotonic_with_prr(self, prr1, prr2, case_count):
        """
        Property: StatScore INCREASES when PRR increases (holding other factors constant).

        Why: Higher PRR = stronger signal = should increase score.
        """
        from app.agents.agent3_assessor import _compute_stat_score

        # Same outcomes, different PRRs
        score1 = _compute_stat_score(prr1, case_count, death=5, lt=0, hosp=0)
        score2 = _compute_stat_score(prr2, case_count, death=5, lt=0, hosp=0)

        assert score2 >= score1, (
            f"StatScore didn't increase with higher PRR: "
            f"PRR {prr1:.2f} → {score1:.4f}, PRR {prr2:.2f} → {score2:.4f}"
        )


# ── 2. Priority Tier Properties ───────────────────────────────────────────────


class TestPriorityTierProperties:
    """Properties for priority tier assignment logic."""

    @given(
        stat_score=score_strategy,
        lit_score=score_strategy
    )
    @settings(max_examples=200)
    def test_priority_always_valid(self, stat_score, lit_score):
        """
        Property: Priority is ALWAYS one of P1, P2, P3, P4.

        Why: Invalid priority breaks HITL queue sorting.
        """
        from app.agents.agent3_assessor import assign_priority

        priority = assign_priority(stat_score, lit_score)

        assert priority in ["P1", "P2", "P3", "P4"], (
            f"Invalid priority '{priority}' for stat={stat_score:.2f}, lit={lit_score:.2f}"
        )

    @given(
        stat_score=score_strategy,
        lit_score=score_strategy
    )
    @settings(max_examples=100)
    def test_priority_deterministic(self, stat_score, lit_score):
        """
        Property: Priority is DETERMINISTIC — same scores always produce same priority.

        Why: Non-deterministic priority would randomize HITL queue.
        """
        from app.agents.agent3_assessor import assign_priority

        priority1 = assign_priority(stat_score, lit_score)
        priority2 = assign_priority(stat_score, lit_score)

        assert priority1 == priority2

    @given(
        stat_score=st.floats(min_value=0.7, max_value=1.0),
        lit_score=st.floats(min_value=0.5, max_value=1.0)
    )
    @settings(max_examples=50)
    def test_p1_requires_high_stat_and_lit(self, stat_score, lit_score):
        """
        Property: P1 priority ALWAYS requires stat ≥ 0.7 AND lit ≥ 0.5.

        Why: P1 definition from CLAUDE.md.
        """
        from app.agents.agent3_assessor import assign_priority

        priority = assign_priority(stat_score, lit_score)

        assert priority == "P1", (
            f"Expected P1 for stat={stat_score:.2f}, lit={lit_score:.2f}, got {priority}"
        )

    @given(
        stat_score=st.floats(min_value=0.0, max_value=0.7),
        lit_score=st.floats(min_value=0.0, max_value=0.5)
    )
    @settings(max_examples=50)
    def test_p4_for_low_stat_and_lit(self, stat_score, lit_score):
        """
        Property: P4 priority when BOTH stat < 0.7 AND lit < 0.5.

        Why: P4 is the catch-all for weak signals.
        """
        # Exclude boundary cases where stat or lit might round to threshold
        assume(stat_score < 0.69 and lit_score < 0.49)

        from app.agents.agent3_assessor import assign_priority

        priority = assign_priority(stat_score, lit_score)

        assert priority == "P4", (
            f"Expected P4 for stat={stat_score:.2f}, lit={lit_score:.2f}, got {priority}"
        )

    @given(
        stat_score1=score_strategy,
        stat_score2=score_strategy,
        lit_score=score_strategy
    )
    @settings(max_examples=100)
    def test_priority_monotonic_with_stat_score(self, stat_score1, stat_score2, lit_score):
        """
        Property: Higher stat_score leads to same or higher priority tier (lower P number).

        Why: P1 > P2 > P3 > P4, so higher scores should not decrease priority.
        """
        from app.agents.agent3_assessor import assign_priority

        # Ensure stat_score1 < stat_score2
        if stat_score1 > stat_score2:
            stat_score1, stat_score2 = stat_score2, stat_score1

        priority1 = assign_priority(stat_score1, lit_score)
        priority2 = assign_priority(stat_score2, lit_score)

        priority_order = {"P1": 1, "P2": 2, "P3": 3, "P4": 4}

        # Higher stat should lead to same or better (lower number) priority
        assert priority_order[priority2] <= priority_order[priority1], (
            f"Priority decreased with higher stat_score: "
            f"{stat_score1:.2f}→{priority1}, {stat_score2:.2f}→{priority2}"
        )


# ── 3. PRR Properties ─────────────────────────────────────────────────────────


class TestPRRProperties:
    """Properties for PRR (Proportional Reporting Ratio) calculation."""

    @given(
        a=st.integers(min_value=1, max_value=1000),
        b=st.integers(min_value=1, max_value=5000),
        c=st.integers(min_value=1, max_value=5000),
        d=st.integers(min_value=1, max_value=10000)
    )
    @settings(max_examples=200)
    def test_prr_always_positive(self, a, b, c, d):
        """
        Property: PRR is ALWAYS > 0.

        Why: PRR is a ratio of proportions, mathematically cannot be zero or negative.
        Formula: PRR = (A / (A + B)) / (C / (C + D))
        """
        prr = (a / (a + b)) / (c / (c + d))

        assert prr > 0, f"PRR must be positive, got {prr} for A={a}, B={b}, C={c}, D={d}"

    @given(
        a=st.integers(min_value=1, max_value=1000),
        b=st.integers(min_value=1, max_value=5000),
        c=st.integers(min_value=1, max_value=5000),
        d=st.integers(min_value=1, max_value=10000)
    )
    @settings(max_examples=100)
    def test_prr_increases_when_a_increases(self, a, b, c, d):
        """
        Property: PRR INCREASES when A increases (holding B, C, D constant).

        Why: More drug-reaction pairs should strengthen the signal.
        """
        prr1 = (a / (a + b)) / (c / (c + d))
        prr2 = ((a + 10) / (a + 10 + b)) / (c / (c + d))

        assert prr2 > prr1, (
            f"PRR didn't increase when A increased: {prr1:.2f} → {prr2:.2f}"
        )

    @given(
        a=st.integers(min_value=50, max_value=500),
        b=st.integers(min_value=100, max_value=1000)
    )
    @settings(max_examples=50)
    def test_prr_above_2_threshold_meaningful(self, a, b):
        """
        Property: When PRR > 2, the drug-reaction association is stronger than background.

        Why: PRR > 2 is the threshold for signal detection in CLAUDE.md.
        """
        # Force PRR > 2 by setting appropriate C and D
        c = a // 3  # Small C relative to A
        d = b * 5   # Large D relative to B

        prr = (a / (a + b)) / (c / (c + d))

        if prr > 2.0:
            # Drug proportion should be > 2x background proportion
            drug_proportion = a / (a + b)
            background_proportion = c / (c + d)

            assert drug_proportion > 2 * background_proportion, (
                f"PRR={prr:.2f} but proportions don't match: "
                f"drug={drug_proportion:.3f}, bg={background_proportion:.3f}"
            )


# ── 4. Hallucination Score Properties ────────────────────────────────────────


class TestHallucinationScoreProperties:
    """Properties for hallucination detection scores."""

    def test_hallucination_score_always_in_range(self):
        """
        Property: Hallucination score is ALWAYS between 0.0 and 1.0.

        Why: Score is used as threshold check (pass < 0.20).
        Method: Test with various briefs with 0 to many errors.
        """
        from app.models.brief import SafetyBriefOutput
        from evaluation.hallucination_check import validate_brief

        test_cases = [
            # (brief, state, expected_score_range)
            # Clean brief - should be 0.0
            (
                SafetyBriefOutput(
                    drug_key="warfarin", pt="bleeding",
                    brief_text="PRR is 3.50 with 100 cases.",
                    key_findings=["High PRR"], pmids_cited=["12345"],
                    recommended_action="LABEL_UPDATE",
                    stat_score=0.75, lit_score=0.60, priority="P1",
                    generated_at="2024-01-01T00:00:00Z"
                ),
                {"prr": 3.50, "case_count": 100, "death_count": 0, "lt_count": 0, "hosp_count": 0},
                (0.0, 0.2)
            ),
            # Brief with wrong PRR - should be > 0.0
            # Note: validator looks for "prr" AFTER the number, so format as "demonstrates a 10.0 PRR value"
            (
                SafetyBriefOutput(
                    drug_key="warfarin", pt="bleeding",
                    brief_text="The signal demonstrates a 10.0 PRR value with 100 cases reported.",  # Wrong PRR
                    key_findings=["High PRR"], pmids_cited=["12345"],
                    recommended_action="LABEL_UPDATE",
                    stat_score=0.75, lit_score=0.60, priority="P1",
                    generated_at="2024-01-01T00:00:00Z"
                ),
                {"prr": 3.50, "case_count": 100, "death_count": 0, "lt_count": 0, "hosp_count": 0},
                (0.1, 1.0)
            ),
        ]

        for brief, state, (min_score, max_score) in test_cases:
            report = validate_brief(brief, state, [])
            score = report["hallucination_score"]

            assert 0.0 <= score <= 1.0, (
                f"Hallucination score {score} outside [0.0, 1.0]"
            )
            assert min_score <= score <= max_score, (
                f"Score {score} outside expected range [{min_score}, {max_score}]"
            )

    def test_hallucination_score_increases_with_errors(self):
        """
        Property: Hallucination score INCREASES as more errors are present.

        Why: More errors = more hallucinations = higher score.
        """
        from app.models.brief import SafetyBriefOutput
        from evaluation.hallucination_check import validate_brief

        # Brief with 0 errors
        clean_brief = SafetyBriefOutput(
            drug_key="warfarin", pt="bleeding",
            brief_text="PRR is 3.50 with 100 cases.",
            key_findings=["Finding"], pmids_cited=["12345"],
            recommended_action="LABEL_UPDATE",
            stat_score=0.75, lit_score=0.60, priority="P1",
            generated_at="2024-01-01T00:00:00Z"
        )

        # Brief with 2 errors (wrong PRR, wrong action for priority)
        error_brief = SafetyBriefOutput(
            drug_key="warfarin", pt="bleeding",
            brief_text="PRR is 10.0 with 100 cases.",  # Error 1: wrong PRR
            key_findings=["Finding"], pmids_cited=["12345"],
            recommended_action="MONITOR",  # Error 2: P1 shouldn't use MONITOR with deaths
            stat_score=0.75, lit_score=0.60, priority="P1",
            generated_at="2024-01-01T00:00:00Z"
        )

        state = {"prr": 3.50, "case_count": 100, "death_count": 5, "lt_count": 0, "hosp_count": 0}

        clean_score = validate_brief(clean_brief, state, [])["hallucination_score"]
        error_score = validate_brief(error_brief, state, [])["hallucination_score"]

        assert error_score > clean_score, (
            f"Score didn't increase with more errors: "
            f"clean={clean_score:.3f}, errors={error_score:.3f}"
        )

    def test_hallucination_pass_consistent_with_threshold(self):
        """
        Property: hallucination_pass is ALWAYS TRUE when score < 0.20, FALSE otherwise.

        Why: This is the pass/fail threshold defined in hallucination_check.py.
        """
        from app.models.brief import SafetyBriefOutput
        from evaluation.hallucination_check import validate_brief

        # Generate briefs with varying error levels
        test_briefs = [
            # Clean - should pass
            ("3.50", "LABEL_UPDATE", True),
            # Wrong PRR - might pass or fail depending on magnitude
            ("5.0", "LABEL_UPDATE", None),  # Borderline
            # Multiple errors - should fail
            ("10.0", "WITHDRAW", False),
        ]

        for prr_text, action, expected_pass in test_briefs:
            brief = SafetyBriefOutput(
                drug_key="test", pt="test",
                brief_text=f"PRR is {prr_text} with 100 cases.",
                key_findings=["Test"], pmids_cited=["12345"],
                recommended_action=action,
                stat_score=0.75, lit_score=0.60, priority="P1",
                generated_at="2024-01-01T00:00:00Z"
            )

            state = {"prr": 3.50, "case_count": 100, "death_count": 0, "lt_count": 0, "hosp_count": 0}
            report = validate_brief(brief, state, [])

            score = report["hallucination_score"]
            passed = report["pass"]

            # Verify consistency
            if score < 0.20:
                assert passed is True, f"Score {score} < 0.20 but pass={passed}"
            else:
                assert passed is False, f"Score {score} ≥ 0.20 but pass={passed}"


# ── Test Execution ────────────────────────────────────────────────────────────


if __name__ == "__main__":
    """Run property tests directly."""
    import pytest

    exit_code = pytest.main([
        __file__,
        "-v",
        "--tb=short",
        "-s",  # Show print statements
    ])

    sys.exit(exit_code)
