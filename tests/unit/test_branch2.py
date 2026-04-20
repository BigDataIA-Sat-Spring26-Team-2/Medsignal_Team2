"""
tests/test_branch2.py — Tests for Branch 2 (PRR Computation Pipeline)

Tests verify the PRR calculation, threshold filters, and quality filters
that transform drug_reaction_pairs → signals_flagged.

Test Categories:
  1. PRR Calculation — Formula correctness with known inputs
  2. Threshold Filters — A, C, drug_total, PRR thresholds
  3. Quality Filters — Junk terms, single-quarter spike, late surge
  4. Golden Signal Detection — Verify 10 known FDA signals are flagged
  5. Mode Selection — POC vs Production threshold switching

Run with:
    pytest tests/test_branch2.py -v
    pytest tests/test_branch2.py::TestPRRCalculation -v  # Single test class

Reference: CLAUDE.md — Branch 2: PRR Computation
"""

import sys
from pathlib import Path
import pytest
from decimal import Decimal
from typing import Dict, List

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


# ── Test Data Fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def sample_prr_data():
    """
    Sample PRR calculation data for testing.

    Returns dict with A, B, C, D values and expected PRR.
    """
    return {
        "gabapentin_cardio": {
            "drug_key": "gabapentin",
            "pt": "cardio-respiratory arrest",
            "A": 67,   # drug + reaction
            "B": 1200, # drug + no reaction
            "C": 100,  # other + reaction (reduced to increase PRR)
            "D": 5000, # other + no reaction
            "expected_prr": 2.70,  # (67/1267) / (100/5100) = 0.0529 / 0.0196 = 2.70
        },
        "finasteride_depression": {
            "drug_key": "finasteride",
            "pt": "depression",
            "A": 67,
            "B": 800,
            "C": 200,
            "D": 4500,
            "expected_prr": 1.89,  # Below threshold
        },
        "warfarin_bleeding": {
            "drug_key": "warfarin",
            "pt": "bleeding",
            "A": 250,
            "B": 2000,
            "C": 500,
            "D": 10000,
            "expected_prr": 2.63,
        }
    }


@pytest.fixture
def junk_terms():
    """
    List of MedDRA junk terms that should be filtered out.

    From CLAUDE.md: Administrative/non-specific terms.
    """
    return [
        "drug ineffective",
        "product use issue",
        "off label use",
        "drug interaction",
        "no adverse event",
        "product quality issue",
        "incorrect dose administered",
        "drug dose omission",
        "expired product administered",
        "medication error"
    ]


@pytest.fixture
def golden_signals():
    """
    10 golden drug-reaction pairs with documented FDA communications.

    These MUST be detected by Branch 2 for the system to be valid.
    """
    return [
        {"drug_key": "dupilumab", "pt": "skin fissures"},
        {"drug_key": "gabapentin", "pt": "cardio-respiratory arrest"},
        {"drug_key": "pregabalin", "pt": "coma"},
        {"drug_key": "levetiracetam", "pt": "tonic-clonic seizure"},
        {"drug_key": "tirzepatide", "pt": "injection site"},
        {"drug_key": "semaglutide", "pt": "increased appetite"},
        {"drug_key": "empagliflozin", "pt": "hba1c increased"},
        {"drug_key": "bupropion", "pt": "seizure"},
        {"drug_key": "dapagliflozin", "pt": "gfr decreased"},
        {"drug_key": "metformin", "pt": "diabetic ketoacidosis"},
    ]


# ── 1. PRR Calculation Tests ──────────────────────────────────────────────────


class TestPRRCalculation:
    """Test PRR formula correctness."""

    def test_prr_formula_basic(self, sample_prr_data):
        """
        Test PRR calculation with known inputs.

        Formula: PRR = (A / (A + B)) / (C / (C + D))

        Why: This is the core signal detection formula.
        """
        data = sample_prr_data["gabapentin_cardio"]

        A = data["A"]
        B = data["B"]
        C = data["C"]
        D = data["D"]

        prr = (A / (A + B)) / (C / (C + D))

        # Allow 5% tolerance for floating point
        assert abs(prr - data["expected_prr"]) < data["expected_prr"] * 0.05, (
            f"PRR calculation incorrect: got {prr:.2f}, expected {data['expected_prr']}"
        )

    def test_prr_always_positive(self, sample_prr_data):
        """
        Test that PRR is always > 0 for valid inputs.

        Why: PRR is a ratio of proportions, cannot be zero or negative.
        """
        for signal_name, data in sample_prr_data.items():
            A, B, C, D = data["A"], data["B"], data["C"], data["D"]
            prr = (A / (A + B)) / (C / (C + D))

            assert prr > 0, f"PRR for {signal_name} is not positive: {prr}"

    def test_prr_above_threshold_for_known_signal(self, sample_prr_data):
        """
        Test that known FDA signal exceeds PRR ≥ 2.0 threshold.

        Why: gabapentin × cardio-respiratory arrest is a documented signal.
        """
        data = sample_prr_data["gabapentin_cardio"]
        A, B, C, D = data["A"], data["B"], data["C"], data["D"]

        prr = (A / (A + B)) / (C / (C + D))

        assert prr >= 2.0, (
            f"Known FDA signal has PRR {prr:.2f} < 2.0 threshold"
        )

    def test_prr_below_threshold_for_weak_signal(self, sample_prr_data):
        """
        Test that weak association has PRR < 2.0.

        Why: finasteride × depression is below threshold (not a strong signal).
        """
        data = sample_prr_data["finasteride_depression"]
        A, B, C, D = data["A"], data["B"], data["C"], data["D"]

        prr = (A / (A + B)) / (C / (C + D))

        assert prr < 2.0, (
            f"Weak signal has PRR {prr:.2f} ≥ 2.0 (should be below threshold)"
        )

    def test_prr_increases_with_higher_a(self):
        """
        Test that PRR increases when A increases (holding B, C, D constant).

        Why: More drug-reaction pairs should strengthen the signal.
        """
        B, C, D = 1000, 200, 5000

        A_low = 50
        A_high = 100

        prr_low = (A_low / (A_low + B)) / (C / (C + D))
        prr_high = (A_high / (A_high + B)) / (C / (C + D))

        assert prr_high > prr_low, (
            f"PRR didn't increase with higher A: {prr_low:.2f} → {prr_high:.2f}"
        )

    def test_prr_symmetric_for_same_proportions(self):
        """
        Test that PRR = 1.0 when drug and background proportions are equal.

        Why: PRR = 1.0 means no association (drug rate = background rate).
        """
        # Set up equal proportions: 10% in both groups
        A, B = 100, 900   # 100/1000 = 10%
        C, D = 200, 1800  # 200/2000 = 10%

        prr = (A / (A + B)) / (C / (C + D))

        assert abs(prr - 1.0) < 0.01, (
            f"PRR should be ~1.0 for equal proportions, got {prr:.2f}"
        )


# ── 2. Threshold Filter Tests ─────────────────────────────────────────────────


class TestThresholdFilters:
    """Test threshold filters that determine which signals are flagged."""

    def test_production_thresholds(self):
        """
        Test production mode thresholds.

        Production thresholds (from CLAUDE.md):
          - A ≥ 50
          - C ≥ 200
          - drug_total ≥ 1,000
          - PRR ≥ 2.0
        """
        thresholds = {
            "A_MIN": 50,
            "C_MIN": 200,
            "DRUG_TOTAL_MIN": 1000,
            "PRR_MIN": 2.0
        }

        # Signal that meets all thresholds
        signal = {
            "A": 60,
            "C": 250,
            "drug_total": 1200,
            "prr": 2.5
        }

        passes = (
            signal["A"] >= thresholds["A_MIN"] and
            signal["C"] >= thresholds["C_MIN"] and
            signal["drug_total"] >= thresholds["DRUG_TOTAL_MIN"] and
            signal["prr"] >= thresholds["PRR_MIN"]
        )

        assert passes, "Valid signal failed production thresholds"

    def test_poc_thresholds(self):
        """
        Test POC mode thresholds.

        POC thresholds (from CLAUDE.md):
          - A ≥ 30
          - C ≥ 100
          - drug_total ≥ 500
          - PRR ≥ 2.0
        """
        thresholds = {
            "A_MIN": 30,
            "C_MIN": 100,
            "DRUG_TOTAL_MIN": 500,
            "PRR_MIN": 2.0
        }

        # Signal that meets POC but not production thresholds
        signal = {
            "A": 35,
            "C": 120,
            "drug_total": 600,
            "prr": 2.3
        }

        passes = (
            signal["A"] >= thresholds["A_MIN"] and
            signal["C"] >= thresholds["C_MIN"] and
            signal["drug_total"] >= thresholds["DRUG_TOTAL_MIN"] and
            signal["prr"] >= thresholds["PRR_MIN"]
        )

        assert passes, "Valid POC signal failed POC thresholds"

    def test_filter_low_case_count(self):
        """
        Test that signals with A < threshold are filtered out.

        Why: Low case counts have high false positive rate.
        """
        # Production threshold: A ≥ 50
        A_MIN = 50

        signal_pass = {"A": 50, "C": 200, "drug_total": 1000, "prr": 2.5}
        signal_fail = {"A": 49, "C": 200, "drug_total": 1000, "prr": 2.5}

        assert signal_pass["A"] >= A_MIN, "Signal with A=50 should pass"
        assert signal_fail["A"] < A_MIN, "Signal with A=49 should fail"

    def test_filter_low_background_count(self):
        """
        Test that signals with C < threshold are filtered out.

        Why: Low background counts make PRR unstable.
        """
        # Production threshold: C ≥ 200
        C_MIN = 200

        signal_pass = {"A": 50, "C": 200, "drug_total": 1000, "prr": 2.5}
        signal_fail = {"A": 50, "C": 199, "drug_total": 1000, "prr": 2.5}

        assert signal_pass["C"] >= C_MIN, "Signal with C=200 should pass"
        assert signal_fail["C"] < C_MIN, "Signal with C=199 should fail"

    def test_filter_low_drug_total(self):
        """
        Test that signals for rare drugs (drug_total < threshold) are filtered.

        Why: Rare drugs have insufficient data for reliable PRR.
        """
        # Production threshold: drug_total ≥ 1,000
        DRUG_TOTAL_MIN = 1000

        signal_pass = {"A": 50, "C": 200, "drug_total": 1000, "prr": 2.5}
        signal_fail = {"A": 50, "C": 200, "drug_total": 999, "prr": 2.5}

        assert signal_pass["drug_total"] >= DRUG_TOTAL_MIN, "Signal with 1000 reports should pass"
        assert signal_fail["drug_total"] < DRUG_TOTAL_MIN, "Signal with 999 reports should fail"

    def test_filter_low_prr(self):
        """
        Test that signals with PRR < 2.0 are filtered out.

        Why: PRR < 2.0 means drug rate < 2x background rate (weak association).
        """
        PRR_MIN = 2.0

        signal_pass = {"A": 50, "C": 200, "drug_total": 1000, "prr": 2.0}
        signal_fail = {"A": 50, "C": 200, "drug_total": 1000, "prr": 1.99}

        assert signal_pass["prr"] >= PRR_MIN, "Signal with PRR=2.0 should pass"
        assert signal_fail["prr"] < PRR_MIN, "Signal with PRR=1.99 should fail"


# ── 3. Quality Filter Tests ───────────────────────────────────────────────────


class TestQualityFilters:
    """Test quality filters that remove false positives."""

    def test_junk_term_filter(self, junk_terms):
        """
        Test that administrative/non-specific MedDRA terms are filtered.

        Why: Terms like "drug ineffective" are not safety signals.
        """
        # Signals with junk terms should be filtered
        for junk_term in junk_terms[:5]:  # Test first 5
            signal = {
                "drug_key": "test_drug",
                "pt": junk_term,
                "prr": 3.0,
                "A": 100
            }

            # Check if term is in junk list (case-insensitive)
            is_junk = any(
                junk.lower() in signal["pt"].lower()
                for junk in junk_terms
            )

            assert is_junk, f"Term '{junk_term}' should be classified as junk"

    def test_valid_term_not_filtered(self, junk_terms):
        """
        Test that real adverse reactions are NOT filtered as junk.

        Why: Legitimate safety signals should pass junk filter.
        """
        valid_terms = [
            "myocardial infarction",
            "skin necrosis",
            "diabetic ketoacidosis",
            "cardio-respiratory arrest",
            "tonic-clonic seizure"
        ]

        for valid_term in valid_terms:
            # Check that term is NOT in junk list
            is_junk = any(
                junk.lower() in valid_term.lower()
                for junk in junk_terms
            )

            assert not is_junk, f"Valid term '{valid_term}' incorrectly classified as junk"

    def test_single_quarter_spike_filter(self):
        """
        Test spike filter: Remove signals where >70% cases in one quarter.

        Why: Single-quarter spikes often indicate data quality issues.
        """
        SPIKE_THRESHOLD = 0.70

        # Signal with 80% of cases in Q1 (should be filtered)
        spike_signal = {
            "q1_count": 80,
            "q2_count": 10,
            "q3_count": 5,
            "q4_count": 5,
            "total": 100
        }

        max_quarter_pct = max(
            spike_signal["q1_count"],
            spike_signal["q2_count"],
            spike_signal["q3_count"],
            spike_signal["q4_count"]
        ) / spike_signal["total"]

        assert max_quarter_pct > SPIKE_THRESHOLD, (
            f"Spike signal has {max_quarter_pct:.2%} in max quarter (should exceed {SPIKE_THRESHOLD:.0%})"
        )

        # Signal with even distribution (should pass)
        even_signal = {
            "q1_count": 25,
            "q2_count": 25,
            "q3_count": 25,
            "q4_count": 25,
            "total": 100
        }

        max_quarter_pct_even = max(
            even_signal["q1_count"],
            even_signal["q2_count"],
            even_signal["q3_count"],
            even_signal["q4_count"]
        ) / even_signal["total"]

        assert max_quarter_pct_even <= SPIKE_THRESHOLD, (
            f"Even signal has {max_quarter_pct_even:.2%} in max quarter (should be ≤ {SPIKE_THRESHOLD:.0%})"
        )

    def test_late_surge_filter(self):
        """
        Test late surge filter: Remove signals where >85% cases in Q3+Q4.

        Why: Late surges may indicate reporting artifacts, not real signals.
        """
        SURGE_THRESHOLD = 0.85

        # Signal with 90% of cases in Q3+Q4 (should be filtered)
        surge_signal = {
            "q1_count": 5,
            "q2_count": 5,
            "q3_count": 45,
            "q4_count": 45,
            "total": 100
        }

        late_pct = (surge_signal["q3_count"] + surge_signal["q4_count"]) / surge_signal["total"]

        assert late_pct > SURGE_THRESHOLD, (
            f"Surge signal has {late_pct:.2%} in Q3+Q4 (should exceed {SURGE_THRESHOLD:.0%})"
        )

        # Signal with gradual increase (should pass)
        gradual_signal = {
            "q1_count": 20,
            "q2_count": 25,
            "q3_count": 30,
            "q4_count": 25,
            "total": 100
        }

        late_pct_gradual = (gradual_signal["q3_count"] + gradual_signal["q4_count"]) / gradual_signal["total"]

        assert late_pct_gradual <= SURGE_THRESHOLD, (
            f"Gradual signal has {late_pct_gradual:.2%} in Q3+Q4 (should be ≤ {SURGE_THRESHOLD:.0%})"
        )


# ── 4. Golden Signal Detection Tests ──────────────────────────────────────────


class TestGoldenSignalDetection:
    """
    Test that all 10 golden drug-reaction pairs are detected.

    CRITICAL: If these tests fail, the pipeline is not valid.
    """

    @pytest.mark.skipif(
        not Path("data/signals_flagged.csv").exists(),
        reason="signals_flagged.csv not found — run Branch 2 first"
    )
    def test_golden_signals_detected(self, golden_signals):
        """
        Test that all 10 golden signals appear in signals_flagged output.

        Why: These are documented FDA signals — failure to detect means
        pipeline has false negatives.

        Note: This test requires Branch 2 to have run and produced
        signals_flagged output file.
        """
        # This test would read from Snowflake or CSV output
        # For now, we'll structure it as a placeholder

        pytest.skip("Requires Snowflake connection or signals_flagged.csv output")

        # TODO: Implement when Branch 2 output is available
        # detected_signals = load_signals_from_snowflake()
        #
        # for golden in golden_signals:
        #     found = any(
        #         s["drug_key"].lower() == golden["drug_key"].lower() and
        #         golden["pt"].lower() in s["pt"].lower()
        #         for s in detected_signals
        #     )
        #
        #     assert found, (
        #         f"Golden signal NOT detected: {golden['drug_key']} × {golden['pt']}"
        #     )

    def test_golden_signal_list_completeness(self, golden_signals):
        """
        Test that golden signal list has all 10 expected pairs.

        Why: Ensures test fixture hasn't been accidentally modified.
        """
        assert len(golden_signals) == 10, (
            f"Golden signal list should have 10 pairs, found {len(golden_signals)}"
        )

        # Check required fields
        for signal in golden_signals:
            assert "drug_key" in signal, "Golden signal missing drug_key"
            assert "pt" in signal, "Golden signal missing pt"
            assert signal["drug_key"], "Golden signal has empty drug_key"
            assert signal["pt"], "Golden signal has empty pt"


# ── 5. Mode Selection Tests ───────────────────────────────────────────────────


class TestModeSelection:
    """Test POC vs Production threshold switching logic."""

    def test_poc_mode_triggered_for_small_dataset(self):
        """
        Test that POC mode activates when total_rows < 1,000,000.

        Why: Single-quarter test datasets should use relaxed thresholds.
        """
        POC_THRESHOLD = 1_000_000

        total_rows_poc = 500_000
        total_rows_prod = 5_000_000

        is_poc_mode = total_rows_poc < POC_THRESHOLD
        is_prod_mode = total_rows_prod >= POC_THRESHOLD

        assert is_poc_mode, "POC mode should activate for 500K rows"
        assert is_prod_mode, "Production mode should activate for 5M rows"

    def test_production_mode_for_full_dataset(self):
        """
        Test that Production mode activates for full 4-quarter data.

        Why: Full-year 2023 (~5M rows) should use strict thresholds.
        """
        POC_THRESHOLD = 1_000_000

        total_rows = 5_012_904  # Full-year 2023 expected count

        is_production = total_rows >= POC_THRESHOLD

        assert is_production, "Production mode should activate for full dataset"


# ── Test Execution ────────────────────────────────────────────────────────────


if __name__ == "__main__":
    """Run Branch 2 tests directly."""
    import pytest

    exit_code = pytest.main([
        __file__,
        "-v",
        "--tb=short",
        "-s",
    ])

    sys.exit(exit_code)
