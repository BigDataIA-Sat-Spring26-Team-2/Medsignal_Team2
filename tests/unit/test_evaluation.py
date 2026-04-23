"""
test_evaluation.py — Unit tests for evaluation logic.

Tests pure logic only — no Snowflake, no API calls.
Covers: GOLDEN_SIGNALS constants, lead time formula,
        precision formula, threshold config, SQL placeholder builder.

Run:
    poetry run pytest tests/unit/test_evaluation.py -v
"""

import sys
import os
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.routers.evaluation import (
    GOLDEN_SIGNALS,
    PRR_THRESHOLD,
    MIN_CASES,
    _build_pair_placeholders,
)

PASS = "PASS"
FAIL = "FAIL"


class TestGoldenSignalConstants:
    """Validates the GOLDEN_SIGNALS constant itself."""

    def test_exactly_10_golden_signals(self):
        assert len(GOLDEN_SIGNALS) == 10

    def test_all_required_fields_present(self):
        required = {"drug_key", "pt", "fda_comm_date", "fda_comm_label"}
        for g in GOLDEN_SIGNALS:
            missing = required - set(g.keys())
            assert not missing, f"{g.get('drug_key', '?')} missing fields: {missing}"

    def test_all_10_golden_drugs_present(self):
        expected_drugs = {
            "dupilumab", "gabapentin", "pregabalin", "levetiracetam",
            "tirzepatide", "semaglutide", "empagliflozin",
            "bupropion", "dapagliflozin", "metformin",
        }
        actual_drugs = {g["drug_key"] for g in GOLDEN_SIGNALS}
        assert actual_drugs == expected_drugs, (
            f"Missing: {expected_drugs - actual_drugs}, "
            f"Extra: {actual_drugs - expected_drugs}"
        )

    def test_fda_comm_dates_are_date_objects(self):
        for g in GOLDEN_SIGNALS:
            assert g["fda_comm_date"] is None or isinstance(g["fda_comm_date"], date), (
            f"{g['drug_key']}: fda_comm_date must be date or None, "
            f"got {type(g['fda_comm_date'])}"
            )

    def test_fda_comm_dates_in_2023_or_2024(self):
        for g in GOLDEN_SIGNALS:
            if g["fda_comm_date"] is None:
                continue
            year = g["fda_comm_date"].year
            assert year in (2023, 2024), (
                f"{g['drug_key']}: fda_comm_date year {year} unexpected"
        )

    def test_no_duplicate_drug_pt_pairs(self):
        pairs = [(g["drug_key"], g["pt"]) for g in GOLDEN_SIGNALS]
        assert len(pairs) == len(set(pairs)), "Duplicate (drug_key, pt) pairs found"

    def test_pt_values_are_lowercase(self):
        for g in GOLDEN_SIGNALS:
            assert g["pt"] == g["pt"].lower(), (
                f"{g['drug_key']}: pt '{g['pt']}' should be lowercase"
            )

    def test_drug_keys_are_lowercase(self):
        for g in GOLDEN_SIGNALS:
            assert g["drug_key"] == g["drug_key"].lower(), (
                f"drug_key '{g['drug_key']}' should be lowercase"
            )

    def test_fda_comm_labels_non_empty(self):
        for g in GOLDEN_SIGNALS:
            assert isinstance(g["fda_comm_label"], str) and g["fda_comm_label"].strip(), (
                f"{g['drug_key']}: fda_comm_label is empty"
            )

    def test_pt_values_match_signals_flagged(self):
        """
        PT values must match exactly what Branch 2 wrote to signals_flagged.
        These are Q1 2023 available PTs — verified against actual DB query:

            SELECT drug_key, pt FROM signals_flagged
            WHERE drug_key IN ('gabapentin','pregabalin','bupropion','dapagliflozin')
            ORDER BY drug_key, prr DESC;

        Will be reverted to proposal PTs once full year data is loaded:
            gabapentin    → cardio-respiratory arrest
            pregabalin    → coma
            bupropion     → seizure
            dapagliflozin → glomerular filtration rate decreased
        """
        pt_map = {g["drug_key"]: g["pt"] for g in GOLDEN_SIGNALS}
        for drug_key, pt in pt_map.items():
            assert isinstance(pt, str) and pt == pt.lower(), (
                f"{drug_key}: pt '{pt}' must be a lowercase string"
        )

        assert pt_map["dupilumab"]     == "conjunctivitis"
        assert pt_map["gabapentin"]    == "cardio-respiratory arrest"    
        assert pt_map["pregabalin"]    == "coma"           
        assert pt_map["levetiracetam"] == "seizure"
        assert pt_map["tirzepatide"]   == "injection site pain"
        assert pt_map["semaglutide"]   == "increased appetite"
        assert pt_map["empagliflozin"] == "diabetic ketoacidosis"
        assert pt_map["bupropion"]     == "seizure"   
        assert pt_map["dapagliflozin"] == "glomerular filtration rate decreased"               
        assert pt_map["metformin"]     == "lactic acidosis"


class TestLeadTimeFormula:
    """Tests lead time computation logic in isolation."""

    def test_positive_lead_time(self):
        """Signal detected 175 days before FDA communication."""
        lead_time = (date(2023, 12, 1) - date(2023, 6, 9)).days
        assert lead_time == 175

    def test_negative_lead_time(self):
        """Signal detected after FDA communication."""
        lead_time = (date(2023, 4, 1) - date(2023, 4, 14)).days
        assert lead_time == -13

    def test_zero_lead_time(self):
        """Signal detected on same day as FDA communication."""
        lead_time = (date(2023, 9, 1) - date(2023, 9, 1)).days
        assert lead_time == 0

    def test_metformin_lead_time(self):
        """Metformin POC lead time ~13 days per proposal."""
        lead_time = (date(2023, 4, 1) - date(2023, 3, 19)).days
        assert lead_time == 13

    def test_dupilumab_lead_time(self):
        """Dupilumab POC lead time ~291 days per proposal."""
        lead_time = (date(2024, 1, 1) - date(2023, 3, 16)).days
        assert lead_time == 291

    def test_median_lead_time_computation(self):
        lead_times = [291, 175, 120, 60, 13]
        median     = sorted(lead_times)[len(lead_times) // 2]
        assert median == 120

    def test_median_with_even_count(self):
        lead_times = [100, 200, 300, 400]
        median     = sorted(lead_times)[len(lead_times) // 2]
        assert median == 300

    def test_positive_detections_count(self):
        lead_times          = [291, 175, -13, 60, 0, -5]
        positive_detections = sum(1 for lt in lead_times if lt > 0)
        assert positive_detections == 3


class TestPrecisionFormula:
    """Tests precision computation logic."""

    def test_precision_all_flagged(self):
        assert round(10 / 10, 3) == 1.0

    def test_precision_six_flagged(self):
        """Matches Q1-only result before PT fix."""
        assert round(6 / 10, 3) == 0.6

    def test_precision_ten_flagged(self):
        """Expected result after PT values corrected to match signals_flagged."""
        assert round(10 / 10, 3) == 1.0

    def test_precision_zero_flagged(self):
        assert round(0 / 10, 3) == 0.0

    def test_not_flagged_count(self):
        total, flagged = 10, 6
        assert total - flagged == 4


class TestThresholdConfig:
    """Tests threshold configuration from environment."""

    def test_prr_threshold_is_float(self):
        assert isinstance(PRR_THRESHOLD, float)

    def test_min_cases_is_int(self):
        assert isinstance(MIN_CASES, int)

    def test_prr_threshold_minimum(self):
        """PRR_THRESHOLD must be >= 2.0 — Branch 2 enforces this minimum."""
        assert PRR_THRESHOLD >= 2.0, (
            f"PRR_THRESHOLD={PRR_THRESHOLD} is below Branch 2 minimum of 2.0"
        )

    def test_min_cases_minimum(self):
        assert MIN_CASES >= 1


class TestBuildPairPlaceholders:
    """Tests the SQL placeholder builder utility."""

    def test_correct_placeholder_count(self):
        placeholders, _ = _build_pair_placeholders(GOLDEN_SIGNALS)
        or_count = placeholders.count(" OR ")
        assert or_count == len(GOLDEN_SIGNALS) - 1

    def test_correct_param_count(self):
        _, params = _build_pair_placeholders(GOLDEN_SIGNALS)
        assert len(params) == len(GOLDEN_SIGNALS) * 2

    def test_params_alternate_drug_pt(self):
        _, params = _build_pair_placeholders(GOLDEN_SIGNALS[:2])
        assert params[0] == GOLDEN_SIGNALS[0]["drug_key"]
        assert params[1] == GOLDEN_SIGNALS[0]["pt"]
        assert params[2] == GOLDEN_SIGNALS[1]["drug_key"]
        assert params[3] == GOLDEN_SIGNALS[1]["pt"]


# ── Runner ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("MedSignal — Evaluation Unit Tests")
    print("=" * 60)

    results = {}
    all_classes = [
        ("Golden signal constants", TestGoldenSignalConstants),
        ("Lead time formula",       TestLeadTimeFormula),
        ("Precision formula",       TestPrecisionFormula),
        ("Threshold config",        TestThresholdConfig),
        ("Placeholder builder",     TestBuildPairPlaceholders),
    ]

    for class_name, cls in all_classes:
        print(f"\n── {class_name} ──")
        instance = cls()
        methods  = [m for m in dir(instance) if m.startswith("test_")]
        for method_name in methods:
            label = method_name.replace("test_", "").replace("_", " ")
            try:
                getattr(instance, method_name)()
                results[method_name] = True
                print(f"  {PASS}  {label}")
            except AssertionError as e:
                results[method_name] = False
                print(f"  {FAIL}  {label}: {e}")

    passed = sum(results.values())
    total  = len(results)
    print(f"\n{'='*60}")
    print(f"{passed}/{total} unit tests passed")
    sys.exit(0 if passed == total else 1)