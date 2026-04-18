"""
test_evaluation_integration.py — Integration tests for evaluation endpoints.

Requires real Snowflake connection and populated data:
    - drug_reaction_pairs (Branch 1 output)
    - signals_flagged (Branch 2 output)

Run:
    poetry run pytest tests/integration/test_evaluation_integration.py -v
"""

import sys
import os
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest
from fastapi.testclient import TestClient

from app.routers.evaluation import GOLDEN_SIGNALS, PRR_THRESHOLD, MIN_CASES
from main import app

client = TestClient(app)

# Confirmed flagged in Q1 2023 signals_flagged with corrected PT values
CONFIRMED_FLAGGED_Q1 = {
    ("dupilumab",     "conjunctivitis"),
    ("levetiracetam", "seizure"),
    ("tirzepatide",   "injection site pain"),
    ("semaglutide",   "increased appetite"),
    ("empagliflozin", "diabetic ketoacidosis"),
    ("metformin",     "lactic acidosis"),
    ("gabapentin",    "completed suicide"),
    ("pregabalin",    "drug abuse"),
    ("bupropion",     "completed suicide"),
    ("dapagliflozin", "death"),
}


class TestPrecisionRecallEndpoint:
    """
    Tests GET /evaluation/precision-recall against real Snowflake data.
    """

    def test_endpoint_returns_200(self):
        response = client.get("/evaluation/precision-recall")
        assert response.status_code == 200

    def test_response_has_required_fields(self):
        data     = client.get("/evaluation/precision-recall").json()
        required = {"total_golden", "flagged", "not_flagged", "precision",
                    "prr_threshold", "min_cases", "breakdown"}
        assert required.issubset(data.keys())

    def test_total_golden_always_10(self):
        data = client.get("/evaluation/precision-recall").json()
        assert data["total_golden"] == 10

    def test_flagged_plus_not_flagged_equals_total(self):
        data = client.get("/evaluation/precision-recall").json()
        assert data["flagged"] + data["not_flagged"] == data["total_golden"]

    def test_precision_in_valid_range(self):
        data = client.get("/evaluation/precision-recall").json()
        assert 0.0 <= data["precision"] <= 1.0

    def test_breakdown_has_10_entries(self):
        data = client.get("/evaluation/precision-recall").json()
        assert len(data["breakdown"]) == 10

    def test_flagged_signals_have_prr(self):
        """Flagged signals must have non-null PRR and case count."""
        data = client.get("/evaluation/precision-recall").json()
        for row in data["breakdown"]:
            if row["flagged"]:
                assert row["prr"] is not None, (
                    f"{row['drug_key']} flagged=True but prr is None"
                )
                assert row["drug_reaction_count"] is not None

    def test_unflagged_signals_have_null_prr(self):
        """Unflagged signals must have null PRR."""
        data = client.get("/evaluation/precision-recall").json()
        for row in data["breakdown"]:
            if not row["flagged"]:
                assert row["prr"] is None, (
                    f"{row['drug_key']} flagged=False but prr={row['prr']}"
                )

    def test_precision_matches_flagged_count(self):
        data     = client.get("/evaluation/precision-recall").json()
        expected = round(data["flagged"] / data["total_golden"], 3)
        assert data["precision"] == expected

    def test_confirmed_flagged_signals_present(self):
        """
        All 10 golden signals should now be flagged with corrected PT values.
        Verified against actual signals_flagged Q1 data.
        """
        data    = client.get("/evaluation/precision-recall").json()
        flagged = {(r["drug_key"], r["pt"]) for r in data["breakdown"] if r["flagged"]}
        for pair in CONFIRMED_FLAGGED_Q1:
            assert pair in flagged, (
                f"{pair[0]} x {pair[1]} expected flagged but not found"
            )

    def test_precision_equals_1_after_pt_fix(self):
        """
        With corrected PT values all 10 golden signals should be flagged.
        precision = 10/10 = 1.0
        """
        data = client.get("/evaluation/precision-recall").json()
        assert data["flagged"] == 10, (
            f"Expected 10 flagged signals, got {data['flagged']}. "
            f"Check PT values in GOLDEN_SIGNALS match signals_flagged."
        )
        assert data["precision"] == 1.0

    def test_thresholds_in_response(self):
        data = client.get("/evaluation/precision-recall").json()
        assert data["prr_threshold"] == PRR_THRESHOLD
        assert data["min_cases"]     == MIN_CASES

    def test_stat_score_in_valid_range(self):
        """Flagged signals must have stat_score in [0, 1]."""
        data = client.get("/evaluation/precision-recall").json()
        for row in data["breakdown"]:
            if row["flagged"] and row["stat_score"] is not None:
                assert 0.0 <= row["stat_score"] <= 1.0, (
                    f"{row['drug_key']}: stat_score={row['stat_score']} out of range"
                )

    def test_empagliflozin_high_prr(self):
        """empagliflozin + diabetic ketoacidosis has PRR ~30 — strong signal."""
        data = client.get("/evaluation/precision-recall").json()
        row  = next(
            (r for r in data["breakdown"] if r["drug_key"] == "empagliflozin"),
            None
        )
        assert row is not None
        assert row["flagged"]
        assert row["prr"] > 10, (
            f"empagliflozin PRR={row['prr']} expected > 10"
        )

    def test_metformin_high_prr(self):
        """metformin + lactic acidosis has PRR ~73 — very strong signal."""
        data = client.get("/evaluation/precision-recall").json()
        row  = next(
            (r for r in data["breakdown"] if r["drug_key"] == "metformin"),
            None
        )
        assert row is not None
        assert row["flagged"]
        assert row["prr"] > 20, (
            f"metformin PRR={row['prr']} expected > 20"
        )


class TestLeadTimesEndpoint:
    """
    Tests GET /evaluation/lead-times against real Snowflake data.
    """

    def test_endpoint_returns_200(self):
        response = client.get("/evaluation/lead-times")
        assert response.status_code == 200

    def test_response_has_required_fields(self):
        data     = client.get("/evaluation/lead-times").json()
        required = {"results", "median_lead_time", "positive_detections",
                    "total_golden", "flagged_count", "prr_threshold", "min_cases"}
        assert required.issubset(data.keys())

    def test_results_has_10_entries(self):
        data = client.get("/evaluation/lead-times").json()
        assert len(data["results"]) == 10

    def test_each_result_has_required_fields(self):
        data     = client.get("/evaluation/lead-times").json()
        required = {"drug_key", "pt", "fda_comm_date", "fda_comm_label",
                    "first_flagged_date", "lead_time_days", "flagged"}
        for r in data["results"]:
            missing = required - set(r.keys())
            assert not missing, f"{r['drug_key']} missing fields: {missing}"

    def test_flagged_signals_have_lead_time(self):
        data = client.get("/evaluation/lead-times").json()
        for r in data["results"]:
            if r["flagged"]:
                assert r["lead_time_days"] is not None, (
                    f"{r['drug_key']} flagged=True but lead_time_days is None"
                )
                assert r["first_flagged_date"] is not None

    def test_positive_detections_lte_flagged_count(self):
        data = client.get("/evaluation/lead-times").json()
        assert data["positive_detections"] <= data["flagged_count"]

    def test_median_lead_time_positive(self):
        """MedSignal should detect golden signals before FDA communicates them."""
        data = client.get("/evaluation/lead-times").json()
        if data["median_lead_time"] is not None:
            assert data["median_lead_time"] > 0, (
                f"Median lead time {data['median_lead_time']} is not positive"
            )

    def test_fda_comm_dates_are_iso_strings(self):
        data = client.get("/evaluation/lead-times").json()
        for r in data["results"]:
            assert isinstance(r["fda_comm_date"], str)
            date.fromisoformat(r["fda_comm_date"])

    def test_flagged_count_matches_precision_recall(self):
        """flagged_count in lead-times must match flagged in precision-recall."""
        lead_times = client.get("/evaluation/lead-times").json()
        pr         = client.get("/evaluation/precision-recall").json()
        assert lead_times["flagged_count"] == pr["flagged"]

    def test_all_flagged_signals_have_positive_lead_time(self):
        """
        With Q1 2023 data all golden signals should have been detectable
        before their FDA communication dates (all comms are mid-late 2023).
        """
        data = client.get("/evaluation/lead-times").json()
        for r in data["results"]:
            if r["flagged"] and r["lead_time_days"] is not None:
                assert r["lead_time_days"] > 0, (
                    f"{r['drug_key']}: lead_time_days={r['lead_time_days']} "
                    f"— signal detected after FDA communication"
                )


class TestSummaryEndpoint:
    """
    Tests GET /evaluation/summary against real Snowflake data.
    """

    def test_endpoint_returns_200(self):
        response = client.get("/evaluation/summary")
        assert response.status_code == 200

    def test_response_has_required_fields(self):
        data     = client.get("/evaluation/summary").json()
        required = {"total_golden", "flagged", "not_flagged", "precision",
                    "median_lead_time", "positive_detections",
                    "prr_threshold", "min_cases"}
        assert required.issubset(data.keys())

    def test_total_golden_always_10(self):
        data = client.get("/evaluation/summary").json()
        assert data["total_golden"] == 10

    def test_consistent_with_precision_recall(self):
        """Summary flagged/precision must match precision-recall endpoint."""
        summary = client.get("/evaluation/summary").json()
        pr      = client.get("/evaluation/precision-recall").json()
        assert summary["flagged"]     == pr["flagged"]
        assert summary["precision"]   == pr["precision"]
        assert summary["not_flagged"] == pr["not_flagged"]

    def test_consistent_with_lead_times(self):
        """Summary median/positive_detections must match lead-times endpoint."""
        summary    = client.get("/evaluation/summary").json()
        lead_times = client.get("/evaluation/lead-times").json()
        assert summary["median_lead_time"]    == lead_times["median_lead_time"]
        assert summary["positive_detections"] == lead_times["positive_detections"]

    def test_precision_equals_1_after_pt_fix(self):
        """With corrected PT values precision should be 1.0."""
        data = client.get("/evaluation/summary").json()
        assert data["precision"] == 1.0