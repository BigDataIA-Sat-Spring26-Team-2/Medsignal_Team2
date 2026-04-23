"""
evaluation.py — FastAPI router for evaluation endpoints.

Endpoints:
    GET /evaluation/lead-times       — detection lead time per golden signal
    GET /evaluation/precision-recall — how many golden signals were correctly flagged
    GET /evaluation/summary          — single endpoint for dashboard header metrics

Data sources:
    drug_reaction_pairs — MIN(fda_dt) per (drug_key, pt) = first flagged date
    signals_flagged     — confirms signal cleared PRR threshold and min cases

Golden signal set:
    10 drug-reaction pairs with documented FDA safety communications in 2023.
    FDA communication dates are hardcoded constants from the proposal table.
    These never change — they are historical FDA records.

Lead time formula:
    lead_time_days = fda_comm_date - first_flagged_date
    Positive = MedSignal detected before FDA communicated (good)
    Negative = MedSignal detected after FDA communicated (signal was late)

Precision formula:
    precision = flagged_golden / total_golden  (10 golden signals)
    A signal is "flagged" if it appears in signals_flagged with
    PRR >= EVAL_PRR_THRESHOLD AND drug_reaction_count >= EVAL_MIN_CASES

Configurable thresholds (.env):
    EVAL_PRR_THRESHOLD  — minimum PRR to count as flagged (default 2.0)
    EVAL_MIN_CASES      — minimum case count to count as flagged (default 50)
"""

import logging
import os
from datetime import date

from dotenv import load_dotenv
from fastapi import APIRouter
from app.utils.snowflake_client import get_conn

load_dotenv()

log    = logging.getLogger(__name__)
router = APIRouter(prefix="/evaluation", tags=["evaluation"])

# ── Configurable thresholds ───────────────────────────────────────────────────
# Set in .env — no code change needed to tune for demo vs development.
# PRR_THRESHOLD below 2.0 has no effect — signals_flagged already enforces 2.0.
# MIN_CASES below 50 has no effect — signals_flagged already enforces A >= 50.

PRR_THRESHOLD = float(os.getenv("EVAL_PRR_THRESHOLD", "2.0"))
MIN_CASES     = int(os.getenv("EVAL_MIN_CASES", "50"))


# ── Golden signal constants ───────────────────────────────────────────────────
# Source: proposal Table — Golden Signal Validation Set (p30-31)
# FDA communication dates are hardcoded — these are historical records.
# PT values must match exactly what Branch 2 wrote to signals_flagged.
# Verify with:
#   SELECT drug_key, pt FROM signals_flagged
#   WHERE drug_key IN ('dupilumab','gabapentin',...)
#   ORDER BY drug_key;

GOLDEN_SIGNALS = [
    {
        "drug_key"      : "dupilumab",
        "pt"            : "conjunctivitis",
        "fda_comm_date" : date(2024, 1, 15),
        "fda_comm_label": "FDA Label Update — January 2024",
    },
    {
        "drug_key"      : "gabapentin",
        "pt"            : "cardio-respiratory arrest",       
        "fda_comm_date" : date(2023, 4, 15),
        "fda_comm_label": "FDA Drug Safety Communication — December 2023",
    },
    {
        "drug_key"      : "pregabalin",
        "pt"            : "coma",               
        "fda_comm_date" : date(2023, 12, 15),
        "fda_comm_label": "FDA Drug Safety Communication — December 2023",
    },
    {
        "drug_key"      : "levetiracetam",
        "pt"            : "seizure",
        "fda_comm_date" : date(2023, 11, 28),
        "fda_comm_label": "FDA Safety Communication — November 2023",
    },
    {
        "drug_key"      : "tirzepatide",
        "pt"            : "injection site pain",
        "fda_comm_date" : date(2023, 4, 15),
        "fda_comm_label": "FDA Drug Safety Communication — September 2023",
    },
    {
        "drug_key"      : "semaglutide",
        "pt"            : "increased appetite",
        "fda_comm_date" : date(2023, 9, 2),
        "fda_comm_label": "FDA Drug Safety Communication — September 2023",
    },
    {
        "drug_key"      : "empagliflozin",
        "pt"            : "diabetic ketoacidosis",
        "fda_comm_date" : date(2023, 9, 15),
        "fda_comm_label": "FDA Drug Safety Communication — August 2023",
    },
    {
        "drug_key"      : "bupropion",
        "pt"            : "seizure",        
        "fda_comm_date" : date(2023, 5, 11),
        "fda_comm_label": "FDA Drug Safety Communication — May 2023",
    },
    {
        "drug_key"      : "dapagliflozin",
        "pt"            : "glomerular filtration rate decreased",                   
        "fda_comm_date" : date(2023, 5, 15),
        "fda_comm_label": "FDA Label Update — May 2023",
    },
    {
        "drug_key"      : "metformin",
        "pt"            : "lactic acidosis",
        "fda_comm_date" : date(2023, 4, 15),
        "fda_comm_label": "FDA Drug Safety Communication — April 2023",
    },
]


# ── Shared helpers ────────────────────────────────────────────────────────────

def _build_pair_placeholders(signals: list) -> tuple[str, list]:
    """
    Build a parameterised OR clause and params list for (drug_key, pt) pairs.
    Returns (placeholders_str, flat_params_list).
    """
    placeholders = " OR ".join(
        ["(drug_key = %s AND pt = %s)"] * len(signals)
    )
    params = []
    for g in signals:
        params.extend([g["drug_key"], g["pt"]])
    return placeholders, params


def _get_flagged_set() -> set:
    """
    Returns set of (drug_key, pt) tuples that are in signals_flagged
    and meet both EVAL_PRR_THRESHOLD and EVAL_MIN_CASES.
    Used by both lead-times and precision-recall endpoints.
    """
    placeholders, params = _build_pair_placeholders(GOLDEN_SIGNALS)

    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(
        f"""
        SELECT drug_key, pt
        FROM signals_flagged
        WHERE ({placeholders})
        AND prr >= %s
        AND drug_reaction_count >= %s
        """,
        params + [PRR_THRESHOLD, MIN_CASES],
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {(row[0], row[1]) for row in rows}


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/lead-times")
def get_lead_times():
    """
    GET /evaluation/lead-times

    For each of the 10 golden signals:
        - Looks up MIN(fda_dt) from drug_reaction_pairs as first_flagged_date
        - Checks signals_flagged to confirm signal cleared PRR + min cases
        - Computes lead_time_days = fda_comm_date - first_flagged_date

    Returns per-signal results plus summary stats:
        median_lead_time    — median days across signals with a lead time
        positive_detections — count of signals detected before FDA comm date
        flagged_count       — count of golden signals in signals_flagged

    Used by: Evaluation Dashboard bar chart + headline metric cards
    """
    # Query 1 — first flagged date per golden pair from drug_reaction_pairs
    drp_placeholders, drp_params = _build_pair_placeholders(GOLDEN_SIGNALS)
    # Rebuild with table alias prefix for drug_reaction_pairs
    drp_placeholders_aliased = " OR ".join(
        ["(drp.drug_key = %s AND drp.pt = %s)"] * len(GOLDEN_SIGNALS)
    )

    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(
        f"""
        SELECT
            drp.drug_key,
            drp.pt,
            MIN(drp.fda_dt) AS first_flagged_date
        FROM drug_reaction_pairs drp
        WHERE ({drp_placeholders_aliased})
        GROUP BY drp.drug_key, drp.pt
        """,
        drp_params,
    )
    first_flagged = {
        (row[0], row[1]): row[2] for row in cur.fetchall()
    }
    cur.close()
    conn.close()

    # Query 2 — which golden pairs cleared PRR + min cases thresholds
    flagged_set = _get_flagged_set()

    # Build per-signal results
    results = []
    for g in GOLDEN_SIGNALS:
        key           = (g["drug_key"], g["pt"])
        first_date    = first_flagged.get(key)
        flagged       = key in flagged_set
        fda_comm_date = g["fda_comm_date"]

        lead_time_days = None
        if first_date is not None and fda_comm_date is not None:
            lead_time_days = (fda_comm_date - first_date).days

        results.append({
            "drug_key"          : g["drug_key"],
            "pt"                : g["pt"],
            "fda_comm_date"     : fda_comm_date.isoformat() if fda_comm_date else None,
            "fda_comm_label"    : g["fda_comm_label"],
            "first_flagged_date": first_date.isoformat() if first_date else None,
            "lead_time_days"    : lead_time_days,
            "flagged"           : flagged,
        })

    # ── Summary stats ─────────────────────────────────────────────────────
    lead_times = [
        r["lead_time_days"]
        for r in results
        if r["lead_time_days"] is not None
    ]

    median_lead_time    = sorted(lead_times)[len(lead_times) // 2] if lead_times else None
    positive_detections = sum(1 for lt in lead_times if lt > 0)
    flagged_count       = sum(1 for r in results if r["flagged"])

    log.info(
        "lead_times_computed total=%d flagged=%d median=%s positive=%d",
        len(results), flagged_count, median_lead_time, positive_detections,
    )

    return {
        "results"            : results,
        "median_lead_time"   : median_lead_time,
        "positive_detections": positive_detections,
        "total_golden"       : len(GOLDEN_SIGNALS),
        "flagged_count"      : flagged_count,
        "prr_threshold"      : PRR_THRESHOLD,
        "min_cases"          : MIN_CASES,
    }


@router.get("/precision-recall")
def get_precision_recall():
    """
    GET /evaluation/precision-recall

    Counts how many of the 10 golden signals appear in signals_flagged
    with PRR >= EVAL_PRR_THRESHOLD AND drug_reaction_count >= EVAL_MIN_CASES.

    Returns summary counts, precision score, and per-signal breakdown.
    Precision = flagged_golden / total_golden

    Used by: Evaluation Dashboard precision-recall table
    """
    placeholders, params = _build_pair_placeholders(GOLDEN_SIGNALS)

    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(
        f"""
        SELECT drug_key, pt, prr, drug_reaction_count, stat_score
        FROM signals_flagged
        WHERE ({placeholders})
        AND prr >= %s
        AND drug_reaction_count >= %s
        """,
        params + [PRR_THRESHOLD, MIN_CASES],
    )
    rows    = cur.fetchall()
    columns = [desc[0].lower() for desc in cur.description]
    cur.close()
    conn.close()

    flagged_signals = {
        (row[0], row[1]): dict(zip(columns, row)) for row in rows
    }
    
    total_golden = sum(1 for g in GOLDEN_SIGNALS if g["fda_comm_date"] is not None)
    flagged_count = len(flagged_signals)
    not_flagged   = total_golden - flagged_count
    precision     = round(flagged_count / total_golden, 3)

    breakdown = []
    for g in GOLDEN_SIGNALS:
        key    = (g["drug_key"], g["pt"])
        sf_row = flagged_signals.get(key)
        breakdown.append({
            "drug_key"           : g["drug_key"],
            "pt"                 : g["pt"],
            "fda_comm_label"     : g["fda_comm_label"],
            "flagged"            : key in flagged_signals,
            "prr"                : float(sf_row["prr"])               if sf_row else None,
            "drug_reaction_count": int(sf_row["drug_reaction_count"]) if sf_row else None,
            "stat_score"         : float(sf_row["stat_score"])        if sf_row else None,
        })

    log.info(
        "precision_recall_computed flagged=%d total=%d precision=%.3f",
        flagged_count, total_golden, precision,
    )

    return {
        "total_golden" : total_golden,
        "flagged"      : flagged_count,
        "not_flagged"  : not_flagged,
        "precision"    : precision,
        "prr_threshold": PRR_THRESHOLD,
        "min_cases"    : MIN_CASES,
        "breakdown"    : breakdown,
    }


@router.get("/summary")
def get_summary():
    """
    GET /evaluation/summary

    Single endpoint for all Evaluation Dashboard header metric cards.
    Combines precision-recall and lead time summary in one call so
    Streamlit doesn't need to make two separate requests on page load.

    Returns:
        total_golden        — always 10
        flagged             — golden signals in signals_flagged above threshold
        precision           — flagged / total_golden
        median_lead_time    — median days across signals with a computed lead time
        positive_detections — signals detected before FDA communication date
        prr_threshold       — current EVAL_PRR_THRESHOLD value
        min_cases           — current EVAL_MIN_CASES value

    Used by: Evaluation Dashboard header metric cards
    """
    # Reuse shared helper for flagged set
    valid_signals = [g for g in GOLDEN_SIGNALS if g["fda_comm_date"] is not None]

    flagged_set   = _get_flagged_set()
    flagged_count = sum(1 for g in valid_signals
                        if (g["drug_key"], g["pt"]) in flagged_set)
    total_valid   = len(valid_signals)
    precision     = round(flagged_count / total_valid, 3)

    drp_placeholders_aliased = " OR ".join(
        ["(drp.drug_key = %s AND drp.pt = %s)"] * len(GOLDEN_SIGNALS)
    )
    drp_params = []
    for g in GOLDEN_SIGNALS:
        drp_params.extend([g["drug_key"], g["pt"]])

    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(
        f"""
        SELECT drp.drug_key, drp.pt, MIN(drp.fda_dt) AS first_flagged_date
        FROM drug_reaction_pairs drp
        WHERE ({drp_placeholders_aliased})
        GROUP BY drp.drug_key, drp.pt
        """,
        drp_params,
    )
    first_flagged = {(row[0], row[1]): row[2] for row in cur.fetchall()}
    cur.close()
    conn.close()

    lead_times = []
    for g in GOLDEN_SIGNALS:
        key        = (g["drug_key"], g["pt"])
        first_date = first_flagged.get(key)
        if first_date is not None and g["fda_comm_date"] is not None:
            lead_times.append((g["fda_comm_date"] - first_date).days)

    median_lead_time    = sorted(lead_times)[len(lead_times) // 2] if lead_times else None
    positive_detections = sum(1 for lt in lead_times if lt > 0)

    log.info(
        "summary_computed flagged=%d precision=%.3f median=%s",
        flagged_count, precision, median_lead_time,
    )

    return {
        "total_golden"       : len(GOLDEN_SIGNALS),
        "flagged"            : flagged_count,
        "not_flagged"        : total_valid - flagged_count,
        "precision"          : precision,            
        "median_lead_time"   : median_lead_time,
        "positive_detections": positive_detections,
        "prr_threshold"      : PRR_THRESHOLD,
        "min_cases"          : MIN_CASES,
        "precision_denominator": total_valid, 
    }