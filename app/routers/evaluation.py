"""
evaluation.py — FastAPI router for evaluation endpoints.

Endpoints:
    GET /evaluation/lead-times       — detection lead time per golden signal

Data sources:
    drug_reaction_pairs — MIN(fda_dt) per (drug_key, pt) = first flagged date
    signals_flagged     — confirms signal cleared PRR threshold
 
Golden signal set:
    10 drug-reaction pairs with documented FDA safety communications in 2023.
    FDA communication dates are hardcoded constants from the proposal table.
    These never change — they are historical FDA records.
 
Lead time formula:
    lead_time_days = fda_comm_date - first_flagged_date
    Positive = MedSignal detected before FDA communicated (good)
    Negative = MedSignal detected after FDA communicated (signal was late)

"""

import logging
from datetime import date
from fastapi import APIRouter, HTTPException
from app.utils.snowflake_client import get_conn

log    = logging.getLogger(__name__)
router = APIRouter(prefix="/evaluation", tags=["evaluation"])

# ── Golden signal constants ───────────────────────────────────────────────────
# Source: proposal Table — Golden Signal Validation Set (p30-31)
# FDA communication dates are hardcoded — these are historical records
# that do not change. drug_key values must match signals_flagged exactly.

GOLDEN_SIGNALS = [
    {
        "drug_key"      : "dupilumab",
        "pt"            : "skin fissures",
        "fda_comm_date" : date(2024, 1, 1),
        "fda_comm_label": "FDA Label Update — January 2024",
    },
    {
        "drug_key"      : "gabapentin",
        "pt"            : "cardio-respiratory arrest",
        "fda_comm_date" : date(2023, 12, 1),
        "fda_comm_label": "FDA Drug Safety Communication — December 2023",
    },
    {
        "drug_key"      : "pregabalin",
        "pt"            : "coma",
        "fda_comm_date" : date(2023, 12, 1),
        "fda_comm_label": "FDA Drug Safety Communication — December 2023",
    },
    {
        "drug_key"      : "levetiracetam",
        "pt"            : "tonic-clonic seizure",
        "fda_comm_date" : date(2023, 11, 1),
        "fda_comm_label": "FDA Safety Communication — November 2023",
    },
    {
        "drug_key"      : "tirzepatide",
        "pt"            : "decreased appetite",
        "fda_comm_date" : date(2023, 9, 1),
        "fda_comm_label": "FDA Drug Safety Communication — September 2023",
    },
    {
        "drug_key"      : "semaglutide",
        "pt"            : "increased appetite",
        "fda_comm_date" : date(2023, 9, 1),
        "fda_comm_label": "FDA Drug Safety Communication — September 2023",
    },
    {
        "drug_key"      : "empagliflozin",
        "pt"            : "haemoglobin a1c increased",
        "fda_comm_date" : date(2023, 8, 1),
        "fda_comm_label": "FDA Drug Safety Communication — August 2023",
    },
    {
        "drug_key"      : "bupropion",
        "pt"            : "seizure",
        "fda_comm_date" : date(2023, 5, 1),
        "fda_comm_label": "FDA Drug Safety Communication — May 2023",
    },
    {
        "drug_key"      : "dapagliflozin",
        "pt"            : "glomerular filtration rate decreased",
        "fda_comm_date" : date(2023, 5, 1),
        "fda_comm_label": "FDA Label Update — May 2023",
    },
    {
        "drug_key"      : "metformin",
        "pt"            : "diabetic ketoacidosis",
        "fda_comm_date" : date(2023, 4, 1),
        "fda_comm_label": "FDA Drug Safety Communication — April 2023",
    },
]


@router.get("/lead-times")
def get_lead_times():
    """
    GET /evaluation/lead-times
 
    For each of the 10 golden signals:
        - Looks up MIN(fda_dt) from drug_reaction_pairs as first_flagged_date
          (earliest date MedSignal saw this drug-reaction pair in FAERS data)
        - Checks signals_flagged to confirm signal cleared PRR threshold
        - Computes lead_time_days = fda_comm_date - first_flagged_date
 
    Returns one row per golden signal including flagged status and lead time.
    Signals not in drug_reaction_pairs return first_flagged_date=null, flagged=false.
 
    Used by: Evaluation Dashboard bar chart
    """
    conn = get_conn()
    cur  = conn.cursor()
 
    # Build parameterised IN clause for all 10 golden drug-reaction pairs
    # Each pair is matched on BOTH drug_key AND pt to avoid cross-drug matches
    placeholders = " OR ".join(
        ["(drp.drug_key = %s AND drp.pt = %s)"] * len(GOLDEN_SIGNALS)
    )
    params_drp = []
    for g in GOLDEN_SIGNALS:
        params_drp.extend([g["drug_key"], g["pt"]])
 
    # Query 1 — first flagged date per golden pair from drug_reaction_pairs
    cur.execute(
        f"""
        SELECT
            drp.drug_key,
            drp.pt,
            MIN(drp.fda_dt) AS first_flagged_date
        FROM drug_reaction_pairs drp
        WHERE ({placeholders})
        GROUP BY drp.drug_key, drp.pt
        """,
        params_drp,
    )
    rows = cur.fetchall()
    first_flagged = {
        (row[0], row[1]): row[2] for row in rows
    }
 
    # Query 2 — which golden pairs are in signals_flagged (cleared PRR threshold)
    placeholders_sf = " OR ".join(
        ["(drug_key = %s AND pt = %s)"] * len(GOLDEN_SIGNALS)
    )
    params_sf = []
    for g in GOLDEN_SIGNALS:
        params_sf.extend([g["drug_key"], g["pt"]])
 
    cur.execute(
        f"""
        SELECT drug_key, pt
        FROM signals_flagged
        WHERE ({placeholders_sf})
        """,
        params_sf,
    )
    flagged_set = {(row[0], row[1]) for row in cur.fetchall()}
 
    cur.close()
    conn.close()
 
    # Build result for each golden signal
    results = []
    for g in GOLDEN_SIGNALS:
        key              = (g["drug_key"], g["pt"])
        first_date       = first_flagged.get(key)
        flagged          = key in flagged_set
        fda_comm_date    = g["fda_comm_date"]
 
        lead_time_days = None
        if first_date is not None:
            # first_date from Snowflake is a datetime.date object
            lead_time_days = (fda_comm_date - first_date).days
 
        results.append({
            "drug_key"         : g["drug_key"],
            "pt"               : g["pt"],
            "fda_comm_date"    : fda_comm_date.isoformat(),
            "fda_comm_label"   : g["fda_comm_label"],
            "first_flagged_date": first_date.isoformat() if first_date else None,
            "lead_time_days"   : lead_time_days,
            "flagged"          : flagged,
        })
 
    log.info(
        "lead_times_computed total=%d flagged=%d",
        len(results),
        sum(1 for r in results if r["flagged"]),
    )
    return results
 
