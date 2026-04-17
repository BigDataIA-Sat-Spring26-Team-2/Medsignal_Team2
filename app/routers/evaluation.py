"""
evaluation.py — FastAPI router for evaluation endpoints.

"""

import logging
from datetime import date

log    = logging.getLogger(__name__)

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



