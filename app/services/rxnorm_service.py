
"""
rxnorm_service.py
-----------------
Builds the RxNorm drug name cache in Supabase.
Resolves to base ingredient level (TTY=IN) so salt forms,
prodrugs, and formulation variants collapse to one canonical name.

Run  : python -m app.services.rxnorm_service

"""

# import os
# import time
# import glob
# import logging
# import requests
# import psycopg2
# import pandas as pd
# from dotenv import load_dotenv

# load_dotenv()

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s  %(levelname)-8s  %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
# )
# log = logging.getLogger(__name__)

# RXNORM_BASE = "https://rxnav.nlm.nih.gov/REST"
# DATA_DIR    = "data/faers"
# SLEEP       = 0.12


# def get_conn():
#     return psycopg2.connect(
#         host    =os.getenv("POSTGRES_HOST"),
#         port    =os.getenv("POSTGRES_PORT"),
#         dbname  =os.getenv("POSTGRES_DB"),
#         user    =os.getenv("POSTGRES_USER"),
#         password=os.getenv("POSTGRES_PASSWORD"),
#     )


# def get_rxcui(drug_name: str) -> str | None:
#     """Call 1 — resolve drug name to RxCUI."""
#     try:
#         r = requests.get(
#             f"{RXNORM_BASE}/rxcui.json",
#             params={"name": drug_name, "search": 1},
#             timeout=10,
#         )
#         r.raise_for_status()
#         ids = r.json().get("idGroup", {}).get("rxnormId", [])
#         return ids[0] if ids else None
#     except Exception as e:
#         log.warning("rxcui_failed drug=%s error=%s", drug_name, e)
#         return None


# def get_base_ingredient(rxcui: str) -> tuple:
#     """
#     Call 2 — resolve RxCUI to base ingredient (TTY=IN).

#     Collapses:
#         bupropion hydrochloride   → bupropion
#         dapagliflozin propanediol → dapagliflozin
#         gabapentin enacarbil      → gabapentin
#     """
#     try:
#         r = requests.get(
#             f"{RXNORM_BASE}/rxcui/{rxcui}/related.json",
#             params={"tty": "IN"},
#             timeout=10,
#         )
#         r.raise_for_status()
#         groups = r.json().get("relatedGroup", {}).get("conceptGroup", [])
#         for group in groups:
#             if group.get("tty") == "IN":
#                 props = group.get("conceptProperties", [])
#                 if props:
#                     return props[0].get("rxcui"), props[0].get("name")
#         return None, None
#     except Exception as e:
#         log.warning("base_ingredient_failed rxcui=%s error=%s", rxcui, e)
#         return None, None


# def resolve_one(drug_name: str) -> dict:
#     """
#     Full resolution for one drug name — two API calls:
#         1. Name → RxCUI
#         2. RxCUI → base ingredient (TTY=IN)

#     Fallback at each step if API fails or returns nothing.
#     """
#     rxcui = get_rxcui(drug_name)
#     time.sleep(SLEEP)

#     if not rxcui:
#         return {"rxcui": None, "canonical": drug_name.lower()}

#     base_rxcui, base_name = get_base_ingredient(rxcui)
#     time.sleep(SLEEP)

#     if base_name:
#         return {"rxcui": base_rxcui, "canonical": base_name.lower()}

#     # RxCUI found but no base ingredient — get canonical name from RxCUI
#     try:
#         nr = requests.get(
#             f"{RXNORM_BASE}/rxcui/{rxcui}/property.json",
#             params={"propName": "RxNorm Name"},
#             timeout=10,
#         )
#         nr.raise_for_status()
#         concepts = (
#             nr.json()
#             .get("propConceptGroup", {})
#             .get("propConcept", [])
#         )
#         canonical = concepts[0].get("propValue", drug_name) if concepts else drug_name
#         return {"rxcui": rxcui, "canonical": canonical.lower()}
#     except Exception:
#         return {"rxcui": rxcui, "canonical": drug_name.lower()}


# def get_unique_drug_names() -> list:
#     names = set()
#     files = glob.glob(f"{DATA_DIR}/**/*.txt", recursive=True)
#     drug_files = [f for f in files if "DRUG" in f.upper()]
#     log.info("Found %d DRUG files", len(drug_files))

#     for filepath in drug_files:
#         df = pd.read_csv(
#             filepath, sep="$", encoding="latin1",
#             usecols=["prod_ai"], dtype=str, low_memory=False,
#         )
#         before = len(names)
#         names.update(df["prod_ai"].dropna().str.upper().str.strip())
#         log.info("  %s → +%d names (total: %d)",
#                  filepath, len(names) - before, len(names))

#     log.info("Total unique drug names: %d", len(names))
#     return list(names)


# def build_cache(drug_names: list):
#     conn = get_conn()
#     cur  = conn.cursor()
#     total = len(drug_names)

#     for i, name in enumerate(drug_names, 1):
#         result = resolve_one(name)

#         name_safe  = name[:1000]                         if name                    else name
#         canon_safe = result["canonical"][:1000]          if result["canonical"]     else result["canonical"]

#         cur.execute(
#             """
#             INSERT INTO rxnorm_cache (prod_ai, rxcui, canonical_name)
#             VALUES (%s, %s, %s)
#             ON CONFLICT (prod_ai) DO UPDATE
#                 SET rxcui          = EXCLUDED.rxcui,
#                     canonical_name = EXCLUDED.canonical_name
#             """,
#             (name_safe, result["rxcui"], canon_safe),
#         )

#         if i % 100 == 0:
#             conn.commit()
#             log.info("Progress: %d / %d (%.0f%%)", i, total, i / total * 100)

#     conn.commit()
#     cur.close()
#     conn.close()
#     log.info("Cache built: %d names resolved", total)


# def validate_cache():
#     conn = get_conn()
#     cur  = conn.cursor()

#     cur.execute("SELECT COUNT(*) FROM rxnorm_cache")
#     log.info("Total rows in rxnorm_cache: %d", cur.fetchone()[0])

#     # Confirm salt forms collapsed to base ingredient
#     test_cases = [
#         ("BUPROPION HYDROCHLORIDE",   "bupropion"),
#         ("DAPAGLIFLOZIN PROPANEDIOL", "dapagliflozin"),
#         ("GABAPENTIN ENACARBIL",      "gabapentin"),
#         ("FINASTERIDE",               "finasteride"),
#         ("DUPILUMAB",                 "dupilumab"),
#     ]

#     all_pass = True
#     for raw, expected in test_cases:
#         cur.execute(
#             "SELECT canonical_name FROM rxnorm_cache WHERE prod_ai = %s", (raw,)
#         )
#         row = cur.fetchone()
#         actual = row[0] if row else "NOT FOUND"
#         if row and expected in actual.lower():
#             log.info("  PASS  %s → %s", raw, actual)
#         else:
#             log.warning("  FAIL  %s → %s (expected: %s)", raw, actual, expected)
#             all_pass = False

#     cur.close()
#     conn.close()
#     return all_pass


# if __name__ == "__main__":
#     log.info("=" * 55)
#     log.info("MedSignal — RxNorm Cache Builder (base ingredient level)")
#     log.info("=" * 55)

#     names = get_unique_drug_names()
#     log.info(
#         "Estimated time: %d names × 2 calls × 0.12s = %.0f minutes",
#         len(names), len(names) * 2 * SLEEP / 60
#     )

#     build_cache(names)

#     passed = validate_cache()
#     if passed:
#         log.info("All validation checks passed.")
#     else:
#         log.warning("Some drugs did not resolve correctly — check cache before running Branch 1.")

#     log.info("Done. Truncate rxnorm_cache first if rebuilding from scratch.")
import os
import time
import glob
import logging
import requests
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
 
load_dotenv()
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)
 
RXNORM_BASE = "https://rxnav.nlm.nih.gov/REST"
DATA_DIR    = "data/faers"
SLEEP       = 0.12
 
 
# ── Snowflake connection ──────────────────────────────────────────────────────
 
def get_conn() -> snowflake.connector.SnowflakeConnection:
    """
    Returns a Snowflake connector connection.
    Reads the same SNOWFLAKE_* env vars used by spark_branch1.py.
    """
    return snowflake.connector.connect(
        account  = os.getenv("SNOWFLAKE_ACCOUNT"),
        user     = os.getenv("SNOWFLAKE_USER"),
        password = os.getenv("SNOWFLAKE_PASSWORD"),
        database = os.getenv("SNOWFLAKE_DATABASE"),
        schema   = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        warehouse= os.getenv("SNOWFLAKE_WAREHOUSE"),
    )
 
 
# ── RxNorm API helpers ────────────────────────────────────────────────────────
 
def get_rxcui(drug_name: str) -> str | None:
    """Call 1 — resolve drug name to RxCUI."""
    try:
        r = requests.get(
            f"{RXNORM_BASE}/rxcui.json",
            params={"name": drug_name, "search": 1},
            timeout=10,
        )
        r.raise_for_status()
        ids = r.json().get("idGroup", {}).get("rxnormId", [])
        return ids[0] if ids else None
    except Exception as e:
        log.warning("rxcui_failed drug=%s error=%s", drug_name, e)
        return None
 
 
def get_base_ingredient(rxcui: str) -> tuple:
    """
    Call 2 — resolve RxCUI to base ingredient (TTY=IN).
 
    Collapses:
        bupropion hydrochloride   → bupropion
        dapagliflozin propanediol → dapagliflozin
        gabapentin enacarbil      → gabapentin
    """
    try:
        r = requests.get(
            f"{RXNORM_BASE}/rxcui/{rxcui}/related.json",
            params={"tty": "IN"},
            timeout=10,
        )
        r.raise_for_status()
        groups = r.json().get("relatedGroup", {}).get("conceptGroup", [])
        for group in groups:
            if group.get("tty") == "IN":
                props = group.get("conceptProperties", [])
                if props:
                    return props[0].get("rxcui"), props[0].get("name")
        return None, None
    except Exception as e:
        log.warning("base_ingredient_failed rxcui=%s error=%s", rxcui, e)
        return None, None
 
 
def resolve_one(drug_name: str) -> dict:
    """
    Full resolution for one drug name — two API calls:
        1. Name → RxCUI
        2. RxCUI → base ingredient (TTY=IN)
 
    Fallback at each step if API fails or returns nothing.
    """
    rxcui = get_rxcui(drug_name)
    time.sleep(SLEEP)
 
    if not rxcui:
        return {"rxcui": None, "canonical": drug_name.lower()}
 
    base_rxcui, base_name = get_base_ingredient(rxcui)
    time.sleep(SLEEP)
 
    if base_name:
        return {"rxcui": base_rxcui, "canonical": base_name.lower()}
 
    # RxCUI found but no base ingredient — get canonical name from RxCUI
    try:
        nr = requests.get(
            f"{RXNORM_BASE}/rxcui/{rxcui}/property.json",
            params={"propName": "RxNorm Name"},
            timeout=10,
        )
        nr.raise_for_status()
        concepts = (
            nr.json()
            .get("propConceptGroup", {})
            .get("propConcept", [])
        )
        canonical = concepts[0].get("propValue", drug_name) if concepts else drug_name
        return {"rxcui": rxcui, "canonical": canonical.lower()}
    except Exception:
        return {"rxcui": rxcui, "canonical": drug_name.lower()}
 
 
# ── Drug name discovery ───────────────────────────────────────────────────────
 
def get_unique_drug_names() -> list:
    names = set()
    files = glob.glob(f"{DATA_DIR}/**/*.txt", recursive=True)
    drug_files = [f for f in files if "DRUG" in f.upper()]
    log.info("Found %d DRUG files", len(drug_files))
 
    for filepath in drug_files:
        df = pd.read_csv(
            filepath, sep="$", encoding="latin1",
            usecols=["prod_ai"], dtype=str, low_memory=False,
        )
        before = len(names)
        names.update(df["prod_ai"].dropna().str.upper().str.strip())
        log.info("  %s → +%d names (total: %d)",
                 filepath, len(names) - before, len(names))
 
    log.info("Total unique drug names: %d", len(names))
    return list(names)
 
 
# ── Cache builder ─────────────────────────────────────────────────────────────
 
def build_cache(drug_names: list):
    """
    Resolves each drug name via RxNorm and upserts into the Snowflake
    rxnorm_cache table.
 
    Snowflake does not support psycopg2-style %s placeholders —
    uses %s via snowflake.connector which supports the same DB-API 2.0
    syntax. MERGE replaces ON CONFLICT (not supported in Snowflake SQL).
    """
    conn  = get_conn()
    cur   = conn.cursor()
    total = len(drug_names)
 
    for i, name in enumerate(drug_names, 1):
        result = resolve_one(name)
 
        name_safe  = name[:1000]                       if name                  else name
        canon_safe = result["canonical"][:1000]        if result["canonical"]   else result["canonical"]
 
        # Snowflake MERGE — equivalent to PostgreSQL ON CONFLICT DO UPDATE
        cur.execute(
            """
            MERGE INTO rxnorm_cache AS target
            USING (SELECT %s AS prod_ai, %s AS rxcui, %s AS canonical_name) AS source
            ON target.prod_ai = source.prod_ai
            WHEN MATCHED THEN
                UPDATE SET rxcui          = source.rxcui,
                           canonical_name = source.canonical_name
            WHEN NOT MATCHED THEN
                INSERT (prod_ai, rxcui, canonical_name)
                VALUES (source.prod_ai, source.rxcui, source.canonical_name)
            """,
            (name_safe, result["rxcui"], canon_safe),
        )
 
        if i % 100 == 0:
            conn.commit()
            log.info("Progress: %d / %d (%.0f%%)", i, total, i / total * 100)
 
    conn.commit()
    cur.close()
    conn.close()
    log.info("Cache built: %d names resolved", total)
 
 
# ── Validation ────────────────────────────────────────────────────────────────
 
def validate_cache():
    conn = get_conn()
    cur  = conn.cursor()
 
    cur.execute("SELECT COUNT(*) FROM rxnorm_cache")
    log.info("Total rows in rxnorm_cache: %d", cur.fetchone()[0])
 
    # Confirm salt forms collapsed to base ingredient
    test_cases = [
        ("BUPROPION HYDROCHLORIDE",   "bupropion"),
        ("DAPAGLIFLOZIN PROPANEDIOL", "dapagliflozin"),
        ("GABAPENTIN ENACARBIL",      "gabapentin"),
        ("FINASTERIDE",               "finasteride"),
        ("DUPILUMAB",                 "dupilumab"),
    ]
 
    all_pass = True
    for raw, expected in test_cases:
        cur.execute(
            "SELECT canonical_name FROM rxnorm_cache WHERE prod_ai = %s", (raw,)
        )
        row = cur.fetchone()
        actual = row[0] if row else "NOT FOUND"
        if row and expected in actual.lower():
            log.info("  PASS  %s → %s", raw, actual)
        else:
            log.warning("  FAIL  %s → %s (expected: %s)", raw, actual, expected)
            all_pass = False
 
    cur.close()
    conn.close()
    return all_pass
 
 
# ── Entry point ───────────────────────────────────────────────────────────────
 
if __name__ == "__main__":
    log.info("=" * 55)
    log.info("MedSignal — RxNorm Cache Builder (Snowflake, base ingredient level)")
    log.info("=" * 55)
 
    names = get_unique_drug_names()
    log.info(
        "Estimated time: %d names × 2 calls × 0.12s = %.0f minutes",
        len(names), len(names) * 2 * SLEEP / 60
    )
 
    build_cache(names)
 
    passed = validate_cache()
    if passed:
        log.info("All validation checks passed.")
    else:
        log.warning("Some drugs did not resolve correctly — check cache before running Branch 1.")
 
    log.info("Done. TRUNCATE rxnorm_cache first in Snowflake if rebuilding from scratch.")
 