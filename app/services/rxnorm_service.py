"""

Builds the RxNorm drug name cache in Supabase.
Run this ONCE before Spark Branch 1.

"""

import os
import time
import glob
import logging
from unittest import result
import requests
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PG_URL       = os.getenv("DATABASE_URL")
RXNORM_BASE  = "https://rxnav.nlm.nih.gov/REST"
DATA_DIR     = "data/faers"
SLEEP        = 0.12   # 0.12s between calls = stay under 10 req/s

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Step 1 — collect unique drug names from raw FAERS files
# ---------------------------------------------------------------------------

def get_unique_drug_names() -> list:
    """
    Read all DRUG*.txt files from all quarters.
    Extract unique values from the prod_ai column.
    prod_ai = active ingredient field — primary normalization target.
    """
    names = set()

    files = glob.glob(f"{DATA_DIR}/**/*.txt", recursive=True)
    drug_files = [f for f in files if "DRUG" in f.upper()]

    log.info("Found %d DRUG files", len(drug_files))

    for filepath in drug_files:
        df = pd.read_csv(
            filepath,
            sep="$",
            encoding="latin1",
            usecols=["prod_ai"],   # only read this one column
            dtype=str,
            low_memory=False,
        )
        before = len(names)
        names.update(
            df["prod_ai"]
            .dropna()
            .str.upper()
            .str.strip()
        )
        log.info("  %s → +%d names (total: %d)", filepath, len(names) - before, len(names))

    log.info("Total unique drug names: %d", len(names))
    return list(names)

# ---------------------------------------------------------------------------
# Step 2 — call NIH RxNorm API for one drug name
# ---------------------------------------------------------------------------

def resolve_one(drug_name: str) -> dict:
    """
    Call RxNorm REST API to get canonical name + RxCUI for one drug.

    Flow:
        1. POST drug name → get RxCUI back
        2. If RxCUI found → get canonical name from RxCUI
        3. If nothing found → use the raw name as fallback

    No API key needed for RxNorm.
    """
    try:
        # Step A — get RxCUI from drug name
        r = requests.get(
            f"{RXNORM_BASE}/rxcui.json",
            params={"name": drug_name, "search": 1},
            timeout=10,
        )
        r.raise_for_status()
        ids = r.json().get("idGroup", {}).get("rxnormId", [])

        if not ids:
            # No match found — use raw name as canonical
            return {"rxcui": None, "canonical": drug_name}

        rxcui = ids[0]

        # Step B — get canonical name from RxCUI
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

        return {"rxcui": rxcui, "canonical": canonical}

    except Exception as e:
        # If API call fails for any reason — fallback to raw name
        log.warning("RxNorm lookup failed for '%s': %s", drug_name, e)
        return {"rxcui": None, "canonical": drug_name}

# ---------------------------------------------------------------------------
# Step 3 — store results in Supabase rxnorm_cache table
# ---------------------------------------------------------------------------

def build_cache(drug_names: list):
    """
    For each drug name:
        - Call RxNorm API
        - Insert result into rxnorm_cache
        - ON CONFLICT DO NOTHING = safe to re-run anytime

    Progress is logged every 100 names.
    """
    conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),  
)
    cur  = conn.cursor()

    total   = len(drug_names)
    success = 0
    failed  = 0

    for i, name in enumerate(drug_names, 1):
        result = resolve_one(name)
        name_safe      = name[:1000] if name else name
        canonical_safe = result["canonical"][:1000] if result["canonical"] else result["canonical"]

        cur.execute(
            """
            INSERT INTO rxnorm_cache (prod_ai, rxcui, canonical_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (prod_ai) DO NOTHING
            """,
            (name, result["rxcui"], result["canonical"]),
        )

        success += 1

        if i % 100 == 0:
            conn.commit()
            log.info("Progress: %d / %d (%.0f%%)", i, total, i / total * 100)

        time.sleep(SLEEP)   # rate limit — stay under 10 req/s

    conn.commit()
    cur.close()
    conn.close()

    log.info("Cache built: %d resolved, %d failed", success, failed)

# ---------------------------------------------------------------------------
# Validation — run after build_cache to confirm it worked
# ---------------------------------------------------------------------------

def validate_cache():
    """
    Quick sanity check after building.
    Confirms key drugs are resolved correctly.
    """
    conn = psycopg2.connect(PG_URL)
    cur  = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM rxnorm_cache")
    total = cur.fetchone()[0]
    log.info("Total rows in rxnorm_cache: %d", total)

    # Check a known drug resolves correctly
    cur.execute(
    "SELECT prod_ai, canonical_name FROM rxnorm_cache WHERE prod_ai ILIKE '%finasteride%' LIMIT 3"
)
    for raw, canonical in rows:
        log.info("  %s → %s", raw, canonical)

    cur.close()
    conn.close()

    assert total > 0, "Cache is empty — something went wrong"
    log.info("Validation passed")

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("=" * 50)
    log.info("MedSignal — RxNorm Cache Builder")
    log.info("=" * 50)

    # Step 1
    log.info("Step 1: Collecting unique drug names from FAERS files...")
    names = get_unique_drug_names()

    # Step 2 + 3
    log.info("Step 2+3: Resolving via RxNorm API and storing in Supabase...")
    log.info("Estimated time: %d names × 0.12s = %.0f minutes",
             len(names), len(names) * 0.12 / 60)
    build_cache(names)

    # Validate
    log.info("Validating cache...")
    validate_cache()

    log.info("Done. Samiksha can now run Spark Branch 1.")