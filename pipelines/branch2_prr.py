"""
branch2_prr.py — Spark Branch 2: PRR Computation

"""

import os
import math
import logging
import psycopg2
import pandas as pd
import sqlalchemy
from typing import Optional
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

JUNK_TERMS: set[str] = {
    "drug ineffective", "product use issue", "off label use", "off-label use",
    "drug interaction", "no adverse event", "product quality issue",
    "condition aggravated", "intentional product use issue",
    "product use in unapproved indication",
    "inappropriate schedule of product administration",
    "drug administered to patient of inappropriate age",
    "expired product administered", "wrong technique in product usage process",
}

LATE_QUARTERS: set[str] = {"2023Q3", "2023Q4"}

PRR_THRESHOLD    = 2.0
POC_THRESHOLD    = 1_000_000   # rows below this → relaxed thresholds
SPIKE_MAX_PCT    = 0.70        # single-quarter concentration limit
SURGE_LATE_PCT   = 0.85        # Q3+Q4 concentration limit

# ── Database ──────────────────────────────────────────────────────────────────

def get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host    = os.getenv("POSTGRES_HOST"),
        port    = os.getenv("POSTGRES_PORT"),
        dbname  = os.getenv("POSTGRES_DB"),
        user    = os.getenv("POSTGRES_USER"),
        password= os.getenv("POSTGRES_PASSWORD"),
    )


def get_engine() -> sqlalchemy.engine.Engine:
    return sqlalchemy.create_engine("postgresql+psycopg2://", creator=get_conn)

# ── Scoring ───────────────────────────────────────────────────────────────────

def compute_stat_score(prr: float, case_count: int,
                       death: int, lt: int, hosp: int) -> float:
    """
    StatScore ∈ [0, 1] — read by Agent 1 from signals_flagged.

    prr_score    = min(PRR / 4.0, 1.0)                    weight 0.50
    volume_score = min(log10(A) / log10(50), 1.0)          weight 0.30
    severity     = 1.0 death | 0.75 LT | 0.50 hosp | 0.0  weight 0.20
    """
    prr_s = min(prr / 4.0, 1.0)
    vol_s = min(math.log10(max(case_count, 1)) / math.log10(50), 1.0)
    sev_s = 1.0 if death else 0.75 if lt else 0.50 if hosp else 0.0
    return round(prr_s * 0.50 + vol_s * 0.30 + sev_s * 0.20, 4)

# ── Pipeline steps ────────────────────────────────────────────────────────────

def load_pairs(engine: sqlalchemy.engine.Engine) -> pd.DataFrame:
    return pd.read_sql("""
        SELECT primaryid, drug_key, pt,
               death_flag, hosp_flag, lt_flag, source_quarter
        FROM   drug_reaction_pairs
    """, engine)


def compute_prr(pairs: pd.DataFrame) -> pd.DataFrame:
    total_cases = pairs["primaryid"].nunique()

    a_df = pairs.groupby(["drug_key", "pt"]).agg(
        A          =("primaryid", "count"),
        death_count=("death_flag", "sum"),
        hosp_count =("hosp_flag",  "sum"),
        lt_count   =("lt_flag",    "sum"),
    ).reset_index()

    drug_totals     = (pairs.groupby("drug_key")["primaryid"]
                       .count().rename("drug_total").reset_index())
    reaction_totals = (pairs.groupby("pt")["primaryid"]
                       .count().rename("reaction_total").reset_index())

    df = a_df.merge(drug_totals, on="drug_key").merge(reaction_totals, on="pt")
    df["B"] = df["drug_total"]     - df["A"]
    df["C"] = df["reaction_total"] - df["A"]
    df["D"] = total_cases - df["drug_total"] - df["reaction_total"] + df["A"]

    valid = (df["C"] > 0) & ((df["C"] + df["D"]) > 0) & ((df["A"] + df["B"]) > 0)
    df.loc[valid, "PRR"] = (
        (df.loc[valid, "A"] / (df.loc[valid, "A"] + df.loc[valid, "B"])) /
        (df.loc[valid, "C"] / (df.loc[valid, "C"] + df.loc[valid, "D"]))
    )
    return df.dropna(subset=["PRR"])


def apply_threshold_filters(df: pd.DataFrame, min_a: int,
                            min_c: int, min_drug: int) -> pd.DataFrame:
    return df[
        (df["A"]          >= min_a)   &
        (df["C"]          >= min_c)   &
        (df["drug_total"] >= min_drug) &
        (df["PRR"]        >= PRR_THRESHOLD) &
        (~df["pt"].isin(JUNK_TERMS))
    ].copy()


def apply_spike_filter(signals: pd.DataFrame,
                       pairs: pd.DataFrame) -> pd.DataFrame:
    if pairs["source_quarter"].nunique() <= 1:
        log.info("Single quarter detected — skipping spike filter")
        return signals

    qcounts = (pairs.groupby(["drug_key", "pt", "source_quarter"])
               .size().rename("qcount").reset_index())
    qtotals = (qcounts.groupby(["drug_key", "pt"])
               .agg(max_q=("qcount", "max"), total_q=("qcount", "sum"))
               .reset_index())
    qtotals["spike_pct"] = qtotals["max_q"] / qtotals["total_q"]
    clean = qtotals[qtotals["spike_pct"] <= SPIKE_MAX_PCT][["drug_key", "pt"]]
    result = signals.merge(clean, on=["drug_key", "pt"], how="inner")
    log.info("After spike filter: %d", len(result))
    return result


def apply_surge_filter(signals: pd.DataFrame,
                       pairs: pd.DataFrame) -> pd.DataFrame:
    if not pairs["source_quarter"].isin(LATE_QUARTERS).any():
        log.info("No Q3/Q4 data — skipping late-surge filter")
        return signals

    surge = pairs.copy()
    surge["is_late"] = surge["source_quarter"].isin(LATE_QUARTERS).astype(int)
    late = (surge.groupby(["drug_key", "pt"])
            .agg(late_n=("is_late", "sum"), total_n=("primaryid", "count"))
            .reset_index())
    late["late_pct"] = late["late_n"] / late["total_n"]
    non_surge = late[late["late_pct"] <= SURGE_LATE_PCT][["drug_key", "pt"]]
    result = signals.merge(non_surge, on=["drug_key", "pt"], how="inner")
    log.info("After late-surge filter: %d", len(result))
    return result


def run_checkpoint(signals: pd.DataFrame) -> bool:
    fin = signals[
        signals["drug_key"].str.contains("finasteride", case=False) &
        signals["pt"].str.contains("depression",  case=False)
    ]
    if fin.empty:
        log.warning("CHECKPOINT: finasteride × depression not found — needs full dataset")
        return False
    row = fin.iloc[0]
    log.info("CHECKPOINT PASSED — PRR=%.2f  A=%d", row["PRR"], row["A"])
    return True


def write_signals(signals: pd.DataFrame) -> None:
    signals = signals.assign(
        stat_score=signals.apply(
            lambda r: compute_stat_score(
                r["PRR"], int(r["A"]),
                int(r["death_count"]), int(r["lt_count"]), int(r["hosp_count"])
            ), axis=1
        )
    )

    records = signals.rename(columns={
        "PRR": "prr",
        "A":   "drug_reaction_count",
        "B":   "drug_no_reaction_count",
        "C":   "other_reaction_count",
        "D":   "other_no_reaction_count",
    })[[
        "drug_key", "pt", "prr",
        "drug_reaction_count", "drug_no_reaction_count",
        "other_reaction_count", "other_no_reaction_count",
        "death_count", "hosp_count", "lt_count",
        "drug_total", "stat_score",
    ]].to_dict("records")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE signals_flagged CASCADE")
            cur.executemany("""
                INSERT INTO signals_flagged (
                    drug_key, pt, prr,
                    drug_reaction_count, drug_no_reaction_count,
                    other_reaction_count, other_no_reaction_count,
                    death_count, hosp_count, lt_count,
                    drug_total, stat_score
                ) VALUES (
                    %(drug_key)s, %(pt)s, %(prr)s,
                    %(drug_reaction_count)s, %(drug_no_reaction_count)s,
                    %(other_reaction_count)s, %(other_no_reaction_count)s,
                    %(death_count)s, %(hosp_count)s, %(lt_count)s,
                    %(drug_total)s, %(stat_score)s
                )
            """, records)
        conn.commit()

    log.info("Written %d signals to signals_flagged", len(records))

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    engine = get_engine()

    pairs      = load_pairs(engine)
    total_rows = len(pairs)
    log.info("Rows: %d | Cases: %d", total_rows, pairs["primaryid"].nunique())

    min_a, min_c, min_drug = (
        (30, 100, 500) if total_rows < POC_THRESHOLD else (50, 200, 1000)
    )

    prr_df  = compute_prr(pairs)
    signals = apply_threshold_filters(prr_df, min_a, min_c, min_drug)
    log.info("After threshold + junk filters: %d", len(signals))

    signals = apply_spike_filter(signals, pairs)
    signals = apply_surge_filter(signals, pairs)

    run_checkpoint(signals)
    write_signals(signals)