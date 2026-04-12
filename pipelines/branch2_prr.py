"""


Reads drug_reaction_pairs from Supabase.
Computes PRR for every (drug_key, pt) pair.
Applies threshold + quality filters.
Writes to signals_flagged in Supabase.


"""

import os
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, lit, when,
    countDistinct, max as spark_max,
    sum as spark_sum, percentile_approx
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PG_URL = (
    f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:"
    f"{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

PG_PROPS = {
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver":   "org.postgresql.Driver",
}

# PRR thresholds — full dataset
MIN_A          = 50
MIN_C          = 200
MIN_DRUG_TOTAL = 1000
MIN_PRR        = 2.0

# PRR thresholds — relaxed for POC / partial data
MIN_A_POC          = 30
MIN_C_POC          = 100
MIN_DRUG_TOTAL_POC = 500

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def get_spark() -> SparkSession:
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"]        = os.environ["PATH"] + ";C:\\hadoop\\bin"

    jar_path = os.path.abspath("jars/postgresql-42.6.0.jar")

    return (
        SparkSession.builder
        .appName("MedSignal-Branch2-PRR")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.ansi.enabled", "false")        # return null on divide-by-zero
        .config("spark.driver.extraClassPath", jar_path)
        .config("spark.executor.extraClassPath", jar_path)
        .getOrCreate()
    )

# ---------------------------------------------------------------------------
# Step 8 — PRR computation
# ---------------------------------------------------------------------------

def compute_prr(spark: SparkSession):
    log.info("Reading drug_reaction_pairs from Supabase...")

    pairs = spark.read.jdbc(
        PG_URL,
        "drug_reaction_pairs",
        properties=PG_PROPS,
    )

    total_rows = pairs.count()
    log.info("Total rows in drug_reaction_pairs: %d", total_rows)

    # Use relaxed thresholds if data is partial
    use_poc = total_rows < 1_000_000
    if use_poc:
        log.warning(
            "Row count %d below 1M — using relaxed POC thresholds "
            "(A>=%d, C>=%d, drug_total>=%d)",
            total_rows, MIN_A_POC, MIN_C_POC, MIN_DRUG_TOTAL_POC
        )
        min_a    = MIN_A_POC
        min_c    = MIN_C_POC
        min_drug = MIN_DRUG_TOTAL_POC
    else:
        min_a    = MIN_A
        min_c    = MIN_C
        min_drug = MIN_DRUG_TOTAL

    # Total unique cases
    total_cases = pairs.select("primaryid").distinct().count()
    log.info("Total unique cases: %d", total_cases)

    # Drug totals
    drug_totals = (
        pairs
        .groupBy("drug_key")
        .agg(count("primaryid").alias("drug_total"))
    )

    # Reaction totals
    reaction_totals = (
        pairs
        .groupBy("pt")
        .agg(count("primaryid").alias("reaction_total"))
    )

    # A counts + outcome counts
    a_counts = (
        pairs
        .groupBy("drug_key", "pt")
        .agg(
            count("primaryid").alias("A"),
            count(when(col("death_flag") == 1, 1)).alias("death_count"),
            count(when(col("hosp_flag")  == 1, 1)).alias("hosp_count"),
            count(when(col("lt_flag")    == 1, 1)).alias("lt_count"),
        )
    )

    # Build 2x2 contingency table
    prr_df = (
        a_counts
        .join(drug_totals,     "drug_key")
        .join(reaction_totals, "pt")
        .withColumn("B", col("drug_total")    - col("A"))
        .withColumn("C", col("reaction_total") - col("A"))
        .withColumn("D",
            lit(total_cases)
            - col("drug_total")
            - col("reaction_total")
            + col("A"))
        # Safe PRR — returns null if denominator is zero
        .withColumn("PRR",
            when(
                (col("C") + col("D") > 0) &
                (col("C")            > 0) &
                (col("A") + col("B") > 0),
                (col("A") / (col("A") + col("B"))) /
                (col("C") / (col("C") + col("D")))
            ).otherwise(None))
        .filter(col("PRR").isNotNull())
        .withColumn("case_count", col("A"))
    )

    return prr_df, min_a, min_c, min_drug

# ---------------------------------------------------------------------------
# Step 9 — Threshold filters
# ---------------------------------------------------------------------------

def apply_filters(prr_df, min_a, min_c, min_drug):
    log.info("Applying threshold filters...")

    signals = (
        prr_df
        .filter(col("A")          >= min_a)
        .filter(col("C")          >= min_c)
        .filter(col("drug_total") >= min_drug)
        .filter(col("PRR")        >= MIN_PRR)
    )

    count_after = signals.count()
    log.info("After threshold filters: %d signals", count_after)
    return signals

# ---------------------------------------------------------------------------
# Step 10 — Auto-detect junk MedDRA terms
# ---------------------------------------------------------------------------

def detect_junk_terms(prr_df, spark):
    """
    Auto-detect junk MedDRA terms using statistical properties.

    A term is junk if ALL three conditions are true:
      1. Appears with > 50% of all unique drugs  — non-specific
      2. Max PRR across all drugs < 1.5          — never drug-specific
      3. Total cases > 99th percentile           — high-volume artifact
    """
    total_drugs = prr_df.select("drug_key").distinct().count()
    log.info("Total unique drugs in dataset: %d", total_drugs)

    term_stats = (
        prr_df
        .groupBy("pt")
        .agg(
            countDistinct("drug_key").alias("drug_coverage"),
            spark_max("PRR").alias("max_prr"),
            spark_sum("A").alias("total_cases"),
        )
        # Safe division — return 0.0 if total_drugs is somehow 0
        .withColumn("coverage_pct",
            when(
                lit(total_drugs) > 0,
                col("drug_coverage") / lit(total_drugs)
            ).otherwise(lit(0.0)))
    )

    # 99th percentile of total cases
    p99 = term_stats.agg(
        percentile_approx("total_cases", 0.99).alias("p99")
    ).collect()[0]["p99"]
    log.info("99th percentile case count: %d", p99)

    junk_df = term_stats.filter(
        (col("coverage_pct") > 0.50) &
        (col("max_prr")      < 1.50) &
        (col("total_cases")  > p99)
    )

    junk_list = [row["pt"] for row in junk_df.collect()]
    log.info("Auto-detected %d junk terms", len(junk_list))
    if junk_list:
        log.info("Sample: %s", junk_list[:10])

    return junk_list


def apply_quality_filters(signals, prr_df, spark):
    log.info("Applying quality filters...")

    junk_terms = detect_junk_terms(prr_df, spark)

    if junk_terms:
        signals = signals.filter(~col("pt").isin(junk_terms))
        log.info("After junk filter: %d signals", signals.count())
    else:
        log.info("No junk terms detected — skipping filter")

    return signals

# ---------------------------------------------------------------------------
# Step 11 — PRR validation checkpoint
# ---------------------------------------------------------------------------

def run_checkpoint(signals):
    log.info("Running PRR validation checkpoint...")

    finasteride = signals.filter(
        col("drug_key").contains("finasteride") &
        col("pt").contains("depression")
    ).collect()

    if not finasteride:
        log.warning(
            "CHECKPOINT: finasteride-depression not found. "
            "Expected with partial data — will pass on full dataset."
        )
        return False

    prr_val    = finasteride[0]["PRR"]
    case_count = finasteride[0]["A"]
    log.info(
        "CHECKPOINT: finasteride x depression — PRR=%.2f, A=%d",
        prr_val, case_count
    )

    if prr_val >= MIN_PRR:
        log.info("CHECKPOINT PASSED ✓")
        return True
    else:
        log.warning("CHECKPOINT: PRR=%.2f below threshold %.1f", prr_val, MIN_PRR)
        return False

# ---------------------------------------------------------------------------
# Step 12 — Write to Supabase
# ---------------------------------------------------------------------------

def write_signals(signals):
    log.info("Writing signals to signals_flagged...")

    output = signals.select(
        col("drug_key"),
        col("pt"),
        col("PRR").alias("prr"),
        col("A").alias("drug_reaction_count"),
        col("B").alias("drug_no_reaction_count"),
        col("C").alias("other_reaction_count"),
        col("D").alias("other_no_reaction_count"),
        col("death_count"),
        col("hosp_count"),
        col("lt_count"),
        col("drug_total"),
    )

    # New — truncate first, then append
    import psycopg2

    def truncate_signals(pg_props):
   
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE signals_flagged CASCADE")
        conn.commit()
        cur.close()
        conn.close()
        log.info("signals_flagged truncated")

    def write_signals(signals):
        log.info("Writing signals to signals_flagged...")

        output = signals.select(
            col("drug_key"),
            col("pt"),
            col("PRR").alias("prr"),
            col("A").alias("drug_reaction_count"),
            col("B").alias("drug_no_reaction_count"),
            col("C").alias("other_reaction_count"),
            col("D").alias("other_no_reaction_count"),
            col("death_count"),
            col("hosp_count"),
            col("lt_count"),
            col("drug_total"),
        )

    # Truncate first — respects FK constraints unlike DROP
        truncate_signals(PG_PROPS)

    # Then append
        output.write.jdbc(
            PG_URL,
            "signals_flagged",
            mode="append",
            properties=PG_PROPS,
        )

        final_count = output.count()
        log.info("Wrote %d signals to signals_flagged", final_count)
        return final_count

  
# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("=" * 55)
    log.info("MedSignal — Spark Branch 2 PRR Computation")
    log.info("=" * 55)

    spark = get_spark()

    prr_df, min_a, min_c, min_drug = compute_prr(spark)
    signals = apply_filters(prr_df, min_a, min_c, min_drug)
    signals = apply_quality_filters(signals, prr_df, spark)

    checkpoint_passed = run_checkpoint(signals)
    final_count       = write_signals(signals)

    log.info("=" * 55)
    log.info("Branch 2 complete")
    log.info("Signals written   : %d", final_count)
    log.info("Checkpoint passed : %s", checkpoint_passed)
    log.info("Expected range    : 1,500-3,000 (full data)")
    log.info("=" * 55)

    spark.stop()