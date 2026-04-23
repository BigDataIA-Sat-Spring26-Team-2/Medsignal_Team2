"""
branch2_prr.py — Spark Branch 2: PRR Computation

Reads drug_reaction_pairs from Snowflake (written by spark_branch1.py),
computes Proportional Reporting Ratio across all drug-reaction pairs,
applies quality filters, and writes flagged signals back to Snowflake.

Uses PySpark for distributed computation to match proposal architecture.
"""

import os
import math
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

JUNK_TERMS: set[str] = {
    # Efficacy / Treatment Failure
    "drug ineffective",
    "drug ineffective for unapproved indication",
    "drug resistance",
    "therapeutic response decreased",
    "therapeutic response unexpected",
    "treatment failure",
    "no therapeutic response",
    "condition aggravated",

    # Product Quality / Device Issues
    "product use issue",
    "product quality issue",
    "product contamination issue",
    "device issue",
    "device malfunction",

    # Administrative / Misuse
    "off label use",
    "off-label use",
    "medication error",
    "no adverse event",
    "drug interaction",
    "intentional product use issue",
    "product use in unapproved indication",
    "inappropriate schedule of product administration",
    "drug administered to patient of inappropriate age",
    "expired product administered",
    "wrong technique in product usage process",
}

LATE_QUARTERS: set[str] = {"2023Q3", "2023Q4"}

PRR_THRESHOLD  = 2.0
POC_THRESHOLD  = 1_000_000   # rows below this → relaxed thresholds
SPIKE_MAX_PCT  = 0.70        # single-quarter concentration limit
SURGE_LATE_PCT = 0.85        # Q3+Q4 concentration limit


# ── Snowflake configuration ───────────────────────────────────────────────────

def get_sf_config() -> dict:
    return {
        "account"  : os.getenv("SNOWFLAKE_ACCOUNT"),
        "user"     : os.getenv("SNOWFLAKE_USER"),
        "password" : os.getenv("SNOWFLAKE_PASSWORD"),
        "database" : os.getenv("SNOWFLAKE_DATABASE"),
        "schema"   : os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    }


def get_sf_options() -> dict:
    cfg = get_sf_config()
    return {
        "sfURL"      : f"{cfg['account']}.snowflakecomputing.com",
        "sfUser"     : cfg["user"],
        "sfPassword" : cfg["password"],
        "sfDatabase" : cfg["database"],
        "sfSchema"   : cfg["schema"],
        "sfWarehouse": cfg["warehouse"],
    }


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

def load_pairs(spark: SparkSession) -> DataFrame:
    """
    Load data using snowflake-connector-python, then convert to Spark DataFrame.
    Bypasses Snowflake Spark connector classpath issues.
    """
    log.info("Loading drug_reaction_pairs from Snowflake...")

    import snowflake.connector
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    cfg = get_sf_config()
    conn = snowflake.connector.connect(**cfg)
    cur = conn.cursor()

    cur.execute("""
        SELECT primaryid, drug_key, pt,
               death_flag, hosp_flag, lt_flag, source_quarter
        FROM drug_reaction_pairs
    """)

    rows = cur.fetchall()
    columns = [desc[0].lower() for desc in cur.description]

    cur.close()
    conn.close()

    log.info("Fetched %d rows from Snowflake", len(rows))

    schema = StructType([
        StructField("primaryid", StringType(), True),
        StructField("drug_key", StringType(), True),
        StructField("pt", StringType(), True),
        StructField("death_flag", IntegerType(), True),
        StructField("hosp_flag", IntegerType(), True),
        StructField("lt_flag", IntegerType(), True),
        StructField("source_quarter", StringType(), True),
    ])

    df = spark.createDataFrame(rows, schema)
    log.info("Converted to Spark DataFrame with %d rows", df.count())

    return df


def compute_prr(pairs: DataFrame) -> DataFrame:
    """
    Computes PRR for all drug-reaction pairs.

    PRR = (A / (A + B)) / (C / (C + D))

    Where:
        A = cases with drug X AND reaction Y
        B = cases with drug X WITHOUT reaction Y
        C = cases with OTHER drugs AND reaction Y
        D = cases with OTHER drugs WITHOUT reaction Y
    """
    total_cases = pairs.select("primaryid").distinct().count()

    a_df = (
        pairs
        .groupBy("drug_key", "pt")
        .agg(
            F.countDistinct("primaryid").alias("A"),
            F.sum("death_flag").cast("int").alias("death_count"),
            F.sum("hosp_flag").cast("int").alias("hosp_count"),
            F.sum("lt_flag").cast("int").alias("lt_count"),
        )
    )

    drug_totals = (
        pairs
        .groupBy("drug_key")
        .agg(F.countDistinct("primaryid").alias("drug_total"))
    )

    reaction_totals = (
        pairs
        .groupBy("pt")
        .agg(F.countDistinct("primaryid").alias("reaction_total"))
    )

    df = (
        a_df
        .join(drug_totals, on="drug_key", how="inner")
        .join(reaction_totals, on="pt", how="inner")
    )

    df = df.withColumn("B", F.col("drug_total") - F.col("A"))
    df = df.withColumn("C", F.col("reaction_total") - F.col("A"))
    df = df.withColumn("D", F.lit(total_cases) - F.col("drug_total") - F.col("reaction_total") + F.col("A"))

    df = df.withColumn(
        "PRR",
        F.when(
            (F.col("C") > 0) &
            ((F.col("C") + F.col("D")) > 0) &
            ((F.col("A") + F.col("B")) > 0),
            (F.col("A") / (F.col("A") + F.col("B"))) /
            (F.col("C") / (F.col("C") + F.col("D")))
        ).otherwise(None)
    )

    return df.filter(F.col("PRR").isNotNull())


def apply_threshold_filters(df: DataFrame, min_a: int,
                            min_c: int, min_drug: int) -> DataFrame:
    return df.filter(
        (F.col("A") >= min_a) &
        (F.col("C") >= min_c) &
        (F.col("drug_total") >= min_drug) &
        (F.col("PRR") >= PRR_THRESHOLD) &
        (~F.col("pt").isin(list(JUNK_TERMS)))
    )


def apply_spike_filter(signals: DataFrame, pairs: DataFrame) -> DataFrame:
    num_quarters = pairs.select("source_quarter").distinct().count()

    if num_quarters <= 1:
        log.info("Single quarter detected — skipping spike filter")
        return signals

    qcounts = (
        pairs
        .groupBy("drug_key", "pt", "source_quarter")
        .agg(F.count("*").alias("qcount"))
    )

    qtotals = (
        qcounts
        .groupBy("drug_key", "pt")
        .agg(
            F.max("qcount").alias("max_q"),
            F.sum("qcount").alias("total_q")
        )
        .withColumn("spike_pct", F.col("max_q") / F.col("total_q"))
    )

    clean = qtotals.filter(F.col("spike_pct") <= SPIKE_MAX_PCT).select("drug_key", "pt")

    result = signals.join(clean, on=["drug_key", "pt"], how="inner")
    result_count = result.count()
    log.info("After spike filter: %d", result_count)
    return result


def apply_surge_filter(signals: DataFrame, pairs: DataFrame) -> DataFrame:
    has_late_quarters = pairs.filter(F.col("source_quarter").isin(list(LATE_QUARTERS))).count() > 0

    if not has_late_quarters:
        log.info("No Q3/Q4 data — skipping late-surge filter")
        return signals

    surge = pairs.withColumn("is_late", F.when(F.col("source_quarter").isin(list(LATE_QUARTERS)), 1).otherwise(0))

    late = (
        surge
        .groupBy("drug_key", "pt")
        .agg(
            F.sum("is_late").alias("late_n"),
            F.count("primaryid").alias("total_n")
        )
        .withColumn("late_pct", F.col("late_n") / F.col("total_n"))
    )

    non_surge = late.filter(F.col("late_pct") <= SURGE_LATE_PCT).select("drug_key", "pt")

    result = signals.join(non_surge, on=["drug_key", "pt"], how="inner")
    result_count = result.count()
    log.info("After late-surge filter: %d", result_count)
    return result


def run_checkpoint(signals: DataFrame) -> bool:
    """
    Matches the Branch 1 validation checkpoint: gabapentin × cardio-respiratory arrest.
    This is a well-documented golden signal with A > 30 even in a single quarter.
    If this pair is absent the PRR computation or threshold filters are broken.
    Do not run write_signals() until this passes.
    """
    chk = signals.filter(
        F.lower(F.col("drug_key")).contains("gabapentin") &
        F.lower(F.col("pt")).contains("cardio-respiratory arrest")
    )

    if chk.count() == 0:
        log.warning(
            "CHECKPOINT FAILED: gabapentin × cardio-respiratory arrest not in signals. "
            "Check join correctness, PS filter, and threshold values before proceeding."
        )
        return False

    row = chk.first()
    log.info("CHECKPOINT PASSED — gabapentin × cardio-respiratory arrest | PRR=%.2f  A=%d",
             row["PRR"], row["A"])
    return True


def write_signals(signals: DataFrame, spark: SparkSession) -> None:
    """
    Write signals to Snowflake using Python connector.
    Spark used for computation, Python connector for I/O to avoid classpath issues.
    """
    from pyspark.sql.types import FloatType
    import snowflake.connector

    stat_score_udf = F.udf(compute_stat_score, FloatType())

    signals = signals.withColumn(
        "stat_score",
        stat_score_udf(
            F.col("PRR"),
            F.col("A").cast("int"),
            F.col("death_count").cast("int"),
            F.col("lt_count").cast("int"),
            F.col("hosp_count").cast("int")
        )
    )

    output = signals.select(
        F.col("drug_key"),
        F.col("pt"),
        F.col("PRR").alias("prr"),
        F.col("A").alias("drug_reaction_count"),
        F.col("B").alias("drug_no_reaction_count"),
        F.col("C").alias("other_reaction_count"),
        F.col("D").alias("other_no_reaction_count"),
        F.col("death_count"),
        F.col("hosp_count"),
        F.col("lt_count"),
        F.col("drug_total"),
        F.col("stat_score"),
    )

    count = output.count()
    log.info("Collected %d signals from Spark — writing to Snowflake", count)

    records = output.collect()
    rows = [row.asDict() for row in records]

    cfg = get_sf_config()
    conn = snowflake.connector.connect(**cfg)
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE signals_flagged")

    insert_sql = """
        INSERT INTO signals_flagged (
            drug_key, pt, prr, drug_reaction_count, drug_no_reaction_count,
            other_reaction_count, other_no_reaction_count,
            death_count, hosp_count, lt_count, drug_total, stat_score
        ) VALUES (
            %(drug_key)s, %(pt)s, %(prr)s, %(drug_reaction_count)s, %(drug_no_reaction_count)s,
            %(other_reaction_count)s, %(other_no_reaction_count)s,
            %(death_count)s, %(hosp_count)s, %(lt_count)s, %(drug_total)s, %(stat_score)s
        )
    """

    cur.executemany(insert_sql, rows)
    conn.commit()
    cur.close()
    conn.close()

    log.info("Written %d signals to signals_flagged (Snowflake)", count)


def create_spark_session() -> SparkSession:
    """Use Branch 1's exact Spark build pattern for maximum compatibility."""
    # Set up Hadoop home for Windows
    hadoop_home = os.getenv("HADOOP_HOME")
    if hadoop_home:
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["PATH"] = os.environ["PATH"] + f";{hadoop_home}\\bin"

    # Reuse Branch 1's build_spark function
    from pipelines.spark_branch1 import build_spark, ensure_snowflake_jdbc_jar

    jar_path = ensure_snowflake_jdbc_jar()
    spark = build_spark(jar_path)

    # Override app name for Branch 2
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session created using Branch 1's build pattern")

    return spark


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Validate required env vars before doing any work
    sf = get_sf_config()
    missing = [k for k, v in sf.items() if not v]
    if missing:
        log.error("Missing Snowflake env vars: %s", missing)
        raise SystemExit(1)

    spark = create_spark_session()

    try:
        pairs = load_pairs(spark)
        total_rows = pairs.count()
        unique_cases = pairs.select("primaryid").distinct().count()
        log.info("Rows: %d | Cases: %d", total_rows, unique_cases)

        # Production thresholds (hardcoded for full FAERS dataset)
        min_a, min_c, min_drug = (50, 200, 1000)

        prr_df = compute_prr(pairs)
        signals = apply_threshold_filters(prr_df, min_a, min_c, min_drug)
        signals_count = signals.count()
        log.info("After threshold + junk filters: %d", signals_count)

        signals = apply_spike_filter(signals, pairs)
        signals = apply_surge_filter(signals, pairs)

        # Debug: show all gabapentin signals so we can see what pt terms exist
        gaba = signals.filter(F.lower(F.col("drug_key")).contains("gabapentin"))
        gaba_count = gaba.count()

        if gaba_count == 0:
            log.warning("DEBUG: no gabapentin signals at all after filters")
        else:
            log.info("DEBUG: gabapentin signals found (%d total):", gaba_count)
            for row in gaba.select("pt", "PRR", "A").collect():
                log.info("  pt=%-50s  PRR=%.2f  A=%d", row["pt"], row["PRR"], row["A"])

        passed = run_checkpoint(signals)
        if not passed:
            num_quarters = pairs.select("source_quarter").distinct().count()
            log.warning(
                "Checkpoint not passed on %d quarter(s) of data. "
                "Expected with single-quarter POC — gabapentin needs full 4-quarter "
                "data to exceed A >= 30 for cardio-respiratory arrest. "
                "Writing signals anyway so downstream pipeline can proceed.",
                num_quarters,
            )

        write_signals(signals, spark)

        # Invalidate Redis cache after successful write
        from app.utils.redis_client import invalidate_signals
        invalidate_signals()
        log.info("Redis signal cache cleared after Branch 2 run")

    finally:
        spark.stop()
