"""
spark_branch1.py — MedSignal Spark Branch 1

Reads the four raw FAERS Kafka topics as static batch dataframes and
produces a clean drug_reaction_pairs table in PostgreSQL (Supabase).

Pipeline steps:
    1.  Load .env and build SparkSession
    2.  Download PostgreSQL JDBC jar if not present
    3.  Read four Kafka topics as static batch dataframes
    4.  Parse JSON values — all fields as StringType first, cast after
    5.  DEMO  → caseversion deduplication (window function, keep highest per caseid)
    6.  DRUG  → PS filter → combination drug split → RxNorm normalize → pair dedup
    7.  REAC  → deduplication on (primaryid, pt)
    8.  OUTC  → aggregate death/hosp/lt flags per primaryid
    9.  Four-file join → pair-level dedup → write to PostgreSQL
    10. Gabapentin validation checkpoint

Usage:
    # Single quarter test (recommended during development)
    poetry run python pipelines/spark_branch1.py --quarter 2023Q1

    # With row limit for quick smoke test
    poetry run python pipelines/spark_branch1.py --quarter 2023Q1 --limit 500000

    # Full single quarter run
    poetry run python pipelines/spark_branch1.py --quarter 2023Q1

Environment variables (loaded from .env):
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
    POSTGRES_USER, POSTGRES_PASSWORD,
    KAFKA_BOOTSTRAP_SERVERS
"""

import argparse
import os
import sys
import urllib.request
from pathlib import Path

import structlog
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Structlog — console renderer, same config as faers_prep.py
# ---------------------------------------------------------------------------

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="%H:%M:%S", utc=False),
        structlog.dev.ConsoleRenderer(colors=True),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(0),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

JDBC_DIR    = Path("drivers")
JDBC_JAR    = JDBC_DIR / "postgresql-42.7.3.jar"
JDBC_URL_DL = "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

# Tuned for local mode — default 200 is designed for large clusters
SHUFFLE_PARTITIONS = "8"

# PRR validation checkpoint — must appear in drug_reaction_pairs after Branch 1
CHECKPOINT_DRUG = "gabapentin"
CHECKPOINT_PT   = "cardio-respiratory arrest"


# ---------------------------------------------------------------------------
# JDBC jar download
# ---------------------------------------------------------------------------

def ensure_jdbc_jar() -> str:
    """
    Downloads the PostgreSQL JDBC jar if not already present.
    Returns the absolute path to the jar.
    """
    JDBC_DIR.mkdir(parents=True, exist_ok=True)

    if JDBC_JAR.exists():
        logger.info("jdbc_jar_ready", path=str(JDBC_JAR))
        return str(JDBC_JAR.resolve())

    logger.info("jdbc_jar_downloading", url=JDBC_URL_DL, dest=str(JDBC_JAR))
    try:
        urllib.request.urlretrieve(JDBC_URL_DL, JDBC_JAR)
        logger.info("jdbc_jar_downloaded", size_mb=round(JDBC_JAR.stat().st_size / 1e6, 1))
    except Exception as exc:
        logger.error("jdbc_jar_download_failed", error=str(exc))
        sys.exit(1)

    return str(JDBC_JAR.resolve())


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------

def build_spark(jdbc_jar_path: str):
    """
    Builds and returns a SparkSession configured for:
      - Local mode using all available cores
      - Kafka batch reading (via spark-sql-kafka package)
      - PostgreSQL writing (via JDBC jar)
    """
    hadoop_home = os.getenv("HADOOP_HOME")
    if hadoop_home:
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["PATH"]        = os.environ["PATH"] + f";{hadoop_home}\\bin"
    else:
        logger.warning("hadoop_home_not_set", hint="Set HADOOP_HOME in .env for Windows")


    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("MedSignal-Branch1")
        .master("local[*]")
        # Kafka connector — downloaded from Maven on first run, cached after
        .config("spark.jars.packages", KAFKA_PACKAGE)
        # PostgreSQL JDBC driver — pre-downloaded by ensure_jdbc_jar()
        .config("spark.jars", jdbc_jar_path)
        # Tuned for local mode — default 200 is for large clusters
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        # Increase driver memory for broadcast join and large dataframes
        .config("spark.driver.memory", "4g")
        # Suppress verbose Spark/Kafka INFO logs — keep console readable
        .config("spark.log.level", "WARN")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Kafka readers
# ---------------------------------------------------------------------------

def read_kafka_topic(
    spark,
    bootstrap_servers: str,
    topic: str,
    limit: int = None,
):
    """
    Reads a Kafka topic as a static batch dataframe.

    Uses spark.read (batch), not spark.readStream, because FAERS data
    is a fixed quarterly dataset — all records exist at read time.

    Args:
        limit: if provided, caps rows read per topic.
               Use during development to speed up test runs.
               Quarter filtering happens in the parse functions after
               JSON parsing, not here.
    """
    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) AS value")
    )

    if limit:
        df = df.limit(limit)

    return df


# ---------------------------------------------------------------------------
# JSON parsers — one per FAERS file type
# ---------------------------------------------------------------------------
# All fields are parsed as StringType first, then cast to the correct type.
# This is necessary because faers_prep.py publishes all values as JSON strings
# (e.g. "primaryid": "100640519") — from_json with LongType returns null.
#
# Quarter filtering is applied after JSON parsing so source_quarter is
# available as a typed column to filter on.

def parse_demo(spark, raw_df, quarter: str = None):
    """
    Parses the faers_demo topic into a typed dataframe.
    Optionally filters to a single quarter via source_quarter.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        IntegerType, LongType, StringType, StructField, StructType
    )

    schema = StructType([
        StructField("primaryid",      StringType(), True),
        StructField("caseid",         StringType(), True),
        StructField("caseversion",    StringType(), True),
        StructField("fda_dt",         StringType(), True),
        StructField("source_quarter", StringType(), True),
    ])

    df = (
        raw_df
        .select(F.from_json(F.col("value"), schema).alias("d"))
        .select("d.*")
        .withColumn("primaryid",   F.col("primaryid").cast(LongType()))
        .withColumn("caseid",      F.col("caseid").cast(LongType()))
        .withColumn("caseversion", F.col("caseversion").cast(IntegerType()))
        .filter(F.col("primaryid").isNotNull())
        .filter(F.col("caseid").isNotNull())
    )

    if quarter:
        df = df.filter(F.col("source_quarter") == quarter)

    return df


def parse_drug(spark, raw_df, quarter: str = None):
    """
    Parses the faers_drug topic into a typed dataframe.
    Optionally filters to a single quarter via source_quarter.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        LongType, StringType, StructField, StructType
    )

    schema = StructType([
        StructField("primaryid",      StringType(), True),
        StructField("caseid",         StringType(), True),
        StructField("role_cod",       StringType(), True),
        StructField("prod_ai",        StringType(), True),
        StructField("drugname",       StringType(), True),
        StructField("source_quarter", StringType(), True),
    ])

    df = (
        raw_df
        .select(F.from_json(F.col("value"), schema).alias("d"))
        .select("d.*")
        .withColumn("primaryid", F.col("primaryid").cast(LongType()))
        .withColumn("caseid",    F.col("caseid").cast(LongType()))
        .filter(F.col("primaryid").isNotNull())
    )

    if quarter:
        df = df.filter(F.col("source_quarter") == quarter)

    return df


def parse_reac(spark, raw_df, quarter: str = None):
    """
    Parses the faers_reac topic into a typed dataframe.
    Optionally filters to a single quarter via source_quarter.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        LongType, StringType, StructField, StructType
    )

    schema = StructType([
        StructField("primaryid",      StringType(), True),
        StructField("pt",             StringType(), True),
        StructField("source_quarter", StringType(), True),
    ])

    df = (
        raw_df
        .select(F.from_json(F.col("value"), schema).alias("d"))
        .select("d.*")
        .withColumn("primaryid", F.col("primaryid").cast(LongType()))
        .filter(F.col("primaryid").isNotNull())
        .filter(F.col("pt").isNotNull())
    )

    if quarter:
        df = df.filter(F.col("source_quarter") == quarter)

    return df


def parse_outc(spark, raw_df, quarter: str = None):
    """
    Parses the faers_outc topic into a typed dataframe.
    Optionally filters to a single quarter via source_quarter.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        LongType, StringType, StructField, StructType
    )

    schema = StructType([
        StructField("primaryid",      StringType(), True),
        StructField("outc_cod",       StringType(), True),
        StructField("source_quarter", StringType(), True),
    ])

    df = (
        raw_df
        .select(F.from_json(F.col("value"), schema).alias("d"))
        .select("d.*")
        .withColumn("primaryid", F.col("primaryid").cast(LongType()))
        .filter(F.col("primaryid").isNotNull())
    )

    if quarter:
        df = df.filter(F.col("source_quarter") == quarter)

    return df


# ---------------------------------------------------------------------------
# Transformation steps
# ---------------------------------------------------------------------------

def dedup_demo(demo_df):
    """
    Step 5: Caseversion deduplication on DEMO.

    FAERS cases are resubmitted across quarters as follow-up updates,
    each incrementing caseversion. Keep only the highest caseversion
    per caseid — this is the most recent authoritative version of the case.

    Uses a window function rather than groupBy+max because we need to
    keep the entire row, not just the max caseversion value.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    window = Window.partitionBy("caseid").orderBy(F.col("caseversion").desc())

    return (
        demo_df
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def filter_and_normalize_drug(drug_df, pg_conn_str: str, spark):
    """
    Step 6: PS filter, combination drug split, and RxNorm normalization.

    PS filter:
        Keep only role_cod = PS (Primary Suspect).
        Discards SS (Secondary Suspect), C (Concomitant), I (Interacting).

    Combination drug split (Option B — matches POC logic):
        e.g. "ACETAMINOPHEN\\HYDROCODONE" → "ACETAMINOPHEN"
        Takes the first component only. Applied to both prod_ai and drugname.

    RxNorm normalization via Spark join:
        Reads rxnorm_cache directly as a Spark dataframe and left joins.
        This carries rxcui through automatically — no Python dict intermediary.
        Drugs not in the cache get rxcui = NULL (correct per schema).

        Normalization hierarchy:
            1. canonical_name from cache if found
            2. prod_ai lowercased if not in cache
            3. drugname lowercased if prod_ai is null
    """
    from pyspark.sql import functions as F

    # Step 6a — PS filter
    drug_df = drug_df.filter(F.upper(F.col("role_cod")) == "PS")

    # Step 6b — Split combination drugs on backslash, take first component
    # e.g. "ACETAMINOPHEN\HYDROCODONE" → "ACETAMINOPHEN"
    drug_df = (
        drug_df
        .withColumn(
            "prod_ai",
            F.when(
                F.col("prod_ai").contains("\\"),
                F.split(F.col("prod_ai"), "\\\\").getItem(0),
            ).otherwise(F.col("prod_ai")),
        )
        .withColumn(
            "drugname",
            F.when(
                F.col("drugname").contains("\\"),
                F.split(F.col("drugname"), "\\\\").getItem(0),
            ).otherwise(F.col("drugname")),
        )
    )

    # Step 6c — Load rxnorm_cache via psycopg2 (reliable small table read)
    import psycopg2
    import pandas as pd

    conn = psycopg2.connect(pg_conn_str)
    cache_pd = pd.read_sql(
        "SELECT UPPER(TRIM(prod_ai)) as prod_ai_upper, canonical_name, rxcui "
        "FROM rxnorm_cache WHERE canonical_name IS NOT NULL",
        conn
    )
    conn.close()

    cache_df = spark.createDataFrame(cache_pd)

    logger.info(
        "rxnorm_cache_loaded",
        source="postgres",
        entries=len(cache_pd),
    )

    # Uppercase prod_ai for join key
    drug_df = drug_df.withColumn(
        "prod_ai_upper",
        F.upper(F.trim(F.col("prod_ai")))
    )

    # Left join — unmatched drugs get canonical_name = null, rxcui = null
    drug_df = drug_df.join(cache_df, on="prod_ai_upper", how="left")

    # Apply normalization hierarchy using native Spark functions
    # rxcui stays in the dataframe — null for unresolved drugs
    drug_df = drug_df.withColumn(
        "drug_key",
        F.when(
            F.col("canonical_name").isNotNull(),
            F.col("canonical_name"),                  # level 1: cache hit
        ).when(
            F.col("prod_ai").isNotNull(),
            F.lower(F.trim(F.col("prod_ai"))),        # level 2: prod_ai as-is
        ).otherwise(
            F.lower(F.trim(F.col("drugname")))        # level 3: drugname fallback
        )
    ).drop("prod_ai_upper", "canonical_name")

    return (
        drug_df
        .filter(F.col("drug_key").isNotNull())
        # One row per (primaryid, drug_key) — same drug reported twice counts once
        .dropDuplicates(["primaryid", "drug_key"])
    )


def dedup_reac(reac_df):
    """
    Step 7: REAC deduplication.

    Removes duplicate reaction terms per case.
    One row per (primaryid, pt) — same reaction reported twice counts once.
    pt is lowercased for consistent matching downstream.
    """
    from pyspark.sql import functions as F

    return (
        reac_df
        .withColumn("pt", F.lower(F.trim(F.col("pt"))))
        .dropDuplicates(["primaryid", "pt"])
    )


def aggregate_outc(outc_df):
    """
    Step 8: OUTC aggregation.

    OUTC has one row per outcome code per case. A case can have multiple
    outcomes (e.g. both DE and HO). Collapse to three binary flags per
    primaryid using max() — which acts as a logical OR across outcome codes.

    outc_cod values:
        DE → death_flag = 1
        HO → hosp_flag  = 1
        LT → lt_flag    = 1

    Left join in build_drug_reaction_pairs() ensures cases with no OUTC
    entry are preserved with all flags = 0 (~28% of FAERS cases).
    """
    from pyspark.sql import functions as F

    return (
        outc_df
        .withColumn("death_flag", F.when(F.upper(F.col("outc_cod")) == "DE", 1).otherwise(0))
        .withColumn("hosp_flag",  F.when(F.upper(F.col("outc_cod")) == "HO", 1).otherwise(0))
        .withColumn("lt_flag",    F.when(F.upper(F.col("outc_cod")) == "LT", 1).otherwise(0))
        .groupBy("primaryid")
        .agg(
            F.max("death_flag").alias("death_flag"),
            F.max("hosp_flag").alias("hosp_flag"),
            F.max("lt_flag").alias("lt_flag"),
        )
    )


def build_drug_reaction_pairs(demo_df, drug_df, reac_df, outc_df):
    """
    Step 9: Four-file join and pair-level deduplication.

    source_quarter exists in all four dataframes (injected by faers_prep.py).
    caseid exists in both DRUG and DEMO.
    Both are dropped from REAC/OUTC/DRUG before joining to avoid
    AMBIGUOUS_REFERENCE errors — DEMO is the authoritative source for both.

    Join order:
        DRUG inner join REAC  → one row per drug-reaction combination per case
        inner join DEMO       → adds caseid, fda_dt, source_quarter
        left join OUTC        → adds outcome flags

    Left join for OUTC is critical — ~28% of FAERS cases have no outcome
    recorded. An inner join would silently drop them and distort PRR values.

    Final dedup on (primaryid, drug_key, pt) gives one clean row per
    patient-drug-reaction triple — the drug_reaction_pairs table.
    rxcui is carried through from the RxNorm normalization join.
    """
    from pyspark.sql import functions as F

    # Drop ambiguous columns before joining
    # source_quarter: present in all four — keep from DEMO (authoritative)
    # caseid: present in both DRUG and DEMO — keep from DEMO (authoritative)
    drug_clean = drug_df.drop("caseid", "source_quarter")
    reac_clean = reac_df.drop("source_quarter")
    outc_clean = outc_df.drop("source_quarter")

    # DRUG inner join REAC on primaryid
    drug_reac = drug_clean.join(reac_clean, on="primaryid", how="inner")

    # inner join DEMO — brings in caseid, fda_dt, source_quarter
    drug_reac_demo = drug_reac.join(
        demo_df.select("primaryid", "caseid", "fda_dt", "source_quarter"),
        on="primaryid",
        how="inner",
    )

    # left join OUTC — preserves cases with no outcome recorded
    joined = drug_reac_demo.join(outc_clean, on="primaryid", how="left")

    # Fill null outcome flags with 0 (cases with no OUTC entry)
    joined = joined.fillna({"death_flag": 0, "hosp_flag": 0, "lt_flag": 0})

    # Pair-level deduplication — one clean row per patient-drug-reaction triple
    pairs = joined.dropDuplicates(["primaryid", "drug_key", "pt"])

    # Final column selection matching drug_reaction_pairs PostgreSQL schema
    # rxcui included — null for drugs not resolved by RxNorm cache
    return pairs.select(
        "primaryid",
        "caseid",
        "drug_key",
        "rxcui",
        "pt",
        # Cast fda_dt from "YYYYMMDD" string to DATE for PostgreSQL
        F.to_date(F.col("fda_dt"), "yyyyMMdd").alias("fda_dt"),
        F.col("death_flag").cast("integer"),
        F.col("hosp_flag").cast("integer"),
        F.col("lt_flag").cast("integer"),
        "source_quarter",
    )


# ---------------------------------------------------------------------------
# PostgreSQL writer
# ---------------------------------------------------------------------------

def write_to_postgres(df, table: str, pg_conn_str: str) -> int:
    """
    Writes a dataframe to PostgreSQL via psycopg2 + SQLAlchemy.
    More reliable than JDBC on Windows — no JVM DNS dependency.
    Uses to_sql with chunksize to avoid memory issues on large dataframes.
    """
    import psycopg2
    from sqlalchemy import create_engine

    logger.info("writing_to_postgres", table=table)

    # Convert Spark dataframe to pandas
    pandas_df = df.toPandas()

    # SQLAlchemy engine using psycopg2 as the underlying driver
    engine = create_engine(
        "postgresql+psycopg2://",
        creator=lambda: psycopg2.connect(pg_conn_str),
    )

    pandas_df.to_sql(
        name=table,
        con=engine,
        if_exists="append",
        index=False,
        chunksize=10000,   # write in batches to avoid memory pressure
        method="multi",    # use multi-row INSERT for speed
    )

    count = len(pandas_df)
    logger.info("write_complete", table=table, rows=count)
    return count


# ---------------------------------------------------------------------------
# Row count validation
# ---------------------------------------------------------------------------

def validate_row_counts(pairs_df, quarter: str = None) -> bool:
    """
    Confirms drug_reaction_pairs contains a plausible number of rows.

    Expected ranges:
        Single quarter : 900K  – 1.8M rows
        Full year      : 4M    – 6M   rows

    A count outside the expected range indicates a join error or
    a missing filter step.
    """
    count = pairs_df.count()

    if quarter:
        # Single quarter range — ~5M rows / 4 quarters = ~1.25M per quarter
        expected_min, expected_max = 900_000, 1_800_000
    else:
        expected_min, expected_max = 4_000_000, 6_000_000

    logger.info(
        "row_count_validation",
        rows=count,
        expected=f"{expected_min:,}–{expected_max:,}",
        mode="single_quarter" if quarter else "full_year",
    )

    if count < expected_min:
        logger.warning(
            "row_count_low",
            rows=count,
            hint="Possible join error or overly strict filter",
        )
        return False

    if count > expected_max:
        logger.warning(
            "row_count_high",
            rows=count,
            hint="Possible missing deduplication step",
        )
        return False

    logger.info("row_count_validation_passed", rows=count)
    return True


# ---------------------------------------------------------------------------
# PRR validation checkpoint
# ---------------------------------------------------------------------------

def run_validation_checkpoint(
    pg_conn_str: str,
    table: str = "drug_reaction_pairs",
) -> bool:
    """
    Confirms that gabapentin + cardio-respiratory arrest exists in
    drug_reaction_pairs after Branch 1 completes.

    Uses a direct psycopg2 COUNT(*) query — far more efficient than
    loading the entire table into Spark just to count matching rows.
    Runs in milliseconds on the indexed table.
    """
    import psycopg2

    logger.info(
        "validation_checkpoint_start",
        drug=CHECKPOINT_DRUG,
        reaction=CHECKPOINT_PT,
        table=table,
    )

    conn = psycopg2.connect(pg_conn_str)
    cur  = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*) FROM {table} WHERE drug_key = %s AND pt = %s",
        (CHECKPOINT_DRUG, CHECKPOINT_PT),
    )
    count = cur.fetchone()[0]
    cur.close()
    conn.close()

    if count > 0:
        logger.info(
            "validation_checkpoint_passed",
            drug=CHECKPOINT_DRUG,
            reaction=CHECKPOINT_PT,
            rows=count,
        )
        return True

    logger.error(
        "validation_checkpoint_failed",
        drug=CHECKPOINT_DRUG,
        reaction=CHECKPOINT_PT,
        hint=(
            "Check PS filter, combination drug split, RxNorm normalization, "
            "and join logic. Do not run Branch 2 until this passes."
        ),
    )
    return False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="MedSignal Spark Branch 1 — FAERS data engineering pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  # Single quarter (recommended during development)\n"
            "  poetry run python pipelines/spark_branch1.py --quarter 2023Q1\n\n"
            "  # Single quarter with row limit (quick smoke test)\n"
            "  poetry run python pipelines/spark_branch1.py --quarter 2023Q1 --limit 500000\n\n"
            "  # Full year (all quarters in Kafka)\n"
            "  poetry run python pipelines/spark_branch1.py\n"
        ),
    )
    parser.add_argument(
        "--quarter",
        type=str,
        default=None,
        help=(
            "Process a single quarter only (e.g. 2023Q1). "
            "Filters by source_quarter after JSON parsing. "
            "Omit when Kafka contains a single quarter already."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Limit rows read per Kafka topic (e.g. --limit 500000). "
            "Use during development to speed up test runs. "
            "Skips row count validation when set."
        ),
    )
    parser.add_argument(
        "--table",
        type=str,
        default="drug_reaction_pairs",
        help=(
            "Target PostgreSQL table to write to "
            "(default: drug_reaction_pairs). "
            "Use --table drug_reaction_pairs_duplicates for testing."
        ),
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    # ------------------------------------------------------------------
    # 1. Load environment variables
    # ------------------------------------------------------------------
    load_dotenv()

    postgres_host = os.getenv("POSTGRES_HOST")
    postgres_port = os.getenv("POSTGRES_PORT", "5432")
    postgres_db   = os.getenv("POSTGRES_DB")
    postgres_user = os.getenv("POSTGRES_USER")
    postgres_pass = os.getenv("POSTGRES_PASSWORD")
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # Validate required env vars are present
    missing = [
        k for k, v in {
            "POSTGRES_HOST":     postgres_host,
            "POSTGRES_DB":       postgres_db,
            "POSTGRES_USER":     postgres_user,
            "POSTGRES_PASSWORD": postgres_pass,
        }.items() if not v
    ]
    if missing:
        logger.error("missing_env_vars", vars=missing)
        sys.exit(1)

    # Supabase requires SSL — sslmode=require is mandatory
    jdbc_url = (
        f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
        f"?sslmode=require"
    )
    jdbc_props = {
        "user":     postgres_user,
        "password": postgres_pass,
        "driver":   "org.postgresql.Driver",
    }
    pg_conn_str = (
        f"host={postgres_host} "
        f"port={postgres_port} "
        f"dbname={postgres_db} "
        f"user={postgres_user} "
        f"password={postgres_pass} "
        f"sslmode=require"
    )

    logger.info(
        "branch1_start",
        kafka=kafka_servers,
        postgres_host=postgres_host,
        postgres_db=postgres_db,
        quarter=args.quarter or "all",
        limit=args.limit or "none",
        table=args.table,
    )

    # ------------------------------------------------------------------
    # 2. Ensure JDBC jar is present
    # ------------------------------------------------------------------
    jdbc_jar_path = ensure_jdbc_jar()

    # ------------------------------------------------------------------
    # 3. Build SparkSession
    # ------------------------------------------------------------------
    spark = build_spark(jdbc_jar_path)

    logger.info(
        "spark_session_ready",
        master="local[*]",
        shuffle_partitions=SHUFFLE_PARTITIONS,
    )

    # ------------------------------------------------------------------
    # 4. Read Kafka topics as static batch dataframes
    # ------------------------------------------------------------------
    logger.info("reading_kafka_topics", bootstrap_servers=kafka_servers)

    raw_demo = read_kafka_topic(spark, kafka_servers, "faers_demo", limit=args.limit)
    raw_drug = read_kafka_topic(spark, kafka_servers, "faers_drug", limit=args.limit)
    raw_reac = read_kafka_topic(spark, kafka_servers, "faers_reac", limit=args.limit)
    raw_outc = read_kafka_topic(spark, kafka_servers, "faers_outc", limit=args.limit)

    # ------------------------------------------------------------------
    # 5. Parse JSON — all fields StringType first, cast after
    #    Quarter filter applied here via source_quarter column
    # ------------------------------------------------------------------
    logger.info("parsing_json", quarter=args.quarter or "all")

    demo_df = parse_demo(spark, raw_demo, quarter=args.quarter)
    drug_df = parse_drug(spark, raw_drug, quarter=args.quarter)
    reac_df = parse_reac(spark, raw_reac, quarter=args.quarter)
    outc_df = parse_outc(spark, raw_outc, quarter=args.quarter)

    # ------------------------------------------------------------------
    # 6. DEMO — caseversion deduplication
    # ------------------------------------------------------------------
    logger.info("deduplicating_demo")
    demo_deduped = dedup_demo(demo_df)

    # ------------------------------------------------------------------
    # 7. DRUG — PS filter + combination split + RxNorm normalize
    #    Reads rxnorm_cache from PostgreSQL directly as Spark dataframe
    #    so rxcui is carried through the join automatically
    # ------------------------------------------------------------------
    logger.info("filtering_and_normalizing_drug")
    drug_normalized = filter_and_normalize_drug(drug_df, pg_conn_str, spark)

    # ------------------------------------------------------------------
    # 8. REAC — deduplication
    # ------------------------------------------------------------------
    logger.info("deduplicating_reac")
    reac_deduped = dedup_reac(reac_df)

    # ------------------------------------------------------------------
    # 9. OUTC — aggregate flags
    # ------------------------------------------------------------------
    logger.info("aggregating_outc_flags")
    outc_aggregated = aggregate_outc(outc_df)

    # ------------------------------------------------------------------
    # 10. Four-file join → pair-level dedup
    # ------------------------------------------------------------------
    logger.info("joining_four_files")
    pairs_df = build_drug_reaction_pairs(
        demo_deduped,
        drug_normalized,
        reac_deduped,
        outc_aggregated,
    )

    # Cache the pairs dataframe — used for both row count validation
    # and the PostgreSQL write. Without caching Spark recomputes the
    # entire join pipeline twice.
    pairs_df.cache()

    # ------------------------------------------------------------------
    # 11. Row count validation
    # ------------------------------------------------------------------
    if args.limit:
        logger.info(
            "row_count_validation_skipped",
            reason="--limit mode — partial dataset expected",
        )
    else:
        logger.info("validating_row_counts")
        validate_row_counts(pairs_df, quarter=args.quarter)

    # ------------------------------------------------------------------
    # 12. Write to PostgreSQL
    # ------------------------------------------------------------------
    write_to_postgres(pairs_df, args.table, pg_conn_str)

    # ------------------------------------------------------------------
    # 13. PRR validation checkpoint
    # ------------------------------------------------------------------
    passed = run_validation_checkpoint(pg_conn_str, table=args.table)

    if not passed:
        logger.error(
            "pipeline_halted",
            reason="PRR validation checkpoint failed",
            action="Fix data engineering layer before running Branch 2",
        )
        spark.stop()
        sys.exit(1)

    logger.info(
        "branch1_complete",
        quarter=args.quarter or "all",
        table=args.table,
        next_step="Run spark_branch2.py to compute PRR signals",
    )

    spark.stop()


if __name__ == "__main__":
    main()