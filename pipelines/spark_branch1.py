"""
spark_branch1.py — MedSignal Spark Branch 1

Reads the four raw FAERS Kafka topics as static batch dataframes and
produces a clean drug_reaction_pairs table in Snowflake.

Pipeline steps:
    1.  Load .env and build SparkSession
    2.  Download Snowflake JDBC jar if not present
    3.  Read four Kafka topics as static batch dataframes
    4.  Parse JSON values — all fields as StringType first, cast after
    5.  DEMO  → caseversion deduplication (window function, keep highest per caseid)
    6.  DRUG  → PS filter → combination drug split → RxNorm normalize → pair dedup
    7.  REAC  → deduplication on (primaryid, pt)
    8.  OUTC  → aggregate death/hosp/lt flags per primaryid
    9.  Four-file join → pair-level dedup → write to Snowflake
    10. Gabapentin validation checkpoint

Usage:
    # Single quarter test (recommended during development)
    poetry run python pipelines/spark_branch1.py --quarter 2023Q1

    # With row limit for quick smoke test
    poetry run python pipelines/spark_branch1.py --quarter 2023Q1 --limit 500000

    # Write to test table
    poetry run python pipelines/spark_branch1.py --quarter 2023Q1 --table drug_reaction_pairs_test

Environment variables (loaded from .env):
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE,
    KAFKA_BOOTSTRAP_SERVERS, HADOOP_HOME
"""

import argparse
import os
import sys
import urllib.request
from pathlib import Path

import pandas as pd
import psycopg2
import snowflake.connector
import structlog
from dotenv import load_dotenv
from sqlalchemy import create_engine

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

JDBC_DIR     = Path("drivers")
JDBC_JAR     = JDBC_DIR / "snowflake-jdbc-3.14.4.jar"
JDBC_URL_DL  = (
    "https://repo1.maven.org/maven2/net/snowflake/"
    "snowflake-jdbc/3.14.4/snowflake-jdbc-3.14.4.jar"
)

KAFKA_PACKAGE      = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
SNOWFLAKE_PACKAGE  = "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3"

# Tuned for local mode — default 200 is designed for large clusters
SHUFFLE_PARTITIONS = "8"

# PRR validation checkpoint — must appear in drug_reaction_pairs after Branch 1
CHECKPOINT_DRUG = "gabapentin"
CHECKPOINT_PT   = "cardio-respiratory arrest"


# ---------------------------------------------------------------------------
# Snowflake connection helper
# ---------------------------------------------------------------------------

def get_sf_conn(sf_config: dict) -> snowflake.connector.SnowflakeConnection:
    """
    Returns a snowflake-connector-python connection.
    Used for small table reads (rxnorm_cache) and point queries (checkpoint).
    Much faster than JDBC for small operations — no JVM overhead.
    """
    return snowflake.connector.connect(
        account  = sf_config["account"],
        user     = sf_config["user"],
        password = sf_config["password"],
        database = sf_config["database"],
        schema   = sf_config["schema"],
        warehouse= sf_config["warehouse"],
    )


# ---------------------------------------------------------------------------
# RxNorm cache loader
# ---------------------------------------------------------------------------

def load_rxnorm_cache(sf_config: dict, spark) -> None:
    """
    Loads the RxNorm cache from Snowflake rxnorm_cache table via
    snowflake-connector-python → pandas → Spark dataframe.

    Returns a Spark dataframe with columns:
        prod_ai_upper  (uppercased prod_ai for join key)
        canonical_name
        rxcui

    Using snowflake-connector-python instead of JDBC avoids JVM DNS
    issues and is much faster for a small 8,636-row table.
    """
    from pyspark.sql import functions as F

    logger.info("rxnorm_cache_loading", source="snowflake")

    conn = get_sf_conn(sf_config)
    cur  = conn.cursor()
    cur.execute(
        "SELECT UPPER(TRIM(prod_ai)) as prod_ai_upper, canonical_name, rxcui "
        "FROM rxnorm_cache WHERE canonical_name IS NOT NULL"
    )
    rows    = cur.fetchall()
    columns = [desc[0].lower() for desc in cur.description]
    cur.close()
    conn.close()

    cache_pd = pd.DataFrame(rows, columns=columns)
    cache_df = spark.createDataFrame(cache_pd)

    logger.info("rxnorm_cache_loaded", source="snowflake", entries=len(cache_pd))
    return cache_df


# ---------------------------------------------------------------------------
# JDBC jar download
# ---------------------------------------------------------------------------

def ensure_snowflake_jdbc_jar() -> str:
    """
    Downloads the Snowflake JDBC jar if not already present.
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
      - Snowflake writing (via Snowflake Spark connector + JDBC jar)
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
        .appName("MedSignal-Branch1-Snowflake")
        .master("local[*]")
        # Kafka + Snowflake connectors — downloaded from Maven on first run
        .config("spark.jars.packages", f"{KAFKA_PACKAGE},{SNOWFLAKE_PACKAGE}")
        # Snowflake JDBC jar — pre-downloaded by ensure_snowflake_jdbc_jar()
        .config("spark.jars", jdbc_jar_path)
        # Tuned for local mode — default 200 is for large clusters
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        # Increase driver memory for large dataframes
        .config("spark.driver.memory", "4g")
        # Suppress verbose Spark/Kafka INFO logs
        .config("spark.log.level", "WARN")
        # ── Fix heartbeat timeout for large datasets
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.network.timeout", "600s")
        # ── Fix Snowflake REST timeout
        .config("spark.executor.extraJavaOptions",
                "-Dnet.snowflake.jdbc.max_connections=3")
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
    Uses spark.read (batch) not spark.readStream — FAERS is a fixed quarterly dataset.
    """
    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("kafka.request.timeout.ms", "120000")       
        .option("kafka.session.timeout.ms", "120000")       
        .option("kafka.fetch.max.wait.ms",  "10000")        
        .option("kafkaConsumer.pollTimeoutMs", "300000")
        .load()
        .selectExpr("CAST(value AS STRING) AS value")
    )

    if limit:
        df = df.limit(limit)

    return df


# ---------------------------------------------------------------------------
# JSON parsers — one per FAERS file type
# ---------------------------------------------------------------------------
# All fields parsed as StringType first, then cast.
# faers_prep.py publishes all values as JSON strings —
# from_json with LongType would silently return null.

def parse_demo(spark, raw_df, quarter: str = None):
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
    Caseversion deduplication — keep highest caseversion per caseid.
    FAERS cases are resubmitted as follow-up updates across quarters.
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


def filter_and_normalize_drug(drug_df, cache_df, spark):
    """
    PS filter, combination drug split, and RxNorm normalization.

    PS filter: keep only role_cod = PS (Primary Suspect).
    Combination split: "ACETAMINOPHEN\\HYDROCODONE" → "ACETAMINOPHEN"
    RxNorm join: left join against rxnorm_cache — carries rxcui through.

    Normalization hierarchy:
        1. canonical_name from cache (if found)
        2. prod_ai lowercased (if not in cache)
        3. drugname lowercased (if prod_ai is null)
    """
    from pyspark.sql import functions as F

    # Step 1 — PS filter
    drug_df = drug_df.filter(F.upper(F.col("role_cod")) == "PS")

    # Step 2 — Split combination drugs on backslash, take first component
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

    # Step 3 — RxNorm join (cache_df already has prod_ai_upper column)
    drug_df = drug_df.withColumn(
        "prod_ai_upper",
        F.upper(F.trim(F.col("prod_ai")))
    )

    drug_df = drug_df.join(cache_df, on="prod_ai_upper", how="left")

    # Step 4 — Apply normalization hierarchy
    drug_df = drug_df.withColumn(
        "drug_key",
        F.when(
            F.col("canonical_name").isNotNull(),
            F.col("canonical_name"),
        ).when(
            F.col("prod_ai").isNotNull(),
            F.lower(F.trim(F.col("prod_ai"))),
        ).otherwise(
            F.lower(F.trim(F.col("drugname")))
        )
    ).drop("prod_ai_upper", "canonical_name")

    return (
        drug_df
        .filter(F.col("drug_key").isNotNull())
        .dropDuplicates(["primaryid", "drug_key"])
    )


def dedup_reac(reac_df):
    """One row per (primaryid, pt) — same reaction reported twice counts once."""
    from pyspark.sql import functions as F

    return (
        reac_df
        .withColumn("pt", F.lower(F.trim(F.col("pt"))))
        .dropDuplicates(["primaryid", "pt"])
    )


def aggregate_outc(outc_df):
    """
    Collapse OUTC to three binary flags per primaryid using max() as logical OR.
    DE → death_flag, HO → hosp_flag, LT → lt_flag.
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
    Four-file join and pair-level deduplication.

    DRUG inner REAC inner DEMO left OUTC.
    Left join for OUTC preserves ~28% of cases with no outcome recorded.
    Final dedup: one row per (primaryid, drug_key, pt).
    rxcui carried through from RxNorm join.
    """
    from pyspark.sql import functions as F

    # Drop ambiguous columns — keep from DEMO (authoritative)
    drug_clean = drug_df.drop("caseid", "source_quarter")
    reac_clean = reac_df.drop("source_quarter")
    outc_clean = outc_df.drop("source_quarter")

    drug_reac = drug_clean.join(reac_clean, on="primaryid", how="inner")

    drug_reac_demo = drug_reac.join(
        demo_df.select("primaryid", "caseid", "fda_dt", "source_quarter"),
        on="primaryid",
        how="inner",
    )

    joined = drug_reac_demo.join(outc_clean, on="primaryid", how="left")
    joined = joined.fillna({"death_flag": 0, "hosp_flag": 0, "lt_flag": 0})

    pairs = joined.dropDuplicates(["primaryid", "drug_key", "pt"])

    return pairs.select(
        "primaryid",
        "caseid",
        "drug_key",
        "rxcui",
        "pt",
        F.to_date(F.col("fda_dt"), "yyyyMMdd").alias("fda_dt"),
        F.col("death_flag").cast("integer"),
        F.col("hosp_flag").cast("integer"),
        F.col("lt_flag").cast("integer"),
        "source_quarter",
    )


# ---------------------------------------------------------------------------
# Snowflake writer
# ---------------------------------------------------------------------------

def write_to_snowflake(df, table: str, jdbc_url: str, jdbc_props: dict) -> int:
    """
    Writes a dataframe to Snowflake via Spark Snowflake JDBC connector.
    Uses append mode — composite PK (primaryid, drug_key, pt) prevents duplicates.
    Returns the row count written.
    """
    logger.info("writing_to_snowflake", table=table)

    df.write.jdbc(
        url=jdbc_url,
        table=table,
        mode="append",
        properties=jdbc_props,
    )

    count = df.count()
    logger.info("write_complete", table=table, rows=count)
    return count


# ---------------------------------------------------------------------------
# Row count validation
# ---------------------------------------------------------------------------

def validate_row_counts(pairs_df, quarter: str = None) -> bool:
    """
    Confirms drug_reaction_pairs contains a plausible number of rows.
    Single quarter: 900K–1.8M. Full year: 4M–6M.
    """
    count = pairs_df.count()

    if quarter:
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
        logger.warning("row_count_low", rows=count, hint="Possible join error or overly strict filter")
        return False
    if count > expected_max:
        logger.warning("row_count_high", rows=count, hint="Possible missing deduplication step")
        return False

    logger.info("row_count_validation_passed", rows=count)
    return True


# ---------------------------------------------------------------------------
# PRR validation checkpoint
# ---------------------------------------------------------------------------

def run_validation_checkpoint(sf_config: dict, table: str = "drug_reaction_pairs") -> bool:
    """
    Confirms gabapentin + cardio-respiratory arrest exists in drug_reaction_pairs.
    Uses direct Snowflake connector COUNT(*) — milliseconds on indexed table.
    Much faster than loading 1.4M+ rows into Spark to count a subset.
    """
    logger.info(
        "validation_checkpoint_start",
        drug=CHECKPOINT_DRUG,
        reaction=CHECKPOINT_PT,
        table=table,
    )

    conn = get_sf_conn(sf_config)
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
        description="MedSignal Spark Branch 1 — FAERS data engineering pipeline (Snowflake).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  poetry run python pipelines/spark_branch1.py --quarter 2023Q1\n"
            "  poetry run python pipelines/spark_branch1.py --quarter 2023Q1 --limit 500000\n"
            "  poetry run python pipelines/spark_branch1.py --quarter 2023Q1 --table drug_reaction_pairs_test\n"
        ),
    )
    parser.add_argument(
        "--quarter",
        type=str,
        default=None,
        help="Filter to single quarter e.g. 2023Q1. Omit when Kafka has one quarter only.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap rows per Kafka topic for smoke tests. Skips row count validation.",
    )
    parser.add_argument(
        "--table",
        type=str,
        default="drug_reaction_pairs",
        help="Target Snowflake table (default: drug_reaction_pairs).",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    load_dotenv()

    # ------------------------------------------------------------------
    # 1. Load and validate environment variables
    # ------------------------------------------------------------------
    sf_account   = os.getenv("SNOWFLAKE_ACCOUNT")
    sf_user      = os.getenv("SNOWFLAKE_USER")
    sf_password  = os.getenv("SNOWFLAKE_PASSWORD")
    sf_database  = os.getenv("SNOWFLAKE_DATABASE")
    sf_schema    = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
    sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    missing = [
        k for k, v in {
            "SNOWFLAKE_ACCOUNT":   sf_account,
            "SNOWFLAKE_USER":      sf_user,
            "SNOWFLAKE_PASSWORD":  sf_password,
            "SNOWFLAKE_DATABASE":  sf_database,
            "SNOWFLAKE_WAREHOUSE": sf_warehouse,
        }.items() if not v
    ]
    if missing:
        logger.error("missing_env_vars", vars=missing)
        sys.exit(1)

    sf_config = {
        "account":   sf_account,
        "user":      sf_user,
        "password":  sf_password,
        "database":  sf_database,
        "schema":    sf_schema,
        "warehouse": sf_warehouse,
    }

    # Snowflake JDBC URL and props for Spark write
    jdbc_url = (
        f"jdbc:snowflake://{sf_account}.snowflakecomputing.com/"
        f"?db={sf_database}&schema={sf_schema}&warehouse={sf_warehouse}"
    )
    jdbc_props = {
        "user":     sf_user,
        "password": sf_password,
        "driver":   "net.snowflake.client.jdbc.SnowflakeDriver",
    }

    logger.info(
        "branch1_start",
        kafka=kafka_servers,
        snowflake_account=sf_account,
        snowflake_database=sf_database,
        quarter=args.quarter or "all",
        limit=args.limit or "none",
        table=args.table,
    )

    # ------------------------------------------------------------------
    # 2. Ensure Snowflake JDBC jar is present
    # ------------------------------------------------------------------
    jdbc_jar_path = ensure_snowflake_jdbc_jar()

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
    # 4. Load RxNorm cache from Snowflake
    # ------------------------------------------------------------------
    cache_df = load_rxnorm_cache(sf_config, spark)

    # ------------------------------------------------------------------
    # 5. Read Kafka topics as static batch dataframes
    # ------------------------------------------------------------------
    logger.info("reading_kafka_topics", bootstrap_servers=kafka_servers)

    raw_demo = read_kafka_topic(spark, kafka_servers, "faers_demo", limit=args.limit)
    raw_drug = read_kafka_topic(spark, kafka_servers, "faers_drug", limit=args.limit)
    raw_reac = read_kafka_topic(spark, kafka_servers, "faers_reac", limit=args.limit)
    raw_outc = read_kafka_topic(spark, kafka_servers, "faers_outc", limit=args.limit)

    # ------------------------------------------------------------------
    # 6. Parse JSON — all fields StringType first, cast after
    # ------------------------------------------------------------------
    logger.info("parsing_json", quarter=args.quarter or "all")

    demo_df = parse_demo(spark, raw_demo, quarter=args.quarter)
    drug_df = parse_drug(spark, raw_drug, quarter=args.quarter)
    reac_df = parse_reac(spark, raw_reac, quarter=args.quarter)
    outc_df = parse_outc(spark, raw_outc, quarter=args.quarter)

    # ------------------------------------------------------------------
    # 7. DEMO — caseversion deduplication
    # ------------------------------------------------------------------
    logger.info("deduplicating_demo")
    demo_deduped = dedup_demo(demo_df)

    # ------------------------------------------------------------------
    # 8. DRUG — PS filter + combination split + RxNorm normalize
    # ------------------------------------------------------------------
    logger.info("filtering_and_normalizing_drug")
    drug_normalized = filter_and_normalize_drug(drug_df, cache_df, spark)

    # ------------------------------------------------------------------
    # 9. REAC — deduplication
    # ------------------------------------------------------------------
    logger.info("deduplicating_reac")
    reac_deduped = dedup_reac(reac_df)

    # ------------------------------------------------------------------
    # 10. OUTC — aggregate flags
    # ------------------------------------------------------------------
    logger.info("aggregating_outc_flags")
    outc_aggregated = aggregate_outc(outc_df)

    # ------------------------------------------------------------------
    # 11. Four-file join → pair-level dedup
    # ------------------------------------------------------------------
    logger.info("joining_four_files")
    pairs_df = build_drug_reaction_pairs(
        demo_deduped,
        drug_normalized,
        reac_deduped,
        outc_aggregated,
    )

    # Cache — used for both row count validation and write
    pairs_df.cache()

    # ------------------------------------------------------------------
    # 12. Row count validation
    # ------------------------------------------------------------------
    if args.limit:
        logger.info("row_count_validation_skipped", reason="--limit mode — partial dataset expected")
    else:
        logger.info("validating_row_counts")
        validate_row_counts(pairs_df, quarter=args.quarter)

    # ------------------------------------------------------------------
    # 13. Write to Snowflake
    # ------------------------------------------------------------------
    write_to_snowflake(pairs_df, args.table, jdbc_url, jdbc_props)

    # ------------------------------------------------------------------
    # 14. PRR validation checkpoint
    # ------------------------------------------------------------------
    passed = run_validation_checkpoint(sf_config, table=args.table)

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
        next_step="Run branch2_prr.py to compute PRR signals",
    )

    spark.stop()


if __name__ == "__main__":
    main()