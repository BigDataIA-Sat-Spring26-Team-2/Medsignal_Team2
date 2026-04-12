"""
spark_branch1.py — MedSignal Spark Branch 1

Reads the four raw FAERS Kafka topics as static batch dataframes and
produces a clean drug_reaction_pairs table in PostgreSQL (Supabase).

Pipeline steps:
    1. Load .env and build SparkSession
    2. Download PostgreSQL JDBC jar if not present
    3. Load RxNorm cache from PostgreSQL → broadcast (mock data for now)
    4. Read four Kafka topics as static batch dataframes
    5. Parse JSON values from Kafka binary format
    6. DEMO  → caseversion deduplication
    7. DRUG  → PS filter → RxNorm normalize → pair deduplication
    8. REAC  → deduplication on (primaryid, pt)
    9. OUTC  → aggregate death/hosp/lt flags per primaryid
    10. Four-file join → pair-level dedup → write to PostgreSQL
    11. PRR validation checkpoint

Usage:
    poetry run python pipelines/spark_branch1.py

Environment variables (loaded from .env):
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
    POSTGRES_USER, POSTGRES_PASSWORD,
    KAFKA_BOOTSTRAP_SERVERS
"""

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

JDBC_DIR     = Path("drivers")
JDBC_JAR     = JDBC_DIR / "postgresql-42.7.3.jar"
JDBC_URL_DL  = (
    "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"
)

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

# FAERS encoding — confirmed from direct file inspection
DELIMITER = "$"
ENCODING  = "latin-1"

# Spark shuffle partitions — tuned for local mode on a laptop.
# Default of 200 is designed for large clusters and kills local performance.
SHUFFLE_PARTITIONS = "8"

# PRR validation checkpoint — gabapentin + cardiorespiratory arrest
# must appear in drug_reaction_pairs after Branch 1 completes.
CHECKPOINT_DRUG = "gabapentin"
CHECKPOINT_PT   = "cardiorespiratory arrest"

# ---------------------------------------------------------------------------
# Mock RxNorm cache
# ---------------------------------------------------------------------------

# Keyed on upper-cased prod_ai value (as it appears in FAERS DRUG file).
# Value is the canonical drug name Branch 1 will write as drug_key.
#
# Structure matches exactly what the real RxNorm cache builder will produce
# in the rxnorm_cache PostgreSQL table:
#   prod_ai (PK) → canonical_name
#
# When your teammate's rxnorm_cache builder is ready, replace
# build_mock_rxnorm_cache() with load_rxnorm_cache_from_postgres()
# below — Branch 1 code does not need any other changes.

MOCK_RXNORM: dict[str, str] = {
    # Golden signal drugs
    "DUPILUMAB":     "dupilumab",
    "DUPIXENT":      "dupilumab",
    "GABAPENTIN":    "gabapentin",
    "NEURONTIN":     "gabapentin",
    "PREGABALIN":    "pregabalin",
    "LYRICA":        "pregabalin",
    "LEVETIRACETAM": "levetiracetam",
    "KEPPRA":        "levetiracetam",
    "TIRZEPATIDE":   "tirzepatide",
    "MOUNJARO":      "tirzepatide",
    "SEMAGLUTIDE":   "semaglutide",
    "OZEMPIC":       "semaglutide",
    "WEGOVY":        "semaglutide",
    "EMPAGLIFLOZIN": "empagliflozin",
    "JARDIANCE":     "empagliflozin",
    "BUPROPION":     "bupropion",
    "WELLBUTRIN":    "bupropion",
    "ZYBAN":         "bupropion",
    "DAPAGLIFLOZIN": "dapagliflozin",
    "FARXIGA":       "dapagliflozin",
    "METFORMIN":     "metformin",
    "GLUCOPHAGE":    "metformin",
    # Common drugs that appear frequently in FAERS — helps PRR denominator
    "ASPIRIN":       "aspirin",
    "ACETYLSALICYLIC ACID": "aspirin",
    "LISINOPRIL":    "lisinopril",
    "ATORVASTATIN":  "atorvastatin",
    "LIPITOR":       "atorvastatin",
    "METOPROLOL":    "metoprolol",
    "AMLODIPINE":    "amlodipine",
    "OMEPRAZOLE":    "omeprazole",
    "LOSARTAN":      "losartan",
    "SIMVASTATIN":   "simvastatin",
    "ZOCOR":         "simvastatin",
}


def build_mock_rxnorm_cache() -> dict[str, str]:
    """
    Returns the mock RxNorm cache as a plain Python dict.
    Keys are upper-cased prod_ai values. Values are canonical drug names.

    Replace this function with load_rxnorm_cache_from_postgres() once
    the real cache is built by your teammate.
    """
    logger.info(
        "rxnorm_cache_loaded",
        source="mock",
        entries=len(MOCK_RXNORM),
        note="Replace with load_rxnorm_cache_from_postgres() when ready",
    )
    return MOCK_RXNORM


def load_rxnorm_cache_from_postgres(jdbc_url: str, props: dict) -> dict[str, str]:
    """
    Loads the real RxNorm cache from the rxnorm_cache PostgreSQL table.
    Returns a plain Python dict: {prod_ai_upper -> canonical_name}

    Uncomment and call this instead of build_mock_rxnorm_cache() once
    your teammate's cache builder has populated the table.

    Args:
        jdbc_url: JDBC connection URL
        props:    connection properties dict with user/password/driver
    """
    # from pyspark.sql import SparkSession
    # spark = SparkSession.getActiveSession()
    # df = spark.read.jdbc(url=jdbc_url, table="rxnorm_cache", properties=props)
    # rows = df.select("prod_ai", "canonical_name").collect()
    # cache = {r["prod_ai"].upper(): r["canonical_name"] for r in rows}
    # logger.info("rxnorm_cache_loaded", source="postgres", entries=len(cache))
    # return cache
    raise NotImplementedError("RxNorm cache not yet built — use build_mock_rxnorm_cache()")


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
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"
    
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
        # Increase driver memory for broadcast join of RxNorm cache
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

# def read_kafka_topic(spark, bootstrap_servers: str, topic: str):
#     """
#     Reads a Kafka topic as a static batch dataframe.
#     Returns a dataframe with columns: value (binary), source_topic.

#     Uses spark.read (batch), not spark.readStream, because FAERS data
#     is a fixed quarterly dataset — all records exist at read time.
#     """
#     return (
#         spark.read
#         .format("kafka")
#         .option("kafka.bootstrap.servers", bootstrap_servers)
#         .option("subscribe", topic)
#         # Read from the very beginning of the topic
#         .option("startingOffsets", "earliest")
#         .option("endingOffsets", "latest")
#         # Allow reading from a topic with no new messages
#         .option("failOnDataLoss", "false")
#         .load()
#         .selectExpr("CAST(value AS STRING) AS value")
#     )


# for testing purposes, we can add a limit to the number of rows read from Kafka.
def read_kafka_topic(spark, bootstrap_servers: str, topic: str, limit: int = None):
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

def parse_demo(spark, raw_df):
    """
    Parses the faers_demo topic into a typed dataframe.
    Schema matches DEMO*.txt fields used by Branch 1.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        LongType, StringType, IntegerType, StructField, StructType
    )

    schema = StructType([
    StructField("primaryid",      StringType(), True),
    StructField("caseid",         StringType(), True),
    StructField("caseversion",    StringType(), True),
    StructField("fda_dt",         StringType(), True),
    StructField("source_quarter", StringType(), True),
    ])

    return (
        raw_df
        .select(F.from_json(F.col("value"), schema).alias("d"))
        .select("d.*")
        .withColumn("primaryid",   F.col("primaryid").cast(LongType()))
        .withColumn("caseid",      F.col("caseid").cast(LongType()))
        .withColumn("caseversion", F.col("caseversion").cast(IntegerType()))
        .filter(F.col("primaryid").isNotNull())
        .filter(F.col("caseid").isNotNull())
    )


def parse_drug(spark, raw_df):
    """
    Parses the faers_drug topic into a typed dataframe.
    Schema includes role_cod and prod_ai for PS filter and normalization.
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

    return (
        raw_df
        .select(F.from_json(F.col("value"), schema).alias("d"))
        .select("d.*")
        .withColumn("primaryid", F.col("primaryid").cast(LongType()))
        .withColumn("caseid",    F.col("caseid").cast(LongType()))
        .filter(F.col("primaryid").isNotNull())
    )


def parse_reac(spark, raw_df):
    """
    Parses the faers_reac topic into a typed dataframe.
    pt is the MedDRA preferred term for the adverse reaction.
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

    return (
        raw_df
        .select(F.from_json(F.col("value"), schema).alias("d"))
        .select("d.*")
        .withColumn("primaryid", F.col("primaryid").cast(LongType()))
        .filter(F.col("primaryid").isNotNull())
        .filter(F.col("pt").isNotNull())
    )


def parse_outc(spark, raw_df):
    """
    Parses the faers_outc topic into a typed dataframe.
    outc_cod values: DE=death, HO=hospitalization, LT=life-threatening.
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

    return (
        raw_df
        .select(F.from_json(F.col("value"), schema).alias("d"))
        .select("d.*")
        .withColumn("primaryid", F.col("primaryid").cast(LongType()))
        .filter(F.col("primaryid").isNotNull())
    )


# ---------------------------------------------------------------------------
# Transformation steps
# ---------------------------------------------------------------------------

def dedup_demo(demo_df):
    """
    Step 2: Caseversion deduplication on DEMO.
    Keeps the highest caseversion per caseid, removing cross-quarter
    follow-up updates from the case count.
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


def filter_and_normalize_drug(drug_df, rxnorm_broadcast):
    """
    Steps 3 & 4: PS filter and RxNorm normalization.

    PS filter: keep only Primary Suspect drugs (role_cod = PS).
    Discards Secondary Suspect (SS), Concomitant (C), Interacting (I).

    Normalization hierarchy:
      1. prod_ai uppercased → lookup in RxNorm cache → canonical_name
      2. If not found → use prod_ai as-is (lowercased)
      3. If prod_ai is null → use drugname as-is (lowercased)

    The RxNorm cache is broadcast so every Spark executor has a local
    copy, avoiding repeated PostgreSQL lookups during the join.
    """
    from pyspark.sql import functions as F

    # UDF that applies the three-level normalization hierarchy
    def normalize(prod_ai: str, drugname: str) -> str:
        cache = rxnorm_broadcast.value
        if prod_ai:
            key = prod_ai.strip().upper()
            if key in cache:
                return cache[key]
            return prod_ai.strip().lower()
        if drugname:
            return drugname.strip().lower()
        return None

    normalize_udf = F.udf(normalize)

    return (
        drug_df
        # Step 3: PS filter
        .filter(F.upper(F.col("role_cod")) == "PS")
        # Step 4: Normalize drug name → drug_key
        .withColumn(
            "drug_key",
            normalize_udf(F.col("prod_ai"), F.col("drugname")),
        )
        .filter(F.col("drug_key").isNotNull())
        # Pair-level dedup: one row per (primaryid, drug_key)
        .dropDuplicates(["primaryid", "drug_key"])
    )


def dedup_reac(reac_df):
    """
    Step 5: REAC deduplication.
    Removes duplicate reaction terms per case — one row per (primaryid, pt).
    pt values are lowercased for consistent matching.
    """
    from pyspark.sql import functions as F

    return (
        reac_df
        .withColumn("pt", F.lower(F.trim(F.col("pt"))))
        .dropDuplicates(["primaryid", "pt"])
    )


def aggregate_outc(outc_df):
    """
    Step 6: OUTC aggregation.
    Produces three binary flags per primaryid using max() groupBy.
    Left join in step 7 ensures cases with no OUTC row are preserved
    with all flags = 0.

    outc_cod values:
        DE → death_flag
        HO → hosp_flag
        LT → lt_flag
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
    Step 7: Four-file join and pair-level deduplication.

    Join order:
        DRUG inner join REAC on primaryid
        inner join DEMO on primaryid
        left join OUTC on primaryid

    Left join for OUTC preserves the ~28% of cases with no outcome
    recorded. Null outcome flags are filled with 0.

    Final dedup on (primaryid, drug_key, pt) gives one clean row per
    patient-drug-reaction triple — the drug_reaction_pairs table.
    """
    from pyspark.sql import functions as F

    # source_quarter appears in all four dataframes — keep only from DEMO
    # caseid appears in both DRUG and DEMO — keep only from DEMO
    # Drop both from REAC and OUTC before joining to avoid ambiguity

    drug_clean = drug_df.drop("caseid")
    reac_clean = reac_df.drop("source_quarter")
    outc_clean = outc_df.drop("source_quarter")

     # DRUG inner join REAC
    drug_reac = drug_clean.join(reac_clean, on="primaryid", how="inner")

    # Drop source_quarter from drug_reac — DEMO is the authoritative source
    drug_reac = drug_reac.drop("source_quarter")

    # inner join DEMO — brings in caseid, fda_dt, source_quarter
    drug_reac_demo = drug_reac.join(
        demo_df.select("primaryid", "caseid", "fda_dt", "source_quarter"),
        on="primaryid",
        how="inner",
    )

    # left join OUTC — preserves cases with no outcome
    joined = drug_reac_demo.join(
        outc_clean,
        on="primaryid",
        how="left",
    )

    # Fill null outcome flags with 0
    joined = joined.fillna({"death_flag": 0, "hosp_flag": 0, "lt_flag": 0})

    # Pair-level deduplication — one row per (primaryid, drug_key, pt)
    pairs = joined.dropDuplicates(["primaryid", "drug_key", "pt"])

    # Select and order final columns matching drug_reaction_pairs schema
    return pairs.select(
        "primaryid",
        "caseid",
        "drug_key",
        "pt",
        F.to_date(F.col("fda_dt"), "yyyyMMdd").alias("fda_dt"),  # ← add this cast
        F.col("death_flag").cast("integer"),
        F.col("hosp_flag").cast("integer"),
        F.col("lt_flag").cast("integer"),
        "source_quarter",
    )

# ---------------------------------------------------------------------------
# PostgreSQL writer
# ---------------------------------------------------------------------------

def write_to_postgres(df, table: str, jdbc_url: str, props: dict) -> int:
    """
    Writes a dataframe to PostgreSQL via JDBC.
    Uses append mode — safe to re-run since drug_reaction_pairs has a
    composite PK (primaryid, drug_key, pt) that prevents duplicates.
    Returns the row count written.
    """
    logger.info("writing_to_postgres", table=table)

    df.write.jdbc(
        url=jdbc_url,
        table=table,
        mode="append",
        properties=props,
    )

    count = df.count()
    logger.info("write_complete", table=table, rows=count)
    return count


# ---------------------------------------------------------------------------
# PRR validation checkpoint
# ---------------------------------------------------------------------------

def run_validation_checkpoint(spark, jdbc_url: str, props: dict) -> bool:
    """
    Confirms that the gabapentin + cardiorespiratory arrest pair exists
    in drug_reaction_pairs with at least 1 row.

    This is the pipeline correctness gate — if it fails, something in the
    data engineering layer is wrong and Branch 2 should not run.
    """
    logger.info(
        "validation_checkpoint_start",
        drug=CHECKPOINT_DRUG,
        reaction=CHECKPOINT_PT,
    )

    df = (
        spark.read.jdbc(
            url=jdbc_url,
            table="drug_reaction_pairs",
            properties=props,
        )
        .filter(
            (spark_col("drug_key") == CHECKPOINT_DRUG) &
            (spark_col("pt") == CHECKPOINT_PT)
        )
    )

    count = df.count()

    if count > 0:
        logger.info(
            "validation_checkpoint_passed",
            drug=CHECKPOINT_DRUG,
            reaction=CHECKPOINT_PT,
            rows=count,
        )
        return True
    else:
        logger.error(
            "validation_checkpoint_failed",
            drug=CHECKPOINT_DRUG,
            reaction=CHECKPOINT_PT,
            hint=(
                "Check PS filter, RxNorm normalization, and join logic. "
                "Do not run Branch 2 until this passes."
            ),
        )
        return False


# ---------------------------------------------------------------------------
# Row count validation
# ---------------------------------------------------------------------------

def validate_row_counts(pairs_df) -> bool:
    """
    Confirms drug_reaction_pairs contains a plausible number of rows
    for full 2023 data. Expected range: 4M to 6M rows.
    A count outside this range suggests a join error or missing filter.
    """
    count = pairs_df.count()
    expected_min = 4_000_000
    expected_max = 6_000_000

    logger.info("row_count_validation", rows=count, expected=f"{expected_min:,}–{expected_max:,}")

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


def debug_transforms(demo_df, drug_df, reac_df, outc_df,
                     demo_deduped, drug_normalized, reac_deduped, outc_aggregated):
    print("\n=== DEBUG: Row counts after each step ===")
    print(f"DEMO raw:        {demo_df.count()}")
    print(f"DEMO deduped:    {demo_deduped.count()}")
    print(f"DRUG raw:        {drug_df.count()}")
    print(f"DRUG normalized: {drug_normalized.count()}")
    print(f"REAC raw:        {reac_df.count()}")
    print(f"REAC deduped:    {reac_deduped.count()}")
    print(f"OUTC raw:        {outc_df.count()}")
    print(f"OUTC aggregated: {outc_aggregated.count()}")

    print("\n=== DEBUG: Sample primaryids from each ===")
    print("DEMO primaryids:")
    demo_deduped.select("primaryid").show(5, truncate=False)
    print("DRUG primaryids:")
    drug_normalized.select("primaryid", "drug_key").show(5, truncate=False)
    print("REAC primaryids:")
    reac_deduped.select("primaryid", "pt").show(5, truncate=False)

    print("\n=== DEBUG: Schema check ===")
    print("DEMO schema:"); demo_deduped.printSchema()
    print("DRUG schema:"); drug_normalized.printSchema()

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
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

    # Validate required env vars
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

    jdbc_url = (
        f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
        f"?sslmode=require"
    )
    jdbc_props = {
        "user":     postgres_user,
        "password": postgres_pass,
        "driver":   "org.postgresql.Driver",
    }

    logger.info(
        "branch1_start",
        kafka=kafka_servers,
        postgres_host=postgres_host,
        postgres_db=postgres_db,
    )

    # ------------------------------------------------------------------
    # 2. Ensure JDBC jar is present
    # ------------------------------------------------------------------
    jdbc_jar_path = ensure_jdbc_jar()

    # ------------------------------------------------------------------
    # 3. Build SparkSession
    # ------------------------------------------------------------------
    spark = build_spark(jdbc_jar_path)

    # Import F here so it's available after Spark is initialized
    from pyspark.sql import functions as F
    # Make col available for validation checkpoint
    global spark_col
    spark_col = F.col

    logger.info("spark_session_ready", master="local[*]", shuffle_partitions=SHUFFLE_PARTITIONS)

    # ------------------------------------------------------------------
    # 4. Load RxNorm cache and broadcast
    # ------------------------------------------------------------------
    rxnorm_cache   = build_mock_rxnorm_cache()
    rxnorm_bcast   = spark.sparkContext.broadcast(rxnorm_cache)

    # ------------------------------------------------------------------
    # 5. Read Kafka topics as static batch dataframes
    # ------------------------------------------------------------------
    logger.info("reading_kafka_topics", bootstrap_servers=kafka_servers)

    # raw_demo = read_kafka_topic(spark, kafka_servers, "faers_demo")
    # raw_drug = read_kafka_topic(spark, kafka_servers, "faers_drug")
    # raw_reac = read_kafka_topic(spark, kafka_servers, "faers_reac")
    # raw_outc = read_kafka_topic(spark, kafka_servers, "faers_outc")


    args = parse_args()  # add at top of main()

    # added limit to read_kafka_topic() calls for testing
    raw_demo = read_kafka_topic(spark, kafka_servers, "faers_demo", args.limit)
    raw_drug = read_kafka_topic(spark, kafka_servers, "faers_drug", args.limit)
    raw_reac = read_kafka_topic(spark, kafka_servers, "faers_reac", args.limit)
    raw_outc = read_kafka_topic(spark, kafka_servers, "faers_outc", args.limit)

    # ------------------------------------------------------------------
    # 6. Parse JSON values
    # ------------------------------------------------------------------
    logger.info("parsing_json")

    demo_df = parse_demo(spark, raw_demo)
    drug_df = parse_drug(spark, raw_drug)
    reac_df = parse_reac(spark, raw_reac)
    outc_df = parse_outc(spark, raw_outc)

    # ------------------------------------------------------------------
    # 7. DEMO — caseversion deduplication
    # ------------------------------------------------------------------
    logger.info("deduplicating_demo")
    demo_deduped = dedup_demo(demo_df)

    # ------------------------------------------------------------------
    # 8. DRUG — PS filter + RxNorm normalize
    # ------------------------------------------------------------------
    logger.info("filtering_and_normalizing_drug")
    drug_normalized = filter_and_normalize_drug(drug_df, rxnorm_bcast)

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

    debug_transforms(demo_df, drug_df, reac_df, outc_df,
                 demo_deduped, drug_normalized, reac_deduped, outc_aggregated)
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

    # Cache the pairs dataframe — it's used for both row count validation
    # and the PostgreSQL write, so we don't want to recompute it twice.
    pairs_df.cache()

    # ------------------------------------------------------------------
    # 12. Row count validation
    # ------------------------------------------------------------------
    logger.info("validating_row_counts")
    validate_row_counts(pairs_df)

    # ------------------------------------------------------------------
    # 13. Write to PostgreSQL
    # ------------------------------------------------------------------
    write_to_postgres(pairs_df, "drug_reaction_pairs", jdbc_url, jdbc_props)

    # ------------------------------------------------------------------
    # 14. PRR validation checkpoint
    # ------------------------------------------------------------------
    passed = run_validation_checkpoint(spark, jdbc_url, jdbc_props)

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
        next_step="Run spark_branch2.py to compute PRR signals",
    )

    spark.stop()

# For testing
import argparse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit rows per Kafka topic for testing (e.g. --limit 10000)",
    )
    return parser.parse_args()

if __name__ == "__main__":
    main()