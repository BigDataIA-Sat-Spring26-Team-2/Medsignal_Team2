"""
branch2_prr.py — Spark Branch 2: PRR Computation
Owner: Prachi
Run:   python pipelines/branch2_prr.py
"""

import os
import logging
import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, when, countDistinct
from pyspark.sql.functions import max as spark_max, sum as spark_sum

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
PG_URL = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
PG_PROPS = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver",
}

JUNK_TERMS = [
    "drug ineffective", "product use issue", "off label use", "off-label use",
    "drug interaction", "no adverse event", "product quality issue",
    "condition aggravated", "intentional product use issue",
    "product use in unapproved indication",
    "inappropriate schedule of product administration",
    "drug administered to patient of inappropriate age",
    "expired product administered", "wrong technique in product usage process",
]

# ── Spark ─────────────────────────────────────────────────────────────────────
def get_spark():
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] += ";C:\\hadoop\\bin"
    jar = os.path.abspath("jars/postgresql-42.6.0.jar")
    return (SparkSession.builder
        .appName("MedSignal-Branch2")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.driver.extraClassPath", jar)
        .config("spark.executor.extraClassPath", jar)
        .getOrCreate())

# ── PRR ───────────────────────────────────────────────────────────────────────
def run(spark):
    # Read
    pairs = spark.read.jdbc(PG_URL, "drug_reaction_pairs", properties=PG_PROPS)
    total_rows  = pairs.count()
    total_cases = pairs.select("primaryid").distinct().count()
    log.info("Rows: %d | Cases: %d", total_rows, total_cases)

    # Thresholds — relaxed if data is partial
    min_a, min_c, min_drug = (30, 100, 500) if total_rows < 1_000_000 else (50, 200, 1000)

    # Aggregations
    drug_totals     = pairs.groupBy("drug_key").agg(count("primaryid").alias("drug_total"))
    reaction_totals = pairs.groupBy("pt").agg(count("primaryid").alias("reaction_total"))
    a_counts = pairs.groupBy("drug_key", "pt").agg(
        count("primaryid").alias("A"),
        count(when(col("death_flag") == 1, 1)).alias("death_count"),
        count(when(col("hosp_flag")  == 1, 1)).alias("hosp_count"),
        count(when(col("lt_flag")    == 1, 1)).alias("lt_count"),
    )

    # PRR formula
    prr_df = (a_counts
        .join(drug_totals,     "drug_key")
        .join(reaction_totals, "pt")
        .withColumn("B", col("drug_total")     - col("A"))
        .withColumn("C", col("reaction_total") - col("A"))
        .withColumn("D", lit(total_cases) - col("drug_total") - col("reaction_total") + col("A"))
        .withColumn("PRR",
            when((col("C") > 0) & (col("C") + col("D") > 0) & (col("A") + col("B") > 0),
                (col("A") / (col("A") + col("B"))) / (col("C") / (col("C") + col("D")))
            ).otherwise(None))
        .filter(col("PRR").isNotNull()))

    # Filters
    signals = (prr_df
        .filter(col("A")          >= min_a)
        .filter(col("C")          >= min_c)
        .filter(col("drug_total") >= min_drug)
        .filter(col("PRR")        >= 2.0)
        .filter(~col("pt").isin(JUNK_TERMS)))

    log.info("Signals after filters: %d", signals.count())

    # Checkpoint
    fin = signals.filter(col("drug_key").contains("finasteride") & col("pt").contains("depression")).collect()
    if fin:
        log.info("CHECKPOINT PASSED — finasteride x depression PRR=%.2f A=%d", fin[0]["PRR"], fin[0]["A"])
    else:
        log.warning("CHECKPOINT: finasteride-depression not found — expected with partial data")

    # Write
    output = signals.select(
        col("drug_key"), col("pt"),
        col("PRR").alias("prr"),
        col("A").alias("drug_reaction_count"),
        col("B").alias("drug_no_reaction_count"),
        col("C").alias("other_reaction_count"),
        col("D").alias("other_no_reaction_count"),
        col("death_count"), col("hosp_count"), col("lt_count"), col("drug_total"),
    ).cache()

    final_count = output.count()

    # Truncate safely (FK constraints prevent DROP)
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"), port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"), user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    conn.cursor().execute("TRUNCATE TABLE signals_flagged CASCADE")
    conn.commit(); conn.close()

    output.write.jdbc(PG_URL, "signals_flagged", mode="append", properties=PG_PROPS)
    output.unpersist()
    log.info("Written %d signals to signals_flagged", final_count)

# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    spark = get_spark()
    run(spark)
    spark.stop()