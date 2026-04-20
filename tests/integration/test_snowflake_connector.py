"""
tests/integration/test_snowflake_connector.py — Snowflake + Spark connectivity test

Tests three things in sequence:
    1. snowflake-connector-python can connect and run a query
    2. Spark can write a small dataframe to Snowflake via JDBC
    3. Spark can read back what was written

Run before committing to a Snowflake migration.

Usage:
    poetry run python tests/integration/test_snowflake_connector.py

Required .env variables:
    SNOWFLAKE_ACCOUNT    e.g. abc12345.us-east-1
    SNOWFLAKE_USER
    SNOWFLAKE_PASSWORD
    SNOWFLAKE_DATABASE
    SNOWFLAKE_SCHEMA
    SNOWFLAKE_WAREHOUSE
    HADOOP_HOME          (Windows only)
"""

import os
import sys
from pathlib import Path

import structlog
from dotenv import load_dotenv

load_dotenv()

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

SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")

JDBC_DIR     = Path("drivers")
JDBC_JAR     = JDBC_DIR / "snowflake-jdbc-3.14.4.jar"
JDBC_URL_DL  = (
    "https://repo1.maven.org/maven2/net/snowflake/"
    "snowflake-jdbc/3.14.4/snowflake-jdbc-3.14.4.jar"
)

SNOWFLAKE_SPARK_PACKAGE = "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3"
KAFKA_PACKAGE           = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

TEST_TABLE = "medsignal_connector_test"


def validate_env() -> bool:
    required = {
        "SNOWFLAKE_ACCOUNT":   SNOWFLAKE_ACCOUNT,
        "SNOWFLAKE_USER":      SNOWFLAKE_USER,
        "SNOWFLAKE_PASSWORD":  SNOWFLAKE_PASSWORD,
        "SNOWFLAKE_DATABASE":  SNOWFLAKE_DATABASE,
        "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        logger.error("missing_env_vars", vars=missing)
        return False
    return True


def test_python_connector() -> bool:
    """
    Verifies snowflake-connector-python can connect and run a simple query.
    """
    logger.info("test1_start", test="snowflake-connector-python")

    try:
        import snowflake.connector
    except ImportError:
        logger.error(
            "test1_failed",
            reason="snowflake-connector-python not installed",
            fix="poetry add snowflake-connector-python",
        )
        return False

    try:
        conn = snowflake.connector.connect(
            account  = SNOWFLAKE_ACCOUNT,
            user     = SNOWFLAKE_USER,
            password = SNOWFLAKE_PASSWORD,
            database = SNOWFLAKE_DATABASE,
            schema   = SNOWFLAKE_SCHEMA,
            warehouse= SNOWFLAKE_WAREHOUSE,
        )
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION()")
        version = cur.fetchone()[0]
        cur.close()
        conn.close()

        logger.info(
            "test1_passed",
            snowflake_version=version,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
        )
        return True

    except Exception as exc:
        logger.error("test1_failed", error=str(exc))
        return False


def ensure_snowflake_jdbc_jar() -> str:
    import urllib.request

    JDBC_DIR.mkdir(parents=True, exist_ok=True)

    if JDBC_JAR.exists():
        logger.info("snowflake_jdbc_jar_ready", path=str(JDBC_JAR))
        return str(JDBC_JAR.resolve())

    logger.info("snowflake_jdbc_jar_downloading", url=JDBC_URL_DL)
    try:
        urllib.request.urlretrieve(JDBC_URL_DL, JDBC_JAR)
        logger.info(
            "snowflake_jdbc_jar_downloaded",
            size_mb=round(JDBC_JAR.stat().st_size / 1e6, 1),
        )
    except Exception as exc:
        logger.error("snowflake_jdbc_jar_download_failed", error=str(exc))
        sys.exit(1)

    return str(JDBC_JAR.resolve())


def test_spark_write(spark, jdbc_url: str, jdbc_props: dict) -> bool:
    """Writes a small test dataframe to Snowflake via the Spark connector."""
    from pyspark.sql import Row
    from pyspark.sql.types import (
        IntegerType, StringType, StructField, StructType
    )

    logger.info("test2_start", test="spark_write", table=TEST_TABLE)

    schema = StructType([
        StructField("id",    IntegerType(), False),
        StructField("label", StringType(),  False),
    ])

    test_data = [
        Row(id=1, label="gabapentin"),
        Row(id=2, label="dupilumab"),
        Row(id=3, label="semaglutide"),
    ]

    test_df = spark.createDataFrame(test_data, schema)

    try:
        test_df.write.jdbc(
            url=jdbc_url,
            table=TEST_TABLE,
            mode="overwrite",
            properties=jdbc_props,
        )
        logger.info("test2_passed", rows_written=3, table=TEST_TABLE)
        return True

    except Exception as exc:
        logger.error(
            "test2_failed",
            error=str(exc),
            hint=(
                "Connector version mismatch is the most common cause. "
                "Try spark-snowflake_2.12:2.11.0-spark_3.3 if 2.12.0 fails."
            ),
        )
        return False


def test_spark_read(spark, jdbc_url: str, jdbc_props: dict) -> bool:
    """Reads back the test table written in test_spark_write."""
    logger.info("test3_start", test="spark_read", table=TEST_TABLE)

    try:
        df = spark.read.jdbc(
            url=jdbc_url,
            table=TEST_TABLE,
            properties=jdbc_props,
        )
        count = df.count()
        logger.info("test3_passed", rows_read=count, table=TEST_TABLE)
        df.show()
        return True

    except Exception as exc:
        logger.error("test3_failed", error=str(exc))
        return False


def cleanup_test_table() -> None:
    """Drops the test table created during testing."""
    try:
        import snowflake.connector
        conn = snowflake.connector.connect(
            account  = SNOWFLAKE_ACCOUNT,
            user     = SNOWFLAKE_USER,
            password = SNOWFLAKE_PASSWORD,
            database = SNOWFLAKE_DATABASE,
            schema   = SNOWFLAKE_SCHEMA,
            warehouse= SNOWFLAKE_WAREHOUSE,
        )
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {TEST_TABLE}")
        conn.commit()
        cur.close()
        conn.close()
        logger.info("cleanup_done", table=TEST_TABLE)
    except Exception as exc:
        logger.warning("cleanup_failed", error=str(exc))


def main():
    logger.info(
        "snowflake_connector_test_start",
        account=SNOWFLAKE_ACCOUNT,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )

    if not validate_env():
        sys.exit(1)

    t1 = test_python_connector()
    if not t1:
        logger.error(
            "stopping_early",
            reason="Python connector failed — fix before testing Spark",
        )
        sys.exit(1)

    jdbc_jar_path = ensure_snowflake_jdbc_jar()

    jdbc_url = (
        f"jdbc:snowflake://{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com/"
        f"?db={SNOWFLAKE_DATABASE}"
        f"&schema={SNOWFLAKE_SCHEMA}"
        f"&warehouse={SNOWFLAKE_WAREHOUSE}"
    )
    jdbc_props = {
        "user":     SNOWFLAKE_USER,
        "password": SNOWFLAKE_PASSWORD,
        "driver":   "net.snowflake.client.jdbc.SnowflakeDriver",
    }

    hadoop_home = os.getenv("HADOOP_HOME")
    if hadoop_home:
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["PATH"]        = os.environ["PATH"] + f";{hadoop_home}\\bin"

    from pyspark.sql import SparkSession

    logger.info("building_spark_session")
    try:
        spark = (
            SparkSession.builder
            .appName("MedSignal-SnowflakeTest")
            .master("local[*]")
            .config(
                "spark.jars.packages",
                f"{KAFKA_PACKAGE},{SNOWFLAKE_SPARK_PACKAGE}",
            )
            .config("spark.jars", jdbc_jar_path)
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "2g")
            .config("spark.log.level", "WARN")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        logger.info("spark_session_ready")
    except Exception as exc:
        logger.error("spark_session_failed", error=str(exc))
        sys.exit(1)

    t2 = test_spark_write(spark, jdbc_url, jdbc_props)

    t3 = False
    if t2:
        t3 = test_spark_read(spark, jdbc_url, jdbc_props)

    spark.stop()
    cleanup_test_table()

    logger.info(
        "test_summary",
        python_connector = "PASSED" if t1 else "FAILED",
        spark_write      = "PASSED" if t2 else "FAILED",
        spark_read       = "PASSED" if t3 else "FAILED",
    )

    if t1 and t2 and t3:
        logger.info(
            "snowflake_connector_verified",
            conclusion=(
                "All three tests passed. Safe to migrate Branch 1 to Snowflake. "
                "Replace psycopg2 with snowflake-connector-python throughout."
            ),
        )
        sys.exit(0)
    else:
        logger.error(
            "snowflake_connector_not_verified",
            conclusion=(
                "One or more tests failed. Do not migrate until all pass. "
                "Check connector version compatibility with PySpark 3.5.x."
            ),
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
