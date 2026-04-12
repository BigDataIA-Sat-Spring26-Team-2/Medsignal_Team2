"""
test_kafka_read.py — Quick test to verify Spark can read from Kafka topics
"""
import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"

from dotenv import load_dotenv
load_dotenv()

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("test-kafka-read")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("\n=== Testing Kafka read ===")

for topic in ["faers_demo", "faers_drug", "faers_reac", "faers_outc"]:
    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    count = df.count()
    print(f"{topic}: {count:,} rows")

print("\n=== Sample raw message from faers_demo ===")
demo = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "faers_demo")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
    .selectExpr("CAST(value AS STRING) as value")
)
demo.show(3, truncate=False)

spark.stop()
print("\nDone.")