#!/bin/bash
# =============================================================================
# kafka_topics.sh
# =============================================================================
# Creates the four Kafka topics required by the MedSignal pipeline.
#
# Run this ONCE after docker compose up and BEFORE running faers_prep.py.
# If topics already exist the script exits cleanly without error.
#
# Each topic maps to one FAERS ASCII file type:
#   faers_demo   <-- DEMO*.txt   patient demographics
#   faers_drug   <-- DRUG*.txt   drug records per case
#   faers_reac   <-- REAC*.txt   MedDRA reaction terms
#   faers_outc   <-- OUTC*.txt   outcome codes
#
# 4 partitions per topic:
#   Spark reads each topic as a static batch dataframe.
#   4 partitions = 4 parallel Spark tasks reading simultaneously.
#   1 partition would serialize reads and slow Branch 1 significantly.
#
# Replication factor = 1:
#   We run a single Kafka broker in local development.
#   Replication factor > 1 requires multiple brokers.
#
# Usage
#   bash docker/kafka_topics.sh
# =============================================================================

set -e  # exit immediately if any command fails

BROKER="localhost:9092"
PARTITIONS=4
REPLICATION=1

TOPICS=(
  "faers_demo"
  "faers_drug"
  "faers_reac"
  "faers_outc"
)

echo "=============================================="
echo "MedSignal — Kafka Topic Setup"
echo "Broker     : $BROKER"
echo "Partitions : $PARTITIONS"
echo "Replication: $REPLICATION"
echo "=============================================="

for TOPIC in "${TOPICS[@]}"; do
  echo ""
  echo "Creating topic: $TOPIC"

  kafka-topics.sh \
    --create \
    --if-not-exists \
    --bootstrap-server "$BROKER" \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION"

  echo "  OK: $TOPIC created (or already exists)"
done

echo ""
echo "=============================================="
echo "Verifying all topics..."
echo "=============================================="
kafka-topics.sh --list --bootstrap-server "$BROKER"

echo ""
echo "=============================================="
echo "Topic details..."
echo "=============================================="
for TOPIC in "${TOPICS[@]}"; do
  kafka-topics.sh \
    --describe \
    --bootstrap-server "$BROKER" \
    --topic "$TOPIC"
done

echo ""
echo "All topics ready. You can now run faers_prep.py"