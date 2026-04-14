import json
import glob
import os
import argparse
import sys
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
from kafka.structs import TopicPartition

BROKER  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DATA_DIR = os.getenv("FAERS_DATA_DIR", "data/faers")

TOPIC_MAP = {
    "DEMO": "faers_demo",
    "DRUG": "faers_drug",
    "REAC": "faers_reac",
    "OUTC": "faers_outc",
}


# ── Kafka helpers ─────────────────────────────────────────────────────────────

def get_published_quarters(broker: str, topic: str) -> set[str]:
    """
    Samples messages from a Kafka topic to discover which
    source_quarters have already been published.

    Strategy: seek to the last 500 messages per partition and
    extract unique source_quarter values. This is fast regardless
    of topic size — we never read all messages.

    Returns a set of quarter labels e.g. {'2023Q1', '2023Q2'}.
    Returns an empty set if the topic is empty or unreachable.
    """
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=broker,
            enable_auto_commit=False,
            consumer_timeout_ms=3000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            consumer.close()
            return set()

        tps = [TopicPartition(topic, p) for p in partitions]
        consumer.assign(tps)

        end_offsets   = consumer.end_offsets(tps)
        begin_offsets = consumer.beginning_offsets(tps)

        quarters_found = set()

        for tp in tps:
            end   = end_offsets[tp]
            begin = begin_offsets[tp]

            if end == begin:
                continue  # empty partition

            # Sample last 500 messages per partition
            seek_to = max(begin, end - 500)
            consumer.seek(tp, seek_to)

        # Read sampled messages
        for msg in consumer:
            quarter = msg.value.get("source_quarter")
            if quarter:
                quarters_found.add(quarter)

        consumer.close()
        return quarters_found

    except Exception as exc:
        print(f"  WARNING: could not check existing quarters in {topic}: {exc}")
        return set()


# ── Producer ──────────────────────────────────────────────────────────────────

def make_producer() -> KafkaProducer:
    try:
        return KafkaProducer(
            bootstrap_servers=BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            batch_size=65536,
            linger_ms=10,
            retries=3,
            retry_backoff_ms=200,
            acks="all",
        )
    except NoBrokersAvailable:
        print(f"ERROR: Cannot connect to Kafka at {BROKER}")
        print("Fix: docker compose -f docker/docker-compose.yml up -d")
        print("     then wait 30 seconds and try again")
        sys.exit(1)


# ── File publisher ────────────────────────────────────────────────────────────

def publish_file(
    producer: KafkaProducer,
    filepath: str,
    topic: str,
    quarter: str,
    dry_run: bool = False,
) -> int:
    """
    Reads a single FAERS ASCII file and publishes each row as a
    JSON message to the given Kafka topic.

    Returns the number of rows sent (or counted in dry-run mode).
    """
    df = pd.read_csv(
        filepath,
        sep="$",
        encoding="latin1",
        low_memory=False,
        dtype=str,
    )
    df["source_quarter"] = quarter
    df = df.fillna("")

    sent = 0
    for row in df.to_dict("records"):
        if not dry_run:
            producer.send(topic, value=row)
        sent += 1
        if sent % 50_000 == 0:
            if not dry_run:
                producer.flush()
            print(
                f"    {topic}: {sent:,} rows "
                f"{'counted' if dry_run else 'sent'} so far..."
            )

    if not dry_run:
        producer.flush()
    print(
        f"    {topic}: DONE — {sent:,} records "
        f"{'(dry run)' if dry_run else ''}"
    )
    return sent


# ── Quarter discovery ─────────────────────────────────────────────────────────

def discover_quarters(
    data_dir: str,
    year: str = None,
    quarters: list = None,
) -> list[tuple]:
    """
    Discovers quarter folders under data_dir/<year>/<quarter>/.
    year=None means all years.
    quarters=None means all quarters.
    Returns list of (quarter_label, quarter_path) tuples.
    """
    results = []

    if year:
        year_dirs = sorted(glob.glob(f"{data_dir}/{year}/"))
    else:
        year_dirs = sorted(glob.glob(f"{data_dir}/*/"))

    for year_dir in year_dirs:
        quarter_dirs = sorted(glob.glob(f"{year_dir}*/"))
        for quarter_dir in quarter_dirs:
            label = os.path.basename(os.path.normpath(quarter_dir))
            if quarters:
                try:
                    q_num = int(label.split("Q")[-1])
                except ValueError:
                    continue
                if q_num not in quarters:
                    continue
            results.append((label, quarter_dir.rstrip("/\\")))

    return results


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Publish FAERS ASCII files to Kafka topics.",
        epilog="""
Examples:
  # All years, all quarters
  poetry run python scripts/faers_prep.py

  # Specific year, all quarters
  poetry run python scripts/faers_prep.py --year 2023

  # Specific year and specific quarters
  poetry run python scripts/faers_prep.py --year 2023 --quarters 1 2

  # Dry run — check files exist without sending to Kafka
  poetry run python scripts/faers_prep.py --year 2023 --dry-run

  # Force republish even if quarter already in Kafka
  poetry run python scripts/faers_prep.py --year 2023 --quarters 1 --force
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--year",
        type=str,
        default=None,
        help="Year to publish (e.g. 2023). Omit for all years.",
    )
    parser.add_argument(
        "--quarters",
        type=int,
        nargs="+",
        choices=[1, 2, 3, 4],
        default=None,
        help="Quarters to publish (e.g. 1 2). Omit for all quarters.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count rows without sending to Kafka. Use to verify files exist.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Republish even if quarter already exists in Kafka topics. "
            "WARNING: creates duplicates. Only use after docker compose down -v "
            "to wipe Kafka state."
        ),
    )
    return parser.parse_args()


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    print("Starting FAERS Kafka producer...")
    print(f"Broker  : {BROKER}")
    print(f"Data dir: {DATA_DIR}")
    if args.year:
        print(f"Year    : {args.year}")
    if args.quarters:
        print(f"Quarters: {args.quarters}")
    if args.dry_run:
        print("Mode    : DRY RUN — no messages will be sent to Kafka")
    if args.force:
        print("Mode    : FORCE — will republish even if quarter already present")
    print()

    quarters = discover_quarters(DATA_DIR, args.year, args.quarters)

    if not quarters:
        print("ERROR: No matching quarter folders found.")
        print("Check that download_faers.py has been run first.")
        sys.exit(1)

    print(f"Found {len(quarters)} quarter(s) to publish:")
    for label, path in quarters:
        print(f"  {label} → {path}")
    print()

    producer = None
    if not args.dry_run:
        producer = make_producer()

    grand_total  = 0
    missing_files = []
    skipped      = []

    for quarter_label, quarter_path in quarters:
        print(f"--- Quarter: {quarter_label} ---")

        for file_type, topic in TOPIC_MAP.items():
            pattern = f"{quarter_path}/{file_type}*.txt"
            files   = glob.glob(pattern)

            if not files:
                print(f"  WARNING: no files found for {pattern}")
                missing_files.append(f"{quarter_label}/{file_type}")
                continue

            for filepath in files:

                # ── Deduplication check ───────────────────────────
                # Before publishing, sample existing Kafka messages
                # to see if this quarter is already present.
                # Skips the file if found, unless --force is set.
                # This prevents duplicate data when faers_prep.py
                # is re-run without wiping Kafka first.
                if not args.dry_run and not args.force:
                    existing = get_published_quarters(BROKER, topic)
                    if quarter_label in existing:
                        print(
                            f"  SKIP: {quarter_label} already in "
                            f"{topic} — use --force to republish"
                        )
                        skipped.append(f"{quarter_label}/{file_type}")
                        continue
                # ─────────────────────────────────────────────────

                print(f"  Publishing {filepath} -> {topic}")
                count = publish_file(
                    producer, filepath, topic,
                    quarter_label, args.dry_run,
                )
                grand_total += count

        print()

    if producer is not None:
        producer.close()

    print(f"All done. Total records: {grand_total:,}")

    if skipped:
        print(f"\nSkipped {len(skipped)} file(s) already in Kafka:")
        for s in skipped:
            print(f"  {s}")
        print("Run with --force to republish.")

    if missing_files:
        print(f"\nWARNING: {len(missing_files)} file(s) not found:")
        for f in missing_files:
            print(f"  {f}")
        print("Spark will run but those topics will be empty.")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()