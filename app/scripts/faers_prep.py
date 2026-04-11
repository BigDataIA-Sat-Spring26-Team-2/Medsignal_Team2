"""
faers_prep.py — FAERS Kafka Producer

Reads the four raw FAERS ASCII files (DEMO, DRUG, REAC, OUTC) for one or
more quarterly releases and publishes each row as a JSON message to the
corresponding Kafka topic. No joining, filtering, normalization, or
deduplication happens here — all of that is Spark's job.

Topic mapping:
    DEMO*.txt  →  faers_demo
    DRUG*.txt  →  faers_drug
    REAC*.txt  →  faers_reac
    OUTC*.txt  →  faers_outc

Each message carries a `source_quarter` field (e.g. "2023Q1") so Spark
can tag rows by quarter without needing to re-inspect filenames.

Usage:
    python faers_prep.py --data-dir data/faers/2023
    python faers_prep.py --data-dir data/faers/2023 --quarters 1 2
    python faers_prep.py --data-dir data/faers/2023 --broker localhost:9092
    python faers_prep.py --data-dir data/faers/2023 --batch-size 500 --dry-run
"""

import argparse
import csv
import json
import sys
import time
from pathlib import Path

import structlog
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

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

DEFAULT_BROKER     = "localhost:9092"
DEFAULT_BATCH_SIZE = 1000
REQUEST_DELAY_S    = 0.0

# Maps the file-type prefix (upper-cased) to its Kafka topic name
FILE_TO_TOPIC: dict[str, str] = {
    "DEMO": "faers_demo",
    "DRUG": "faers_drug",
    "REAC": "faers_reac",
    "OUTC": "faers_outc",
}

# FAERS files are dollar-sign delimited
DELIMITER = "$"
ENCODING  = "latin-1"


# Producer setup
def build_producer(broker: str) -> KafkaProducer:
    """
    Creates a KafkaProducer with JSON serialization.
    Raises SystemExit if the broker is unreachable.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            # Accumulate up to 64 KB or 10 ms before sending a batch —
            # reduces round-trips without adding meaningful latency.
            batch_size=65536,
            linger_ms=10,
            # Retry up to 3 times on transient send failures.
            retries=3,
            retry_backoff_ms=200,
            # Wait for all in-sync replicas (acks=all) for durability.
            # With replication_factor=1 this is equivalent to acks=1 but
            # keeps the config correct for a future multi-broker setup.
            acks="all",
        )
        return producer
    except NoBrokersAvailable:
        logger.error(
            "kafka_unavailable",
            broker=broker,
            hint="Run: docker compose -f docker/docker-compose.yml up -d, then wait ~30s",
        )
        sys.exit(1)


# File discovery
def discover_quarter_dirs(data_dir: Path, quarters: list[int]) -> list[tuple[str, Path]]:
    """
    Returns a sorted list of (quarter_label, quarter_path) tuples for each
    requested quarter found under data_dir.
    """
    found = []

    for child in sorted(data_dir.iterdir()):
        if not child.is_dir():
            continue

        # Match directories named like "2023Q1", "2023Q2", etc.
        name = child.name.upper()
        if len(name) != 6 or not name[:4].isdigit() or name[4] != "Q":
            continue

        try:
            q_num = int(name[5])
        except ValueError:
            continue

        if quarters and q_num not in quarters:
            continue

        found.append((child.name, child))

    return found


def find_file_for_type(quarter_dir: Path, file_type: str) -> Path | None:
    """
    Finds the single FAERS .txt file for a given file type (DEMO, DRUG, etc.)
    within a quarter directory. Returns None if not found.
    """
    for candidate in quarter_dir.glob("*.txt"):
        if candidate.stem.upper().startswith(file_type):
            return candidate
    return None


# Core publish logic
def publish_file(
    producer: KafkaProducer,
    filepath: Path,
    topic: str,
    quarter_label: str,
    batch_size: int,
    dry_run: bool,
) -> int:
    """
    Reads a single FAERS ASCII file and publishes each row as a JSON message
    to the given Kafka topic.

    Returns the number of rows published.

    The `source_quarter` field is injected into every message so that Spark
    can identify which quarterly release each row came from.
    """
    row_count   = 0
    error_count = 0

    with open(filepath, encoding=ENCODING, errors="replace") as fh:
        reader = csv.DictReader(fh, delimiter=DELIMITER)

        # Normalise header names: strip whitespace and uppercase.
        # FAERS column names are inconsistently cased across quarters.
        if reader.fieldnames is None:
            logger.warning("no_headers", file=filepath.name)
            return 0

        reader.fieldnames = [
            f.strip().lower() for f in reader.fieldnames
        ]

        for raw_row in reader:
            # Inject the quarter label so Spark knows the data provenance.
            row = dict(raw_row)
            row["source_quarter"] = quarter_label

            # Strip leading/trailing whitespace from all values.
            # FAERS files often have padded fields.
            row = {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}

            if not dry_run:
                try:
                    producer.send(topic, value=row)
                except KafkaError as exc:
                    error_count += 1
                    if error_count <= 5:
                        logger.warning("send_error", row=row_count, error=str(exc))

            row_count += 1

            # Flush periodically to avoid accumulating too much in the
            # producer's internal buffer on very large files.
            if row_count % batch_size == 0:
                if not dry_run:
                    producer.flush()
                print(
                    f"\r    {row_count:,} rows {'(dry-run) ' if dry_run else ''}published...",
                    end="",
                    flush=True,
                )

    print()  # newline after the progress line

    if error_count:
        logger.warning("publish_errors", error_count=error_count, total_rows=row_count)

    return row_count


# Quarter-level orchestration
def process_quarter(
    producer: KafkaProducer,
    quarter_label: str,
    quarter_dir: Path,
    batch_size: int,
    dry_run: bool,
) -> dict[str, int]:
    """
    Publishes all four FAERS file types for a single quarter.
    Returns a dict mapping file_type → rows_published.
    """
    log_quarter = logger.bind(quarter=quarter_label, quarter_dir=str(quarter_dir))
    log_quarter.info("processing_quarter")

    results: dict[str, int] = {}

    for file_type, topic in FILE_TO_TOPIC.items():
        filepath = find_file_for_type(quarter_dir, file_type)

        if filepath is None:
            log_quarter.warning("file_not_found", file_type=file_type, topic=topic)
            results[file_type] = 0
            continue

        size_mb = filepath.stat().st_size / 1_000_000
        log_quarter.info(
            "publishing_file",
            file_type=file_type,
            topic=topic,
            file=filepath.name,
            size_mb=round(size_mb, 1),
        )

        t_start = time.perf_counter()
        rows    = publish_file(
            producer=producer,
            filepath=filepath,
            topic=topic,
            quarter_label=quarter_label,
            batch_size=batch_size,
            dry_run=dry_run,
        )
        elapsed = time.perf_counter() - t_start

        log_quarter.info(
            "file_done",
            file_type=file_type,
            rows=rows,
            elapsed_s=round(elapsed, 1),
            rows_per_s=int(rows / elapsed) if elapsed > 0 else 0,
        )
        results[file_type] = rows

        if REQUEST_DELAY_S:
            time.sleep(REQUEST_DELAY_S)

    if not dry_run:
        producer.flush()
        log_quarter.info("buffer_flushed")

    return results


# Summary
def print_summary(
    all_results: dict[str, dict[str, int]],
    dry_run: bool,
) -> None:
    grand_total = 0

    for quarter_label, file_results in all_results.items():
        quarter_total = sum(file_results.values())
        grand_total  += quarter_total
        logger.info(
            "quarter_summary",
            quarter=quarter_label,
            **{ft.lower(): rows for ft, rows in file_results.items()},
            total=quarter_total,
        )

    logger.info(
        "publish_complete",
        grand_total=grand_total,
        quarters=len(all_results),
        dry_run=dry_run,
        next_step="spark.read.format('kafka') in batch mode",
    )


# CLI
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Publish raw FAERS ASCII records to Kafka topics.\n"
            "No processing — just reads and publishes."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python faers_prep.py --data-dir data/faers/2023\n"
            "  python faers_prep.py --data-dir data/faers/2023 --quarters 1 2\n"
            "  python faers_prep.py --data-dir data/faers/2023 --dry-run\n"
            "  python faers_prep.py --data-dir data/faers/2023 --broker localhost:9092\n"
        ),
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help=(
            "Root directory for a single year's FAERS data.\n"
            "Expected layout: <data-dir>/<year>Q<n>/*.txt\n"
            "e.g. data/faers/2023 → data/faers/2023/2023Q1/DEMO23Q1.txt"
        ),
    )
    parser.add_argument(
        "--quarters",
        type=int,
        nargs="+",
        choices=[1, 2, 3, 4],
        default=[],
        help="Quarters to publish (default: all found under --data-dir).",
    )
    parser.add_argument(
        "--broker",
        type=str,
        default=DEFAULT_BROKER,
        help=f"Kafka bootstrap server (default: {DEFAULT_BROKER}).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Rows between producer.flush() calls (default: {DEFAULT_BATCH_SIZE}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse files and count rows without sending to Kafka.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    logger.info(
        "faers_producer_start",
        data_dir=str(args.data_dir.resolve()),
        broker=args.broker,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        quarters=args.quarters or "all",
    )

    # Validate data directory
    if not args.data_dir.exists():
        logger.error(
            "data_dir_not_found",
            data_dir=str(args.data_dir),
            hint="Run download_faers.py first, or check --data-dir",
        )
        sys.exit(1)

    # Discover quarter directories
    quarters = discover_quarter_dirs(args.data_dir, args.quarters)
    if not quarters:
        logger.error(
            "no_quarters_found",
            data_dir=str(args.data_dir),
            hint="Expected subdirectories like 2023Q1, 2023Q2 ...",
        )
        sys.exit(1)

    logger.info("quarters_found", count=len(quarters), labels=[q for q, _ in quarters])

    # Build producer (skipped in dry-run mode)
    producer = None
    if not args.dry_run:
        producer = build_producer(args.broker)

    # Process each quarter
    all_results: dict[str, dict[str, int]] = {}

    try:
        for quarter_label, quarter_dir in quarters:
            results = process_quarter(
                producer=producer,
                quarter_label=quarter_label,
                quarter_dir=quarter_dir,
                batch_size=args.batch_size,
                dry_run=args.dry_run,
            )
            all_results[quarter_label] = results

    finally:
        if producer is not None:
            producer.flush()
            producer.close()
            logger.info("producer_closed")

    print_summary(all_results, dry_run=args.dry_run)

    # Exit with error if any file was missing (0-row result)
    missing = [
        f"{q}/{ft}"
        for q, res in all_results.items()
        for ft, rows in res.items()
        if rows == 0
    ]
    if missing:
        logger.warning(
            "missing_files",
            files=missing,
            hint="Spark will still run but those topics will be empty",
        )
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()