"""
app/observability/metrics.py — Prometheus metrics for MedSignal pipeline.

Metrics exposed at GET /prometheus (Prometheus text format).
Metrics also readable as JSON at GET /metrics (for Streamlit dashboard).

Metric naming follows Prometheus conventions:
    medsignal_<subsystem>_<name>_<unit>

Counter  — monotonically increasing, never resets (except process restart)
Gauge    — can go up or down (current state)
Histogram — samples observations into buckets (latency, sizes)

Usage in agent code:
    from app.observability.metrics import LLM_TOKENS_USED
    LLM_TOKENS_USED.labels(agent="agent1", type="input").inc(100)
"""

from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry

# ── Registry ──────────────────────────────────────────────────────────────────
# Use default registry — prometheus_client exposes this at /metrics by default.
# We use a custom path /prometheus to avoid conflict with our JSON /metrics.

REGISTRY = CollectorRegistry()

# ── Kafka metrics ─────────────────────────────────────────────────────────────

KAFKA_RECORDS_PUBLISHED = Counter(
    "medsignal_kafka_records_total",
    "Total records published to Kafka by topic",
    ["topic"],
    registry=REGISTRY,
)

# ── Spark metrics ─────────────────────────────────────────────────────────────

SPARK_JOB_DURATION = Histogram(
    "medsignal_spark_job_seconds",
    "Spark batch job duration in seconds",
    ["branch"],
    buckets=[30, 60, 120, 300, 600, 1800, 3600],
    registry=REGISTRY,
)

# ── Signal metrics ────────────────────────────────────────────────────────────

SIGNALS_FLAGGED = Gauge(
    "medsignal_signals_flagged_total",
    "Total signals in signals_flagged table",
    registry=REGISTRY,
)

# ── Agent execution metrics ───────────────────────────────────────────────────

AGENT_DURATION = Histogram(
    "medsignal_agent_seconds",
    "Agent execution time in seconds",
    ["agent"],
    buckets=[0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
    registry=REGISTRY,
)

PIPELINE_DURATION = Histogram(
    "medsignal_pipeline_duration_seconds",
    "End-to-end agent pipeline duration per signal",
    buckets=[10, 30, 60, 120, 180, 300],
    registry=REGISTRY,
)

# ── LLM token metrics ─────────────────────────────────────────────────────────

LLM_TOKENS_USED = Counter(
    "medsignal_llm_tokens_total",
    "GPT-4o tokens consumed",
    ["agent", "type"],   # type: input | output
    registry=REGISTRY,
)

# ── SafetyBrief metrics ───────────────────────────────────────────────────────

SAFETY_BRIEFS_GENERATED = Counter(
    "medsignal_safety_briefs_total",
    "SafetyBriefs generated",
    ["priority", "status"],   # status: success | retry | error
    registry=REGISTRY,
)

# ── Agent 2 retrieval metrics ─────────────────────────────────────────────────

AGENT2_ABSTRACTS_RETRIEVED = Histogram(
    "medsignal_agent2_abstracts_retrieved",
    "Number of abstracts returned by Agent 2 per query",
    buckets=[0, 1, 2, 3, 4, 5],
    registry=REGISTRY,
)

AGENT2_ZERO_RESULTS = Counter(
    "medsignal_agent2_zero_results_total",
    "Agent 2 queries returning zero abstracts above threshold",
    registry=REGISTRY,
)

# ── Agent 3 quality metrics ───────────────────────────────────────────────────

AGENT3_PYDANTIC_RETRIES = Counter(
    "medsignal_agent3_pydantic_retries_total",
    "Agent 3 Pydantic validation retries triggered",
    registry=REGISTRY,
)

AGENT3_CITATIONS_REMOVED = Counter(
    "medsignal_agent3_citations_removed_total",
    "Hallucinated PMIDs removed by citation validator",
    registry=REGISTRY,
)

# ── HITL metrics ──────────────────────────────────────────────────────────────

HITL_QUEUE_DEPTH = Gauge(
    "medsignal_hitl_queue_depth",
    "Number of signals awaiting HITL review",
    registry=REGISTRY,
)

HITL_DECISIONS = Counter(
    "medsignal_hitl_decisions_total",
    "HITL decisions recorded",
    ["decision"],   # approve | reject | escalate
    registry=REGISTRY,
)


# ── Helper: refresh gauges from Snowflake ─────────────────────────────────────

def refresh_gauges(pg_conn=None) -> None:
    """
    Update Gauge metrics from Snowflake.
    Called before every Prometheus scrape so values are current.

    Gauges cannot be incremented like counters — they need the
    current absolute value from the database.
    """
    try:
        from app.utils.snowflake_client import get_conn
        from app.utils.redis_client import get_queue_depth

        conn = get_conn()
        cur  = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM signals_flagged")
        SIGNALS_FLAGGED.set(cur.fetchone()[0])

        cur.execute("""
            SELECT COUNT(*) FROM signals_flagged sf
            WHERE NOT EXISTS (
                SELECT 1 FROM hitl_decisions hd
                WHERE hd.drug_key = sf.drug_key AND hd.pt = sf.pt
            )
        """)
        HITL_QUEUE_DEPTH.set(cur.fetchone()[0])

        cur.close()
        conn.close()

    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(
            "refresh_gauges_failed error=%s", e
        )