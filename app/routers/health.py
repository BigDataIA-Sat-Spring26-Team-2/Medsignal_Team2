"""
health.py — FastAPI health check and system metrics endpoints.

Confirms the API is running and Snowflake is reachable.
Used by Streamlit on startup and Prometheus for liveness checks.
"""

import os
from datetime import datetime, timezone

import snowflake.connector
from dotenv import load_dotenv
from fastapi import APIRouter

from app.utils.redis_client import get_queue_depth
from app.utils.snowflake_client import get_conn
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
load_dotenv()

router = APIRouter(tags=["health"])


@router.get("/health")
def health_check():
    """
    GET /health

    Connects to Snowflake and returns the current version as proof
    that the database is reachable. Returns status=error with detail
    if the connection fails so the caller knows why.
    """
    try:
        conn = snowflake.connector.connect(
            account  =os.getenv("SNOWFLAKE_ACCOUNT"),
            user     =os.getenv("SNOWFLAKE_USER"),
            password =os.getenv("SNOWFLAKE_PASSWORD"),
            database =os.getenv("SNOWFLAKE_DATABASE"),
            schema   =os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        )
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION()")
        version = cur.fetchone()[0]
        cur.close()
        conn.close()
        return {"status": "ok", "snowflake_version": version}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@router.get("/metrics")
def get_metrics():
    """
    GET /metrics

    Returns aggregated system metrics sourced from Snowflake and Redis:
      - signals_flagged       : total rows in signals_flagged
      - safety_briefs         : rows where generation_error = FALSE
      - hitl_decisions        : total rows in hitl_decisions
      - priority_distribution : count per priority tier from safety_briefs
      - decision_breakdown    : count per decision value from hitl_decisions
      - queue_depth           : pending HITL queue depth read from Redis

    Returns status=error with detail if any Snowflake query fails.
    Redis failure is handled gracefully — queue_depth returns 0.
    """
    conn = None
    cur  = None
    try:
        conn = get_conn()
        cur  = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM signals_flagged")
        signals_count = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(*) FROM safety_briefs WHERE generation_error = FALSE"
        )
        briefs_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM hitl_decisions")
        decisions_count = cur.fetchone()[0]

        cur.execute(
            "SELECT priority, COUNT(*) "
            "FROM safety_briefs "
            "WHERE priority IS NOT NULL "
            "GROUP BY priority "
            "ORDER BY priority"
        )
        priority_distribution = {row[0]: row[1] for row in cur.fetchall()}

        cur.execute(
            "SELECT decision, COUNT(*) "
            "FROM hitl_decisions "
            "GROUP BY decision "
            "ORDER BY decision"
        )
        decision_breakdown = {row[0]: row[1] for row in cur.fetchall()}
        # ── Agent behavior metrics from safety_briefs ─────────────────────
        cur.execute("""
            SELECT
                COUNT(*)                                          AS total_runs,
                COALESCE(SUM(input_tokens), 0)                   AS tokens_in,
                COALESCE(SUM(output_tokens), 0)                  AS tokens_out,
                COALESCE(AVG(lit_score), 0)                      AS avg_lit_score,
                COUNT(CASE WHEN lit_score = 0 THEN 1 END)        AS zero_lit_runs,
                COUNT(CASE WHEN generation_error = TRUE THEN 1 END) AS gen_errors
            FROM safety_briefs
        """)
        row          = cur.fetchone()
        total_runs   = int(row[0] or 0)
        tokens_in    = int(row[1] or 0)
        tokens_out   = int(row[2] or 0)
        avg_lit      = round(float(row[3] or 0), 4)
        zero_lit     = int(row[4] or 0)
        gen_errors   = int(row[5] or 0)

        estimated_cost = round(
            (tokens_in * 0.15 + tokens_out * 0.60) / 1_000_000, 4
        )
        return {
            "status"               : "ok",
            "timestamp"            : datetime.now(timezone.utc).isoformat(),
            "signals_flagged"      : signals_count,
            "safety_briefs"        : briefs_count,
            "hitl_decisions"       : decisions_count,
            "queue_depth"          : get_queue_depth(),
            "priority_distribution": priority_distribution,
            "decision_breakdown"   : decision_breakdown,
            "agent_metrics"        : {
                "total_pipeline_runs" : total_runs,
                "total_tokens_input"  : tokens_in,
                "total_tokens_output" : tokens_out,
                "estimated_cost_usd"  : estimated_cost,
                "avg_lit_score"       : avg_lit,
                "zero_lit_score_runs" : zero_lit,
                "generation_errors"   : gen_errors,
                "pydantic_retries"    : 0,
                "citations_removed"   : 0,
                "avg_pipeline_duration_s": 0,
            },
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}
    finally:
        if cur  is not None:
            cur.close()
        if conn is not None:
            conn.close()
@router.get("/prometheus")
def prometheus_metrics():
    """
    GET /prometheus

    Returns metrics in Prometheus text exposition format.
    Scrape this endpoint with Prometheus server:
        scrape_configs:
          - job_name: medsignal
            static_configs:
              - targets: ['localhost:8001']
            metrics_path: /prometheus

    Also readable directly in browser to verify metrics are updating.
    """
    from app.observability.metrics import REGISTRY, refresh_gauges

    # Refresh Snowflake-backed gauges before generating output
    refresh_gauges()

    data = generate_latest(REGISTRY)
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)