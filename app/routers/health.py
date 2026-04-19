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

        cur.close()
        conn.close()

        return {
            "status"               : "ok",
            "timestamp"            : datetime.now(timezone.utc).isoformat(),
            "signals_flagged"      : signals_count,
            "safety_briefs"        : briefs_count,
            "hitl_decisions"       : decisions_count,
            "queue_depth"          : get_queue_depth(),
            "priority_distribution": priority_distribution,
            "decision_breakdown"   : decision_breakdown,
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}
