"""
health.py — FastAPI health check endpoint.

Confirms the API is running and Snowflake is reachable.
Used by Streamlit on startup and Prometheus for liveness checks.
"""

import os
import snowflake.connector
from dotenv import load_dotenv
from fastapi import APIRouter

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