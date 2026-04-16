"""
main.py — MedSignal FastAPI application

Routers:
    /signals     — signal feed and signal detail
    /hitl        — HITL queue and decision submission
    /evaluation  — lead times and precision-recall
    /health      — Snowflake connectivity check

Run:
    poetry run uvicorn main:app --reload --port 8000

Docs:
    http://localhost:8000/docs      — Swagger UI
"""

import os
import snowflake.connector
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers.signals import router as signals_router

load_dotenv()

app = FastAPI(
    title      = "MedSignal API",
    description= "Drug safety signal detection",
    version    = "1.0.0",
)

# Allow Streamlit (localhost:8501) to call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins    = ["http://localhost:8501"],
    allow_methods    = ["*"],
    allow_headers    = ["*"],
)

# Register routers
app.include_router(signals_router)
# app.include_router(hitl_router)        # add when hitl.py is ready
# app.include_router(evaluation_router)  # add when evaluation.py is ready


@app.get("/health", tags=["health"])
def health_check():
    """
    GET /health

    Confirms the API is running and Snowflake is reachable.
    Returns Snowflake version as a connectivity proof.
    """
    try:
        conn = snowflake.connector.connect(
            account  = os.getenv("SNOWFLAKE_ACCOUNT"),
            user     = os.getenv("SNOWFLAKE_USER"),
            password = os.getenv("SNOWFLAKE_PASSWORD"),
            database = os.getenv("SNOWFLAKE_DATABASE"),
            schema   = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
            warehouse= os.getenv("SNOWFLAKE_WAREHOUSE"),
        )
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION()")
        version = cur.fetchone()[0]
        cur.close()
        conn.close()
        return {"status": "ok", "snowflake_version": version}
    except Exception as e:
        return {"status": "error", "detail": str(e)}