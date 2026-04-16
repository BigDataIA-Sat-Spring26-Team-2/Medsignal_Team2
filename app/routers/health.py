"""
health.py — FastAPI health check endpoint.

Used by:
    docker compose healthcheck
    Prometheus to verify API is alive before scraping
    Streamlit to confirm backend is reachable on startup
"""

from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
def health():
    return {"status": "ok"}