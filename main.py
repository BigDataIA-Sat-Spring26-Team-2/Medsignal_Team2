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
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers.signals import router as signals_router
from app.routers.hitl    import router as hitl_router
from app.routers.health  import router as health_router

load_dotenv()

app = FastAPI(
    title      ="MedSignal API",
    description="Drug safety signal detection",
    version    ="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8501"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(signals_router)
app.include_router(hitl_router)
app.include_router(health_router)
# app.include_router(evaluation_router)  # add when evaluation.py is ready