import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import signals, hitl, health

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

app = FastAPI(
    title      ="MedSignal API",
    description="Drug safety signal detection — FAERS + LangGraph + HITL",
    version    ="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Streamlit connects from localhost
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(signals.router)
app.include_router(hitl.router)
app.include_router(health.router)