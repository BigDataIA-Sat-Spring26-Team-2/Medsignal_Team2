"""
hitl.py — FastAPI router for HITL decision endpoints.

Endpoints:
    GET  /hitl/queue       — all signals pending review, P1 first
    GET  /hitl/decisions   — full audit log of past decisions
    POST /hitl/decisions   — submit approve/reject/escalate decision

Design:
    Every decision is a new INSERT — never UPDATE.
    The audit log is immutable. If a reviewer changes their mind,
    a second row is written. The latest row per (drug_key, pt) wins.

    queue depth is written to Redis after every POST so Prometheus
    reads the updated value without hitting Snowflake every 15 seconds.
"""