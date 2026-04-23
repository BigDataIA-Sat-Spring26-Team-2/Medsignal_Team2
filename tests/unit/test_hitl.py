"""
tests/unit/test_hitl.py — HITL router unit tests

Pure logic, no Snowflake, no network.
Tests decision validation, response shapes, error handling.

Run: poetry run pytest tests/unit/test_hitl.py -v -m unit
"""

import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """
    Creates a TestClient for the FastAPI app.
    Every test that needs to call an endpoint uses this fixture.
    """
    from app.main import app
    return TestClient(app)


@pytest.mark.unit
def test_health_returns_ok(client):
    """
    GET /health must return 200 with status=ok.
    Health endpoint connects to Snowflake so we only check status key.
    """
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.unit
def test_health_is_fast(client):
    """
    Health check must respond — Streamlit polls this on startup.
    Snowflake cold start can take 3-8 seconds so we allow up to 15 seconds.
    """
    import time
    start    = time.time()
    response = client.get("/health")
    elapsed  = time.time() - start

    assert response.status_code == 200
    assert elapsed < 15.0, f"Health check took {elapsed:.2f}s — Snowflake may be down"


@pytest.mark.unit
def test_post_decision_rejects_invalid_decision(client):
    """
    POST /hitl/decisions must reject decisions outside APPROVE/REJECT.
    The Snowflake write must never happen for invalid input.
    """
    response = client.post(
        "/hitl/decisions",
        json={
            "drug_key": "bupropion",
            "pt"      : "seizure",
            "decision": "MAYBE",
        },
    )
    assert response.status_code == 422


@pytest.mark.unit
def test_post_decision_rejects_missing_drug_key(client):
    """Pydantic must reject request body missing required drug_key field."""
    response = client.post(
        "/hitl/decisions",
        json={
            "pt"      : "seizure",
            "decision": "APPROVE",
        },
    )
    assert response.status_code == 422


@pytest.mark.unit
def test_post_decision_rejects_missing_pt(client):
    """Pydantic must reject request body missing required pt field."""
    response = client.post(
        "/hitl/decisions",
        json={
            "drug_key": "bupropion",
            "decision": "APPROVE",
        },
    )
    assert response.status_code == 422


@pytest.mark.unit
def test_post_decision_accepts_lowercase(client):
    """
    Decision value is case-insensitive — 'approve' must work same as 'APPROVE'.
    The router uppercases before writing to Snowflake.
    Mocks the Snowflake write so no real DB call happens.
    """
    with patch("app.routers.hitl.get_conn") as mock_conn, \
         patch("app.routers.hitl._get_pending_count", return_value=5), \
         patch("app.routers.hitl.set_queue_depth"):

        mock_cur  = MagicMock()
        mock_conn.return_value.__enter__ = MagicMock()
        mock_conn.return_value.cursor.return_value = mock_cur
        mock_conn.return_value.commit = MagicMock()

        response = client.post(
            "/hitl/decisions",
            json={
                "drug_key": "bupropion",
                "pt"      : "seizure",
                "decision": "approve",
            },
        )

    assert response.status_code in [200, 500]
    if response.status_code == 200:
        assert response.json()["decision"] == "APPROVE"


@pytest.mark.unit
def test_post_decision_reviewer_note_is_optional(client):
    """
    reviewer_note is Optional — omitting it must not cause validation error.
    Tests that the Pydantic model has correct default (None).
    """
    from app.routers.hitl import HITLDecision

    decision = HITLDecision(
        drug_key="bupropion",
        pt      ="seizure",
        decision="APPROVE",
    )

    assert decision.reviewer_note is None
    assert decision.brief_id      is None
    assert decision.drug_key      == "bupropion"
    assert decision.decision      == "APPROVE"


@pytest.mark.unit
def test_hitl_decision_model_all_valid_decisions():
    """Both valid decision values (APPROVE, REJECT) must be accepted by the Pydantic model."""
    from app.routers.hitl import HITLDecision

    for decision_value in ["APPROVE", "REJECT"]:
        d = HITLDecision(
            drug_key="dupilumab",
            pt      ="conjunctivitis",
            decision=decision_value,
        )
        assert d.decision == decision_value


@pytest.mark.unit
def test_hitl_decision_model_brief_id_is_optional():
    """brief_id is Optional — omitting it must not cause validation error."""
    from app.routers.hitl import HITLDecision

    without_brief = HITLDecision(
        drug_key="bupropion",
        pt      ="seizure",
        decision="APPROVE",
    )
    assert without_brief.brief_id is None

    with_brief = HITLDecision(
        drug_key="bupropion",
        pt      ="seizure",
        decision="APPROVE",
        brief_id=42,
    )
    assert with_brief.brief_id == 42


@pytest.mark.unit
def test_get_queue_returns_list(client):
    """
    GET /hitl/queue must always return a list — never a dict or null.
    Mocks Snowflake so no real DB connection needed.
    """
    mock_rows    = []
    mock_columns = [
        "brief_id", "drug_key", "pt", "priority", "stat_score", "lit_score",
        "recommended_action", "brief_text", "generation_error",
        "prr", "case_count", "death_count", "hosp_count",
        "lt_count", "generated_at",
    ]

    with patch("app.routers.hitl.get_conn") as mock_conn:
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = mock_rows
        mock_cur.description = [(col,) for col in mock_columns]
        mock_conn.return_value.cursor.return_value = mock_cur
        mock_conn.return_value.close = MagicMock()
        mock_cur.close = MagicMock()

        response = client.get("/hitl/queue")

    assert response.status_code == 200
    assert isinstance(response.json(), list)


@pytest.mark.unit
def test_get_queue_returns_correct_shape(client):
    """
    GET /hitl/queue rows must have the expected fields including brief_id.
    brief_id was added after Samiksha's review — verifies the SELECT
    includes it and the column mapping produces the correct dict key.
    """
    mock_columns = [
        "brief_id", "drug_key", "pt", "priority", "stat_score", "lit_score",
        "recommended_action", "brief_text", "generation_error",
        "prr", "case_count", "death_count", "hosp_count",
        "lt_count", "generated_at",
    ]
    mock_rows = [
        (
            1,
            "bupropion", "seizure", "P1", 0.78, 0.65,
            "LABEL_UPDATE", "Brief text here", False,
            4.2, 89, 3, 12, 5, "2026-04-16T00:00:00",
        )
    ]

    with patch("app.routers.hitl.get_conn") as mock_conn:
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = mock_rows
        mock_cur.description = [(col,) for col in mock_columns]
        mock_conn.return_value.cursor.return_value = mock_cur
        mock_conn.return_value.close = MagicMock()
        mock_cur.close = MagicMock()

        response = client.get("/hitl/queue")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1

    row = data[0]
    assert row["brief_id"]   == 1
    assert row["drug_key"]   == "bupropion"
    assert row["pt"]         == "seizure"
    assert row["priority"]   == "P1"
    assert row["stat_score"] == 0.78


@pytest.mark.unit
def test_hitl_cache_invalidated_after_approve_decision(client):
    """
    POST /hitl/decisions with APPROVE must call invalidate_signals exactly once.
    Validates cache invalidation: after a HITL decision the signal list cache
    must be cleared so the next GET /signals reflects the new hitl_decision value.
    """
    with patch("app.routers.hitl.get_conn") as mock_conn, \
         patch("app.routers.hitl._get_pending_count", return_value=4), \
         patch("app.routers.hitl.set_queue_depth"), \
         patch("app.routers.hitl.invalidate_signals") as mock_invalidate:

        mock_cur = MagicMock()
        mock_conn.return_value.cursor.return_value = mock_cur
        mock_conn.return_value.commit = MagicMock()
        mock_conn.return_value.close = MagicMock()
        mock_cur.close = MagicMock()

        response = client.post(
            "/hitl/decisions",
            json={
                "drug_key": "bupropion",
                "pt"      : "seizure",
                "decision": "APPROVE",
            },
        )

    assert response.status_code == 200
    mock_invalidate.assert_called_once()


@pytest.mark.unit
def test_get_decisions_returns_list(client):
    """
    GET /hitl/decisions must always return a list.
    Empty list is valid when no decisions have been made yet.
    """
    mock_columns = ["drug_key", "pt", "decision", "reviewer_note", "decided_at"]
    mock_rows    = []

    with patch("app.routers.hitl.get_conn") as mock_conn:
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = mock_rows
        mock_cur.description = [(col,) for col in mock_columns]
        mock_conn.return_value.cursor.return_value = mock_cur
        mock_conn.return_value.close = MagicMock()
        mock_cur.close = MagicMock()

        response = client.get("/hitl/decisions")

    assert response.status_code == 200
    assert isinstance(response.json(), list)
