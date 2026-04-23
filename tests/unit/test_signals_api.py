"""
tests/unit/test_signals_api.py — Unit tests for GET /signals endpoints.

Tests signal list, counts, filtering, and brief 404 behaviour.
All dependencies mocked — no Snowflake, no Redis, no network.

Run: poetry run pytest tests/unit/test_signals_api.py -v -m unit
"""

import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    from app.main import app
    return TestClient(app)


_SAMPLE_SIGNALS = [
    {
        "drug_key": "bupropion", "pt": "seizure", "prr": 4.2,
        "stat_score": 0.78, "drug_reaction_count": 89,
        "death_count": 3, "hosp_count": 12, "lt_count": 5,
        "drug_total": 1200, "computed_at": "2026-04-01 00:00:00",
        "lit_score": 0.65, "priority": "P1", "generation_error": False,
        "hitl_decision": "APPROVE",
    },
    {
        "drug_key": "dupilumab", "pt": "eczema", "prr": 3.1,
        "stat_score": 0.55, "drug_reaction_count": 45,
        "death_count": 0, "hosp_count": 5, "lt_count": 1,
        "drug_total": 800, "computed_at": "2026-04-01 00:00:00",
        "lit_score": 0.40, "priority": "P2", "generation_error": False,
        "hitl_decision": None,
    },
]


@pytest.mark.unit
def test_list_signals_has_hitl_decision_key(client):
    """
    Mock Snowflake to return a row that includes a hitl_decision value.
    Every item in GET /signals response must have a hitl_decision key
    (value can be null or a string).
    Validates that the query joins hitl_decisions and exposes the field.
    """
    mock_columns = [
        "drug_key", "pt", "prr", "stat_score", "drug_reaction_count",
        "death_count", "hosp_count", "lt_count", "drug_total", "computed_at",
        "lit_score", "priority", "generation_error", "hitl_decision",
    ]
    mock_row = (
        "bupropion", "seizure", 4.2, 0.78, 89,
        3, 12, 5, 1200, "2026-04-01 00:00:00",
        0.65, "P1", False, "APPROVE",
    )

    with patch("app.services.signal_service.cache_get", return_value=None), \
         patch("app.services.signal_service.cache_set"), \
         patch("app.services.signal_service.get_conn") as mock_conn:

        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [mock_row]
        mock_cur.description = [(col,) for col in mock_columns]
        mock_conn.return_value.cursor.return_value = mock_cur
        mock_conn.return_value.close = MagicMock()
        mock_cur.close = MagicMock()

        response = client.get("/signals")

    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0
    for item in data:
        assert "hitl_decision" in item, (
            f"hitl_decision key missing from signal item: {item.keys()}"
        )


@pytest.mark.unit
def test_signal_count_endpoint_shape(client):
    """
    GET /signals/count must return a dict with keys total, P1, P2, P3, P4,
    uninvestigated, all with integer values.
    """
    mock_counts = {
        "total": 150, "P1": 12, "P2": 30, "P3": 45, "P4": 38, "uninvestigated": 25,
    }

    with patch("app.routers.signals.get_signal_counts", return_value=mock_counts):
        response = client.get("/signals/count")

    assert response.status_code == 200
    data = response.json()
    for key in ("total", "P1", "P2", "P3", "P4", "uninvestigated"):
        assert key in data, f"Missing key: {key}"
        assert isinstance(data[key], int), (
            f"{key} must be int, got {type(data[key])}: {data[key]}"
        )


@pytest.mark.unit
def test_list_signals_priority_filter(client):
    """
    GET /signals?priority=P1 must return only P1 signals.
    Uses a side_effect that simulates the service-layer filtering so the
    test verifies the router passes priority correctly and the result is filtered.
    """
    def _filtered_signals(priority=None, limit=200, offset=0, search=None):
        if priority:
            return [s for s in _SAMPLE_SIGNALS if s.get("priority") == priority]
        return _SAMPLE_SIGNALS

    with patch("app.routers.signals.get_all_signals", side_effect=_filtered_signals):
        response = client.get("/signals?priority=P1")

    assert response.status_code == 200
    data = response.json()
    assert len(data) > 0, "Expected at least one P1 signal in response"
    assert all(item["priority"] == "P1" for item in data), (
        f"Non-P1 signal found: {[i['priority'] for i in data]}"
    )


@pytest.mark.unit
def test_get_brief_returns_404_when_not_found(client):
    """
    GET /signals/{drug_key}/{pt}/brief must return 404 when no SafetyBrief
    exists for the signal (Agent 3 has not run yet for this drug-reaction pair).
    """
    with patch("app.routers.signals.get_safety_brief", return_value=None):
        response = client.get("/signals/bupropion/seizure/brief")

    assert response.status_code == 404
