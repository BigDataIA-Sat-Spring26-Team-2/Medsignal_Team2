"""
tests/integration/test_hitl.py — HITL router integration tests

Real Snowflake — requires SNOWFLAKE_* env vars in .env.

Run: poetry run pytest tests/integration/test_hitl.py -v -s -m integration
"""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    from app.main import app
    return TestClient(app)


@pytest.mark.integration
def test_health_endpoint_live(client):
    """
    Live health check — connects to real Snowflake.
    Returns status=ok and snowflake_version as connectivity proof.
    """
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert "snowflake_version" in response.json()


@pytest.mark.integration
def test_get_queue_live(client):
    """
    Live GET /hitl/queue against real Snowflake.
    Requires safety_briefs to have at least one row from pipeline run.
    Returns a list — may be empty if all signals already have decisions.
    """
    import os
    from dotenv import load_dotenv
    load_dotenv()

    if not os.getenv("SNOWFLAKE_ACCOUNT"):
        pytest.skip("Snowflake credentials not set")

    response = client.get("/hitl/queue")

    print(f"\nQueue depth: {len(response.json())} signals pending")
    for row in response.json()[:3]:
        print(
            f"  brief_id={row.get('brief_id')} | "
            f"{row['drug_key']} x {row['pt']} | "
            f"priority={row['priority']}"
        )

    assert response.status_code == 200
    assert isinstance(response.json(), list)


@pytest.mark.integration
def test_get_decisions_live(client):
    """
    Live GET /hitl/decisions against real Snowflake.
    Returns full audit log — may be empty if no decisions made yet.
    """
    import os
    from dotenv import load_dotenv
    load_dotenv()

    if not os.getenv("SNOWFLAKE_ACCOUNT"):
        pytest.skip("Snowflake credentials not set")

    response = client.get("/hitl/decisions")

    print(f"\nTotal decisions: {len(response.json())}")

    assert response.status_code == 200
    assert isinstance(response.json(), list)


@pytest.mark.integration
def test_post_decision_live(client):
    """
    Live POST /hitl/decisions against real Snowflake.
    Writes a real row to hitl_decisions with brief_id=None (no brief yet).
    Uses bupropion x seizure — must exist in safety_briefs first.

    Verify after running:
        SELECT * FROM hitl_decisions
        WHERE drug_key = 'bupropion' AND pt = 'seizure'
        ORDER BY decided_at DESC LIMIT 1;
    """
    import os
    import snowflake.connector
    from dotenv import load_dotenv
    load_dotenv()

    if not os.getenv("SNOWFLAKE_ACCOUNT"):
        pytest.skip("Snowflake credentials not set")

    response = client.post(
        "/hitl/decisions",
        json={
            "drug_key"     : "bupropion",
            "pt"           : "seizure",
            "decision"     : "APPROVE",
            "reviewer_note": "Integration test — strong statistical evidence",
        },
    )

    print(f"\nResponse: {response.json()}")

    assert response.status_code == 200
    body = response.json()
    assert body["status"]   == "recorded"
    assert body["decision"] == "APPROVE"

    conn = snowflake.connector.connect(
        account  =os.getenv("SNOWFLAKE_ACCOUNT"),
        user     =os.getenv("SNOWFLAKE_USER"),
        password =os.getenv("SNOWFLAKE_PASSWORD"),
        database =os.getenv("SNOWFLAKE_DATABASE"),
        schema   =os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )
    cur = conn.cursor()
    cur.execute(
        """
        SELECT decision, reviewer_note
        FROM   hitl_decisions
        WHERE  drug_key = 'bupropion'
        AND    pt       = 'seizure'
        ORDER  BY decided_at DESC
        LIMIT  1
        """,
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    assert row is not None, "Row not found in hitl_decisions — write failed"
    assert row[0] == "APPROVE"
    assert "Integration test" in row[1]

    print(f"✓ Row in Snowflake — decision={row[0]}")
