"""
tests/test_smoke.py — Smoke Tests for MedSignal

Smoke tests verify that critical system components are alive and accessible.
These are the first tests to run after deployment or configuration changes.

If smoke tests fail, the system is not ready for further testing.

Test Categories:
  1. Application Bootstrap — Can FastAPI start?
  2. Database Connectivity — Snowflake, Redis, ChromaDB reachable?
  3. API Health — All endpoints respond?
  4. Core Services — Agents, models, cache functional?

Run with:
    pytest tests/test_smoke.py -v
    pytest tests/test_smoke.py -v --tb=short  # Less verbose on failure

Expected runtime: < 30 seconds
"""

import os
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def app():
    """
    Load FastAPI application.

    Scope: module — app is created once for all tests in this file.
    This is faster than creating a new app for each test.
    """
    from app.main import app as fastapi_app
    return fastapi_app


@pytest.fixture(scope="module")
def client(app):
    """
    TestClient for making HTTP requests to FastAPI.

    TestClient runs the app in-process (no server needed).
    Requests are synchronous and instant.
    """
    return TestClient(app)


@pytest.fixture(scope="module")
def snowflake_conn():
    """
    Snowflake connection for database smoke tests.

    Yields connection, then closes it after all tests complete.
    Scope: module — one connection shared across all tests.
    """
    from app.utils.snowflake_client import get_conn

    conn = get_conn()
    yield conn

    # Cleanup
    conn.close()


@pytest.fixture(scope="module")
def redis_client():
    """
    Redis client for cache smoke tests.

    Returns the internal Redis client (may be None if Redis unavailable).
    Tests should handle None gracefully.
    """
    from app.utils.redis_client import _get_client
    return _get_client()


@pytest.fixture(scope="module")
def chromadb_client():
    """
    ChromaDB client for vector store smoke tests.

    Returns the ChromaDB client instance.
    """
    from app.utils.chromadb_client import get_client
    return get_client()


# ── 1. Application Bootstrap Tests ───────────────────────────────────────────


class TestApplicationBootstrap:
    """Verify FastAPI application can start and basic routes work."""

    def test_app_imports_successfully(self, app):
        """
        Test that app.main can be imported without errors.

        Why: Import errors indicate missing dependencies or syntax errors.
        """
        assert app is not None, "FastAPI app failed to import"
        assert hasattr(app, "routes"), "App has no routes attribute"

    def test_app_has_expected_routers(self, app):
        """
        Test that all required routers are registered.

        Why: Missing routers = broken endpoints.
        Expected routers: health, signals, hitl, evaluation
        """
        # Extract all route paths from app
        routes = [route.path for route in app.routes]

        # Critical endpoints that must exist
        required_prefixes = [
            "/health",
            "/prometheus",
            "/metrics",
            "/signals",
            "/hitl",
            "/evaluation"
        ]

        for prefix in required_prefixes:
            matching = [r for r in routes if r.startswith(prefix)]
            assert len(matching) > 0, (
                f"No routes found with prefix '{prefix}'. "
                f"Router may not be registered. Found routes: {routes}"
            )

    def test_app_has_cors_middleware(self, app):
        """
        Test that CORS middleware is configured.

        Why: Streamlit runs on different port and needs CORS enabled.
        """
        middleware_types = [type(m).__name__ for m in app.user_middleware]
        assert "CORSMiddleware" in middleware_types, (
            "CORS middleware not configured — Streamlit requests will fail"
        )


# ── 2. Database Connectivity Tests ───────────────────────────────────────────


class TestDatabaseConnectivity:
    """Verify all databases are reachable and credentials work."""

    def test_snowflake_connection_alive(self, snowflake_conn):
        """
        Test that Snowflake connection works and can execute queries.

        Why: All data reads/writes depend on Snowflake.
        Method: Execute lightweight query (SELECT 1).
        """
        cursor = snowflake_conn.cursor()
        cursor.execute("SELECT 1 AS test_value")
        result = cursor.fetchone()
        cursor.close()

        assert result is not None, "Snowflake query returned no result"
        assert result[0] == 1, f"Expected 1, got {result[0]}"

    def test_snowflake_has_required_tables(self, snowflake_conn):
        """
        Test that critical tables exist in Snowflake.

        Why: Missing tables = pipeline hasn't run or schema migration failed.
        Tables checked: signals_flagged, safety_briefs, hitl_decisions
        """
        cursor = snowflake_conn.cursor()

        required_tables = [
            "drug_reaction_pairs",
            "signals_flagged",
            "safety_briefs",
            "hitl_decisions",
            "rxnorm_cache"
        ]

        # Query information_schema for table names
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = CURRENT_SCHEMA()
        """)
        existing_tables = {row[0].lower() for row in cursor.fetchall()}
        cursor.close()

        missing = [t for t in required_tables if t.lower() not in existing_tables]
        assert len(missing) == 0, (
            f"Missing required tables: {missing}. "
            f"Run migrations or Spark pipelines first. "
            f"Found tables: {existing_tables}"
        )

    def test_snowflake_has_hallucination_columns(self, snowflake_conn):
        """
        Test that safety_briefs has hallucination detection columns.

        Why: New feature requires schema migration.
        Expected columns: hallucination_score, hallucination_pass, hallucination_flags
        """
        cursor = snowflake_conn.cursor()

        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = CURRENT_SCHEMA()
              AND table_name = 'SAFETY_BRIEFS'
        """)
        columns = {row[0].lower() for row in cursor.fetchall()}
        cursor.close()

        required_columns = ["hallucination_score", "hallucination_pass", "hallucination_flags"]
        missing = [c for c in required_columns if c not in columns]

        assert len(missing) == 0, (
            f"safety_briefs missing hallucination columns: {missing}. "
            f"Run: migrations/add_hallucination_columns.sql"
        )

    def test_redis_connection_alive(self, redis_client):
        """
        Test that Redis is reachable and can execute commands.

        Why: Redis caching speeds up Streamlit pages significantly.
        Note: Redis is optional — if unavailable, app falls back to Snowflake.
        """
        if redis_client is None:
            pytest.skip("Redis not configured — caching disabled, but app still works")

        # Test ping
        pong = redis_client.ping()
        assert pong is True, "Redis PING failed"

        # Test basic set/get
        test_key = "medsignal:smoke_test"
        redis_client.setex(test_key, 10, "alive")  # 10s TTL
        value = redis_client.get(test_key)
        redis_client.delete(test_key)  # Cleanup

        assert value == "alive", f"Redis set/get failed: expected 'alive', got '{value}'"

    def test_chromadb_connection_alive(self, chromadb_client):
        """
        Test that ChromaDB is accessible and can list collections.

        Why: Agent 2 requires ChromaDB for literature retrieval.
        Expected: pubmed_abstracts collection exists.
        """
        # List all collections
        collections = chromadb_client.list_collections()
        collection_names = [c.name for c in collections]

        assert "pubmed_abstracts" in collection_names, (
            f"ChromaDB missing 'pubmed_abstracts' collection. "
            f"Run: python app/scripts/load_pubmed.py. "
            f"Found collections: {collection_names}"
        )

    def test_chromadb_has_abstracts(self, chromadb_client):
        """
        Test that pubmed_abstracts collection contains data.

        Why: Empty collection = Agent 2 will return zero results.
        Expected: At least 1,800 abstracts across 10 golden drugs.
        """
        collection = chromadb_client.get_collection("pubmed_abstracts")
        count = collection.count()

        assert count > 0, "ChromaDB pubmed_abstracts collection is empty"
        assert count >= 1800, (
            f"ChromaDB has only {count} abstracts. "
            f"Expected at least 1,800 for 10 golden drugs. "
            f"Re-run: python app/scripts/load_pubmed.py"
        )


# ── 3. API Health Tests ──────────────────────────────────────────────────────


class TestAPIHealth:
    """Verify all API endpoints are reachable and return expected status codes."""

    def test_health_endpoint_returns_ok(self, client):
        """
        Test GET /health returns 200 OK with Snowflake version.

        Why: This is the liveness check for deployment health.
        """
        response = client.get("/health")

        assert response.status_code == 200, (
            f"Health check failed with {response.status_code}: {response.text}"
        )

        data = response.json()
        assert data.get("status") == "ok", f"Health status not OK: {data}"
        assert "snowflake_version" in data, "Health response missing Snowflake version"

    def test_metrics_endpoint_returns_json(self, client):
        """
        Test GET /metrics returns JSON metrics for Streamlit dashboard.

        Why: Streamlit metrics page depends on this endpoint.
        """
        response = client.get("/metrics")

        assert response.status_code == 200, (
            f"Metrics endpoint failed with {response.status_code}"
        )

        data = response.json()
        required_metrics = [
            "signals_flagged",
            "safety_briefs",
            "hitl_decisions",
            "queue_depth"
        ]

        for metric in required_metrics:
            assert metric in data, f"Metrics response missing '{metric}'"

    def test_prometheus_endpoint_returns_text_format(self, client):
        """
        Test GET /prometheus returns Prometheus exposition format.

        Why: Prometheus scraper expects text/plain, not JSON.
        """
        response = client.get("/prometheus")

        assert response.status_code == 200, "Prometheus endpoint failed"
        assert "text/plain" in response.headers.get("content-type", ""), (
            "Prometheus endpoint should return text/plain"
        )

        # Check for expected metric names in output
        text = response.text
        expected_metrics = [
            "medsignal_signals_flagged_total",
            "medsignal_hitl_queue_depth"
        ]

        for metric in expected_metrics:
            assert metric in text, f"Prometheus output missing metric '{metric}'"

    def test_signals_endpoint_returns_list(self, client):
        """
        Test GET /signals returns list of signals.

        Why: Streamlit Signal Feed depends on this endpoint.
        Note: May return empty list if Branch 2 hasn't run yet.
        """
        response = client.get("/signals?limit=10")

        assert response.status_code == 200, "Signals endpoint failed"

        data = response.json()
        assert isinstance(data, list), f"Expected list, got {type(data)}"
        # Not asserting length > 0 because fresh DB may be empty

    def test_hitl_queue_endpoint_accessible(self, client):
        """
        Test GET /hitl/queue is accessible.

        Why: HITL workflow depends on queue endpoint.
        """
        response = client.get("/hitl/queue")
        assert response.status_code == 200, "HITL queue endpoint failed"

    def test_evaluation_endpoints_accessible(self, client):
        """
        Test all evaluation endpoints are accessible.

        Why: Evaluation dashboard depends on these endpoints.
        """
        endpoints = [
            "/evaluation/summary",
            "/evaluation/lead-times",
            "/evaluation/precision-recall"
        ]

        for endpoint in endpoints:
            response = client.get(endpoint)
            assert response.status_code == 200, (
                f"Evaluation endpoint {endpoint} failed with {response.status_code}"
            )


# ── 4. Core Services Tests ───────────────────────────────────────────────────


class TestCoreServices:
    """Verify core application services are functional."""

    def test_embedding_model_loads(self):
        """
        Test that sentence-transformers model can load.

        Why: Agent 2 and hallucination checks require this model.
        Model: all-MiniLM-L6-v2 (384-dim embeddings)
        """
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer("all-MiniLM-L6-v2")

        # Test encode
        embedding = model.encode("test sentence")

        assert embedding is not None, "Model encoding returned None"
        assert len(embedding) == 384, f"Expected 384-dim embedding, got {len(embedding)}"

    def test_openai_client_initializes(self):
        """
        Test that OpenAI client can initialize.

        Why: Agents 1 and 3 require OpenAI for GPT-4o calls.
        Note: Does NOT test API call (costs money), only client creation.
        """
        from openai import OpenAI

        client = OpenAI()  # Reads OPENAI_API_KEY from env

        assert client is not None, "OpenAI client initialization failed"
        assert hasattr(client, "chat"), "OpenAI client missing chat attribute"

    def test_pydantic_validation_works(self):
        """
        Test that SafetyBrief Pydantic model validates correctly.

        Why: Agent 3 relies on Pydantic v2 for output validation.
        """
        from app.models.brief import SafetyBriefOutput
        from pydantic import ValidationError

        # Valid brief should pass
        valid_brief = SafetyBriefOutput(
            drug_key="warfarin",
            pt="skin necrosis",
            brief_text="Test brief",
            key_findings=["Finding 1"],
            pmids_cited=["12345678"],
            recommended_action="MONITOR",
            stat_score=0.5,
            lit_score=0.3,
            priority="P3",
            generated_at="2024-01-01T00:00:00Z"
        )

        assert valid_brief.drug_key == "warfarin"

        # Invalid brief should raise ValidationError
        with pytest.raises(ValidationError):
            SafetyBriefOutput(
                drug_key="test",
                pt="test",
                brief_text="test",
                key_findings=[],
                pmids_cited=[],
                recommended_action="INVALID_ACTION",  # Should fail - not in Literal
                stat_score=0.5,
                lit_score=0.3,
                priority="P3",
                generated_at="2024-01-01T00:00:00Z"
            )

    def test_hallucination_check_imports(self):
        """
        Test that hallucination_check module can be imported.

        Why: Agent 3 calls this after generating briefs.
        """
        from evaluation.hallucination_check import validate_brief

        assert validate_brief is not None, "validate_brief function not found"
        assert callable(validate_brief), "validate_brief is not callable"


# ── Test Execution Summary ───────────────────────────────────────────────────


if __name__ == "__main__":
    """
    Run smoke tests directly with:
        python tests/test_smoke.py

    This uses pytest's main() to run tests programmatically.
    """
    import pytest

    exit_code = pytest.main([
        __file__,
        "-v",              # Verbose output
        "--tb=short",      # Short traceback format
        "--durations=10",  # Show 10 slowest tests
    ])

    if exit_code == 0:
        print("\n" + "="*80)
        print("✓ ALL SMOKE TESTS PASSED — System is ready for further testing")
        print("="*80)
    else:
        print("\n" + "="*80)
        print("✗ SMOKE TESTS FAILED — Fix critical issues before proceeding")
        print("="*80)

    sys.exit(exit_code)
