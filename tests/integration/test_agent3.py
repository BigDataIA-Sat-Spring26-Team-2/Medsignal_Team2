"""
tests/integration/test_agent3.py — Agent 3 integration test

Calls real GPT-4o and writes to real Snowflake.
Requires OPENAI_API_KEY and SNOWFLAKE_* env vars in .env.

Run: poetry run pytest tests/integration/test_agent3.py -v -s -m integration
"""

import pytest


@pytest.mark.integration
def test_agent3_full_run_bupropion():
    """
    End-to-end Agent 3 test with real GPT-4o and real Snowflake.
    Uses bupropion x seizure — a confirmed golden signal.
    Verifies: priority assigned, brief written, PMIDs valid, row in Snowflake.

    Requires: OPENAI_API_KEY, SNOWFLAKE_* env vars in .env
    """
    import os
    import snowflake.connector
    from dotenv import load_dotenv
    from app.agents.agent3_assessor import agent3_node

    load_dotenv()

    if not os.getenv("OPENAI_API_KEY") or not os.getenv("SNOWFLAKE_ACCOUNT"):
        pytest.skip("OPENAI_API_KEY or Snowflake credentials not set")

    retrieved_pmids = ["36100001", "36100002", "36100003"]

    state = {
        "drug_key"      : "bupropion",
        "pt"            : "seizure",
        "prr"           : 4.2,
        "case_count"    : 89,
        "death_count"   : 3,
        "hosp_count"    : 12,
        "lt_count"      : 5,
        "stat_score"    : 0.78,
        "lit_score"     : 0.65,
        "search_queries": [
            "bupropion seizure mechanism CNS threshold",
            "bupropion seizure incidence risk factors",
            "bupropion seizure clinical outcomes management",
        ],
        "abstracts": [
            {
                "pmid"      : "36100001",
                "text"      : (
                    "Bupropion, a norepinephrine-dopamine reuptake inhibitor, "
                    "lowers the seizure threshold in a dose-dependent manner. "
                    "Post-marketing surveillance confirms elevated seizure risk "
                    "particularly at doses above 450mg/day."
                ),
                "similarity": 0.72,
                "distance"  : 0.28,
                "drug_name" : "bupropion",
                "retriever" : "hnsw",
                "rrf_score" : 0.031,
            },
            {
                "pmid"      : "36100002",
                "text"      : (
                    "Retrospective FAERS analysis identified bupropion as a "
                    "significant disproportionality signal for seizure with "
                    "PRR=4.1 across 2019-2022 quarterly data."
                ),
                "similarity": 0.68,
                "distance"  : 0.32,
                "drug_name" : "bupropion",
                "retriever" : "bm25",
                "rrf_score" : 0.028,
            },
            {
                "pmid"      : "36100003",
                "text"      : (
                    "Clinical management of bupropion-associated seizures "
                    "requires immediate dose reduction. Most cases resolve "
                    "without permanent neurological sequelae."
                ),
                "similarity": 0.63,
                "distance"  : 0.37,
                "drug_name" : "bupropion",
                "retriever" : "hnsw",
                "rrf_score" : 0.024,
            },
        ],
        "priority": None,
        "brief"   : None,
        "error"   : None,
    }

    result = agent3_node(state)

    assert result["priority"] in ["P1", "P2", "P3", "P4"]
    assert result["brief"] is not None, "Brief should not be None for a valid signal"
    assert result["error"] is None, f"Unexpected error: {result['error']}"

    brief = result["brief"]

    assert isinstance(brief["brief_text"], str) and len(brief["brief_text"]) > 50
    assert isinstance(brief["key_findings"], list) and len(brief["key_findings"]) > 0
    assert isinstance(brief["pmids_cited"], list)
    assert brief["recommended_action"] in [
        "MONITOR", "LABEL_UPDATE", "RESTRICT", "WITHDRAW"
    ]

    for pmid in brief["pmids_cited"]:
        assert pmid in retrieved_pmids, (
            f"PMID {pmid} was cited but not retrieved — citation guard failed"
        )

    assert result["priority"] == "P1", (
        f"Expected P1 for stat=0.78 lit=0.65, got {result['priority']}"
    )

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
        "SELECT priority, generation_error FROM safety_briefs "
        "WHERE drug_key = %s AND pt = %s",
        ("bupropion", "seizure"),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    assert row is not None, "No row found in safety_briefs — Snowflake write failed"
    assert row[0] == "P1"
    assert row[1] is False

    print(f"\n✓ Priority    : {result['priority']}")
    print(f"✓ Action      : {brief['recommended_action']}")
    print(f"✓ PMIDs cited : {brief['pmids_cited']}")
    print(f"✓ Key findings: {len(brief['key_findings'])}")
    print(f"✓ Brief text  : {brief['brief_text'][:120]}...")
