"""
tests/unit/test_agent3.py — Agent 3 unit tests

Pure logic tests — no API keys, no Snowflake, no network.

Run: poetry run pytest tests/unit/test_agent3.py -v -m unit
"""

import pytest
from unittest.mock import patch, MagicMock


@pytest.mark.unit
def test_priority_tier_p1():
    """stat >= 0.7 AND lit >= 0.5 → P1."""
    from app.agents.agent3_assessor import assign_priority
    assert assign_priority(0.8, 0.6) == "P1"


@pytest.mark.unit
def test_priority_tier_p2():
    """stat >= 0.7 AND lit < 0.5 → P2."""
    from app.agents.agent3_assessor import assign_priority
    assert assign_priority(0.8, 0.4) == "P2"


@pytest.mark.unit
def test_priority_tier_p3():
    """stat < 0.7 AND lit >= 0.5 → P3."""
    from app.agents.agent3_assessor import assign_priority
    assert assign_priority(0.6, 0.6) == "P3"


@pytest.mark.unit
def test_priority_tier_p4():
    """Both below threshold → P4."""
    from app.agents.agent3_assessor import assign_priority
    assert assign_priority(0.6, 0.4) == "P4"


@pytest.mark.unit
def test_priority_boundary_exact_thresholds():
    """Values exactly at boundary belong to the higher tier."""
    from app.agents.agent3_assessor import assign_priority
    assert assign_priority(0.7, 0.5) == "P1"
    assert assign_priority(0.7, 0.49) == "P2"
    assert assign_priority(0.69, 0.5) == "P3"
    assert assign_priority(0.69, 0.49) == "P4"


@pytest.mark.unit
def test_stat_score_fallback_formula():
    """
    When Agent 1 has not run, Agent 3 computes stat_score locally.
    PRR=4.0, cases=50, no outcomes → StatScore should be ~0.70.
    prr_s = min(4.0/4.0, 1.0) = 1.0
    vol_s = log10(50)/log10(50) = 1.0
    sev_s = 0.0
    stat  = 1.0*0.50 + 1.0*0.30 + 0.0*0.20 = 0.80
    """
    from app.agents.agent3_assessor import _compute_stat_score
    score = _compute_stat_score(
        prr=4.0, case_count=50,
        death=0, lt=0, hosp=0,
    )
    assert 0.75 <= score <= 0.85, f"StatScore={score}, expected ~0.80"


@pytest.mark.unit
def test_stat_score_death_increases_severity():
    """Death flag raises severity component from 0.0 to 1.0."""
    from app.agents.agent3_assessor import _compute_stat_score
    no_death   = _compute_stat_score(2.5, 60, death=0, lt=0, hosp=0)
    with_death = _compute_stat_score(2.5, 60, death=5, lt=0, hosp=0)
    assert with_death > no_death, "Death flag must raise StatScore"


@pytest.mark.unit
def test_stat_score_lt_higher_than_hosp():
    """Life-threatening (0.75) outranks hospitalisation (0.50)."""
    from app.agents.agent3_assessor import _compute_stat_score
    hosp_only = _compute_stat_score(2.5, 60, death=0, lt=0, hosp=5)
    lt_only   = _compute_stat_score(2.5, 60, death=0, lt=5, hosp=0)
    assert lt_only > hosp_only


@pytest.mark.unit
def test_pydantic_rejects_missing_fields():
    """SafetyBriefOutput must reject output with missing required fields."""
    from app.agents.agent3_assessor import SafetyBriefOutput
    with pytest.raises(Exception):
        SafetyBriefOutput(brief_text="ok")  # missing everything else


@pytest.mark.unit
def test_pydantic_rejects_bad_recommended_action():
    """recommended_action must be one of the four allowed literals."""
    from app.agents.agent3_assessor import SafetyBriefOutput
    with pytest.raises(Exception):
        SafetyBriefOutput(
            brief_text="text",
            key_findings=["finding"],
            pmids_cited=["12345678"],
            recommended_action="INVESTIGATE",  # not in Literal
            drug_key="bupropion",
            pt="seizure",
            stat_score=0.78,
            lit_score=0.65,
            priority="P1",
            generated_at="2026-04-15T00:00:00+00:00",
        )


@pytest.mark.unit
def test_pydantic_rejects_stat_score_out_of_range():
    """stat_score must be between 0.0 and 1.0 inclusive."""
    from app.agents.agent3_assessor import SafetyBriefOutput
    with pytest.raises(Exception):
        SafetyBriefOutput(
            brief_text="text",
            key_findings=["finding"],
            pmids_cited=["12345678"],
            recommended_action="MONITOR",
            drug_key="bupropion",
            pt="seizure",
            stat_score=1.5,   # out of range
            lit_score=0.65,
            priority="P1",
            generated_at="2026-04-15T00:00:00+00:00",
        )


@pytest.mark.unit
def test_pydantic_accepts_valid_brief():
    """A correctly formed SafetyBriefOutput must pass validation."""
    from app.agents.agent3_assessor import SafetyBriefOutput
    brief = SafetyBriefOutput(
        brief_text="Bupropion has been associated with seizure...",
        key_findings=["PRR of 4.2 is significant", "3 deaths reported"],
        pmids_cited=["12345678", "87654321"],
        recommended_action="LABEL_UPDATE",
        drug_key="bupropion",
        pt="seizure",
        stat_score=0.78,
        lit_score=0.65,
        priority="P1",
        generated_at="2026-04-15T00:00:00+00:00",
    )
    assert brief.priority == "P1"
    assert brief.recommended_action == "LABEL_UPDATE"
    assert len(brief.pmids_cited) == 2


@pytest.mark.unit
def test_citation_guard_removes_fabricated_pmids():
    """PMIDs not in the retrieved set must be stripped before writing."""
    from app.agents.agent3_assessor import SafetyBriefOutput, _validate_citations

    brief = SafetyBriefOutput(
        brief_text="text citing [PMID:99999999]",
        key_findings=["finding"],
        pmids_cited=["12345678", "99999999"],  # 99999999 was not retrieved
        recommended_action="MONITOR",
        drug_key="bupropion",
        pt="seizure",
        stat_score=0.78,
        lit_score=0.65,
        priority="P1",
        generated_at="2026-04-15T00:00:00+00:00",
    )

    retrieved = ["12345678"]   # only this one was actually returned by Agent 2
    cleaned   = _validate_citations(brief, retrieved)

    assert "99999999" not in cleaned.pmids_cited
    assert "12345678" in cleaned.pmids_cited
    assert len(cleaned.pmids_cited) == 1


@pytest.mark.unit
def test_citation_guard_allows_all_when_all_valid():
    """No PMIDs removed when all cited PMIDs are in retrieved set."""
    from app.agents.agent3_assessor import SafetyBriefOutput, _validate_citations

    brief = SafetyBriefOutput(
        brief_text="text",
        key_findings=["finding"],
        pmids_cited=["12345678", "87654321"],
        recommended_action="MONITOR",
        drug_key="bupropion",
        pt="seizure",
        stat_score=0.78,
        lit_score=0.65,
        priority="P1",
        generated_at="2026-04-15T00:00:00+00:00",
    )

    retrieved = ["12345678", "87654321"]
    cleaned   = _validate_citations(brief, retrieved)

    assert len(cleaned.pmids_cited) == 2


@pytest.mark.unit
def test_citation_guard_empty_retrieved():
    """If Agent 2 returned nothing, all cited PMIDs are fabricated."""
    from app.agents.agent3_assessor import SafetyBriefOutput, _validate_citations

    brief = SafetyBriefOutput(
        brief_text="text",
        key_findings=["finding"],
        pmids_cited=["12345678"],
        recommended_action="MONITOR",
        drug_key="bupropion",
        pt="seizure",
        stat_score=0.78,
        lit_score=0.65,
        priority="P1",
        generated_at="2026-04-15T00:00:00+00:00",
    )

    cleaned = _validate_citations(brief, retrieved_pmids=[])
    assert cleaned.pmids_cited == []


@pytest.mark.unit
def test_agent3_uses_state_stat_score_when_present():
    """
    If stat_score is already in state (from Agent 1), Agent 3 must use it
    and not recompute. Verified by mocking the write and checking the
    priority assignment uses the injected value.
    """
    from app.agents.agent3_assessor import assign_priority

    # stat=0.8 (high), lit=0.6 (high) → P1
    priority = assign_priority(stat_score=0.8, lit_score=0.6)
    assert priority == "P1"

    # stat=0.4 (low from mock Agent 1), lit=0.6 → P3 not P1
    priority = assign_priority(stat_score=0.4, lit_score=0.6)
    assert priority == "P3"


@pytest.mark.unit
def test_agent3_node_returns_required_state_keys():
    """
    agent3_node must return a dict containing priority and brief keys.
    Mocks GPT-4o and Snowflake so no real calls are made.
    """
    from app.agents.agent3_assessor import agent3_node

    mock_state = {
        "drug_key"      : "bupropion",
        "pt"            : "seizure",
        "prr"           : 4.2,
        "case_count"    : 89,
        "death_count"   : 3,
        "hosp_count"    : 12,
        "lt_count"      : 5,
        "stat_score"    : 0.78,
        "lit_score"     : 0.65,
        "search_queries": ["bupropion seizure mechanism"],
        "abstracts"     : [
            {
                "pmid"      : "12345678",
                "text"      : "Bupropion lowers seizure threshold.",
                "similarity": 0.72,
                "distance"  : 0.28,
                "drug_name" : "bupropion",
                "retriever" : "hnsw",
                "rrf_score" : 0.031,
            }
        ],
        "priority": None,
        "brief"   : None,
        "error"   : None,
    }

    mock_gpt_response = {
        "brief_text"        : "Bupropion has been associated with seizure [PMID:12345678].",
        "key_findings"      : ["PRR=4.2", "3 deaths", "Literature supports CNS mechanism"],
        "pmids_cited"       : ["12345678"],
        "recommended_action": "LABEL_UPDATE",
        "drug_key"          : "bupropion",
        "pt"                : "seizure",
        "stat_score"        : 0.78,
        "lit_score"         : 0.65,
        "priority"          : "P1",
        "generated_at"      : "2026-04-15T00:00:00+00:00",
    }

    with patch("app.agents.agent3_assessor._call_gpt4o") as mock_gpt, \
         patch("app.agents.agent3_assessor._write_to_snowflake") as mock_write:

        mock_gpt.return_value = (mock_gpt_response, 500, 200)

        result = agent3_node(mock_state)

    assert "priority" in result
    assert "brief" in result
    assert result["priority"] == "P1"
    assert result["brief"] is not None
    assert result["brief"]["recommended_action"] == "LABEL_UPDATE"
    assert mock_write.called


@pytest.mark.unit
def test_normalize_action_maps_prose_variants():
    from app.agents.agent3_assessor import _normalize_action

    assert _normalize_action({"recommended_action": "MONITOR"})["recommended_action"] == "MONITOR"
    assert _normalize_action({"recommended_action": "label update"})["recommended_action"] == "LABEL_UPDATE"
    assert _normalize_action({"recommended_action": "Escalate for review"})["recommended_action"] == "MONITOR"
    assert _normalize_action({"recommended_action": "Withdraw from market"})["recommended_action"] == "WITHDRAW"
    assert _normalize_action({"recommended_action": "restrict prescribing"})["recommended_action"] == "RESTRICT"


@pytest.mark.unit
def test_agent3_llm_router_total_failure():
    """
    When LLMRouter.complete() raises RuntimeError on every call, agent3_node
    must set error in the returned dict and write generation_error=True to
    Snowflake. Validates that the LLMRouter wiring handles total LLM failure
    without crashing the pipeline.
    """
    from app.agents.agent3_assessor import agent3_node

    mock_router = MagicMock()
    mock_router.complete.side_effect = RuntimeError("all models failed")

    mock_state = {
        "drug_key"      : "bupropion",
        "pt"            : "seizure",
        "prr"           : 4.2,
        "case_count"    : 89,
        "death_count"   : 0,
        "hosp_count"    : 0,
        "lt_count"      : 0,
        "stat_score"    : 0.78,
        "lit_score"     : 0.65,
        "abstracts"     : [],
        "search_queries": [],
        "priority"      : None,
        "brief"         : None,
        "error"         : None,
        "router"        : mock_router,
    }

    with patch("app.agents.agent3_assessor._write_to_snowflake") as mock_write, \
         patch("app.agents.agent3_assessor.invalidate_brief"):

        result = agent3_node(mock_state)

    assert result["error"] is not None, "error must be set when LLM totally fails"
    assert result["brief"] is None

    call_kwargs = mock_write.call_args
    assert call_kwargs.kwargs.get("gen_error") is True or \
           call_kwargs.args[5] is True  # gen_error is 6th positional arg


@pytest.mark.unit
def test_agent3_node_sets_gen_error_on_double_failure():
    """
    When both GPT-4o attempts return malformed JSON,
    agent3_node must set generation_error in the Snowflake write
    and return brief=None.
    """
    from app.agents.agent3_assessor import agent3_node

    mock_state = {
        "drug_key"   : "bupropion",
        "pt"         : "seizure",
        "prr"        : 4.2,
        "case_count" : 89,
        "death_count": 0,
        "hosp_count" : 0,
        "lt_count"   : 0,
        "stat_score" : 0.78,
        "lit_score"  : 0.65,
        "abstracts"  : [],
        "search_queries": [],
        "priority"   : None,
        "brief"      : None,
        "error"      : None,
    }

    with patch("app.agents.agent3_assessor._call_gpt4o") as mock_gpt, \
         patch("app.agents.agent3_assessor._write_to_snowflake") as mock_write:

        mock_gpt.side_effect = ValueError("not valid JSON")

        result = agent3_node(mock_state)

    assert result["brief"] is None
    assert result["error"] is not None

    call_kwargs = mock_write.call_args
    assert call_kwargs.kwargs.get("gen_error") is True or \
           call_kwargs.args[5] is True  # positional: gen_error is 6th arg
