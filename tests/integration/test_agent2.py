"""
tests/integration/test_agent2.py — Agent 2 integration (live) tests

Requires ChromaDB loaded with 1800+ abstracts.

Run: poetry run pytest tests/integration/test_agent2.py -v -s -m live
"""

import pytest
from app.agents.agent2_retriever import (
    agent2_node,
    _get_collection,
)


@pytest.mark.live
def test_chromadb_loaded():
    """ChromaDB must have 1800+ abstracts before any live test runs."""
    count = _get_collection().count()
    print(f"\nChromaDB abstracts: {count}")
    assert count >= 1800, (
        f"ChromaDB has only {count} abstracts. "
        f" run load_pubmed.py first."
    )


@pytest.mark.live
def test_dupilumab_conjunctivitis():
    """
    dupilumab x conjunctivitis is a known golden signal.
    Hybrid retrieval must return at least 3 abstracts above threshold.
    POC confirmed scores of 0.61-0.76 for this pair.
    """
    state = {
        "drug_key"      : "dupilumab",
        "pt"            : "conjunctivitis",
        "prr"           : 5.97,
        "case_count"    : 214,
        "death_count"   : 0,
        "hosp_count"    : 12,
        "lt_count"      : 3,
        "stat_score"    : 0.82,
        "search_queries": [
            "dupilumab conjunctivitis ocular adverse effects mechanism",
            "dupilumab eye inflammation incidence epidemiology",
            "dupilumab conjunctivitis clinical outcomes risk factors",
        ],
    }

    result    = agent2_node(state)
    abstracts = result["abstracts"]
    lit_score = result["lit_score"]

    print(f"\nAbstracts returned : {len(abstracts)}")
    print(f"LitScore           : {lit_score}")
    for a in abstracts:
        print(f"  PMID {a['pmid']} | similarity={a['similarity']:.3f} | retriever={a.get('retriever','?')}")

    assert len(abstracts) >= 3, f"Expected >= 3 abstracts, got {len(abstracts)}"
    assert lit_score > 0.5, f"LitScore {lit_score} too low for known golden signal"


@pytest.mark.live
def test_gabapentin_respiratory():
    """gabapentin x respiratory arrest — hybrid retrieval validation."""
    state = {
        "drug_key"      : "gabapentin",
        "pt"            : "cardio-respiratory arrest",
        "prr"           : 3.2,
        "case_count"    : 55,
        "death_count"   : 8,
        "hosp_count"    : 20,
        "lt_count"      : 5,
        "stat_score"    : 0.75,
        "search_queries": [
            "gabapentin respiratory depression adverse effects",
            "gabapentin cardiorespiratory arrest risk mechanism",
            "gabapentin breathing problems clinical outcomes",
        ],
    }

    result = agent2_node(state)
    print(f"\ngabapentin abstracts : {len(result['abstracts'])}")
    print(f"LitScore             : {result['lit_score']}")
    for a in result["abstracts"]:
        print(f"  PMID {a['pmid']} | similarity={a['similarity']:.3f} | retriever={a.get('retriever','?')}")

    assert "abstracts" in result
    assert "lit_score" in result
    assert result["lit_score"] >= 0.0


@pytest.mark.live
def test_bm25_finds_different_papers_than_hnsw():
    """
    Validates that BM25 and HNSW find at least some different papers.
    If they return identical results, hybrid retrieval adds no value.
    This test confirms the hybrid approach is justified.
    """
    from app.agents.agent2_retriever import hnsw_search, bm25_search

    query    = "dupilumab conjunctivitis ocular adverse effects"
    drug_key = "dupilumab"

    hnsw_pmids = {r["pmid"] for r in hnsw_search(query, drug_key)}
    bm25_pmids = {r["pmid"] for r in bm25_search(query, drug_key)}

    overlap    = hnsw_pmids & bm25_pmids
    hnsw_only  = hnsw_pmids - bm25_pmids
    bm25_only  = bm25_pmids - hnsw_pmids

    print(f"\nHNSW results  : {len(hnsw_pmids)}")
    print(f"BM25 results  : {len(bm25_pmids)}")
    print(f"Overlap       : {len(overlap)}")
    print(f"HNSW only     : {len(hnsw_only)}")
    print(f"BM25 only     : {len(bm25_only)}")

    assert len(bm25_only) > 0 or len(hnsw_only) > 0, (
        "HNSW and BM25 returned identical results — "
        "hybrid retrieval provides no additional coverage"
    )


@pytest.mark.live
def test_empty_queries_graceful():
    """If Agent 1 failed and sent no queries, Agent 2 must not crash."""
    state = {
        "drug_key"      : "dupilumab",
        "pt"            : "conjunctivitis",
        "prr"           : 5.97,
        "case_count"    : 214,
        "death_count"   : 0,
        "hosp_count"    : 12,
        "lt_count"      : 3,
        "stat_score"    : 0.82,
        "search_queries": [],
    }

    result = agent2_node(state)
    print(f"\nEmpty queries: abstracts={result['abstracts']} lit_score={result['lit_score']}")
    assert result["abstracts"] == []
    assert result["lit_score"] == 0.0
