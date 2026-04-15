"""
tests/test_agent2.py — Agent 2 full test suite

Section 1: Unit tests — no ChromaDB needed, pure logic only
Section 2: Live tests — requires ChromaDB loaded with 1800+ abstracts

Run all  : .venv\Scripts\python.exe -m pytest tests/test_agent2.py -v -s
Run unit : .venv\Scripts\python.exe -m pytest tests/test_agent2.py -v -s -m unit
Run live : .venv\Scripts\python.exe -m pytest tests/test_agent2.py -v -s -m live
"""

import pytest
from app.agents.agent2_retriever import (
    reciprocal_rank_fusion,
    compute_lit_score,
    agent2_node,
    _get_collection,
)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Unit tests (no ChromaDB, no model, no BM25 — pure logic)
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.unit
def test_lit_score_empty():
    """Zero abstracts must return exactly 0.0."""
    assert compute_lit_score([]) == 0.0


@pytest.mark.unit
def test_lit_score_five_perfect():
    """
    Five abstracts with distance=0.05 should score > 0.9.
    relevance = 1.0 - (0.05/1.5) = 0.967
    volume    = 5/5 = 1.0
    LitScore  = 0.967*0.70 + 1.0*0.30 = 0.977
    """
    abstracts = [{"distance": 0.05, "pmid": str(i), "text": "x"} for i in range(5)]
    assert compute_lit_score(abstracts) > 0.9


@pytest.mark.unit
def test_lit_score_one_lower_than_five():
    """One abstract scores lower than five at same similarity."""
    one  = compute_lit_score([{"distance": 0.05, "pmid": "1", "text": "x"}])
    five = compute_lit_score([{"distance": 0.05, "pmid": str(i), "text": "x"} for i in range(5)])
    assert one < five


@pytest.mark.unit
def test_rrf_multi_query_wins():
    """Paper in two result sets ranks above paper in one result set."""
    results_1 = [
        {"pmid": "paperA", "distance": 0.30, "similarity": 0.70, "text": "x", "drug_name": "dupilumab"},
        {"pmid": "paperB", "distance": 0.35, "similarity": 0.65, "text": "x", "drug_name": "dupilumab"},
    ]
    results_2 = [
        {"pmid": "paperA", "distance": 0.28, "similarity": 0.72, "text": "x", "drug_name": "dupilumab"},
        {"pmid": "paperC", "distance": 0.38, "similarity": 0.62, "text": "x", "drug_name": "dupilumab"},
    ]
    fused   = reciprocal_rank_fusion([results_1, results_2])
    paper_a = next(p for p in fused if p["pmid"] == "paperA")
    paper_b = next(p for p in fused if p["pmid"] == "paperB")
    assert fused[0]["pmid"] == "paperA"
    assert paper_a["rrf_score"] > paper_b["rrf_score"]


@pytest.mark.unit
def test_rrf_no_duplicates():
    """Same PMID from two result sets appears only once in output."""
    r1    = [{"pmid": "12345", "distance": 0.30, "similarity": 0.70, "text": "x", "drug_name": "dupilumab"}]
    r2    = [{"pmid": "12345", "distance": 0.28, "similarity": 0.72, "text": "x", "drug_name": "dupilumab"}]
    fused = reciprocal_rank_fusion([r1, r2])
    assert [r["pmid"] for r in fused].count("12345") == 1


@pytest.mark.unit
def test_rrf_keeps_best_similarity():
    """When paper appears in two results, best similarity is kept."""
    r1      = [{"pmid": "paperA", "distance": 0.40, "similarity": 0.60, "text": "x", "drug_name": "dupilumab"}]
    r2      = [{"pmid": "paperA", "distance": 0.25, "similarity": 0.75, "text": "x", "drug_name": "dupilumab"}]
    fused   = reciprocal_rank_fusion([r1, r2])
    paper_a = next(p for p in fused if p["pmid"] == "paperA")
    assert paper_a["distance"] == 0.25


@pytest.mark.unit
def test_rrf_hnsw_and_bm25_same_paper_scores_highest():
    """
    Paper found by both HNSW and BM25 must rank above paper found by only one.
    This validates that hybrid retrieval correctly rewards cross-retriever papers.
    """
    hnsw_results = [
        {"pmid": "paperA", "distance": 0.30, "similarity": 0.70, "text": "x", "drug_name": "dupilumab", "retriever": "hnsw"},
        {"pmid": "paperB", "distance": 0.32, "similarity": 0.68, "text": "x", "drug_name": "dupilumab", "retriever": "hnsw"},
    ]
    bm25_results = [
        {"pmid": "paperA", "distance": 0.35, "similarity": 0.65, "text": "x", "drug_name": "dupilumab", "retriever": "bm25"},
        {"pmid": "paperC", "distance": 0.38, "similarity": 0.62, "text": "x", "drug_name": "dupilumab", "retriever": "bm25"},
    ]
    fused   = reciprocal_rank_fusion([hnsw_results, bm25_results])
    paper_a = next(p for p in fused if p["pmid"] == "paperA")
    paper_b = next(p for p in fused if p["pmid"] == "paperB")
    paper_c = next(p for p in fused if p["pmid"] == "paperC")
    # paperA found by both → must rank first
    assert fused[0]["pmid"] == "paperA"
    assert paper_a["rrf_score"] > paper_b["rrf_score"]
    assert paper_a["rrf_score"] > paper_c["rrf_score"]


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Live tests (requires ChromaDB loaded with 1800+ abstracts)
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.live
def test_chromadb_loaded():
    """ChromaDB must have 1800+ abstracts before any live test runs."""
    count = _get_collection().count()
    print(f"\nChromaDB abstracts: {count}")
    assert count >= 1800, (
        f"ChromaDB has only {count} abstracts. "
        f"Siddharth needs to run load_pubmed.py first."
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

    # BM25 must find at least some papers HNSW does not
    # If this fails, BM25 adds nothing — investigate why
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