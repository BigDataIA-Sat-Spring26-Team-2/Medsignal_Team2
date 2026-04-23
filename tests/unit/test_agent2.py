"""
tests/unit/test_agent2.py — Agent 2 unit tests

Pure logic only — no ChromaDB, no embedding model, no BM25.

Run: poetry run pytest tests/unit/test_agent2.py -v -s
"""

import pytest
from app.agents.agent2_retriever import (
    reciprocal_rank_fusion,
    compute_lit_score,
)


@pytest.mark.unit
def test_lit_score_empty():
    """Zero abstracts must return exactly 0.0."""
    assert compute_lit_score([]) == 0.0


@pytest.mark.unit
def test_lit_score_five_perfect():
    """
    Five HNSW abstracts with similarity=0.95 should score > 0.9.
    relevance = avg similarity = 0.95
    volume    = 5/5 = 1.0
    LitScore  = 0.95*0.70 + 1.0*0.30 = 0.965
    """
    abstracts = [
        {"distance": 0.05, "similarity": 0.95, "retriever": "hnsw", "pmid": str(i), "text": "x"}
        for i in range(5)
    ]
    assert compute_lit_score(abstracts) > 0.9


@pytest.mark.unit
def test_lit_score_one_lower_than_five():
    """One HNSW abstract scores lower than five at same similarity (volume effect)."""
    one  = compute_lit_score([
        {"distance": 0.05, "similarity": 0.95, "retriever": "hnsw", "pmid": "1", "text": "x"}
    ])
    five = compute_lit_score([
        {"distance": 0.05, "similarity": 0.95, "retriever": "hnsw", "pmid": str(i), "text": "x"}
        for i in range(5)
    ])
    assert one < five


@pytest.mark.unit
def test_lit_score_bm25_only_uses_fallback_relevance():
    """
    BM25-only abstracts (no HNSW) must use BM25_ONLY_RELEVANCE_FALLBACK=0.65.
    With 5 BM25 abstracts: lit_score = 0.65*0.70 + 1.0*0.30 = 0.755.
    """
    abstracts = [
        {"distance": 0.10, "similarity": 0.90, "retriever": "bm25", "pmid": str(i), "text": "x"}
        for i in range(5)
    ]
    score = compute_lit_score(abstracts)
    expected = round(0.65 * 0.70 + 1.0 * 0.30, 4)
    assert score == expected, f"BM25-only fallback should be {expected}, got {score}"


@pytest.mark.unit
def test_lit_score_bm25_counts_for_volume_not_relevance():
    """
    Mixed HNSW+BM25: relevance uses only HNSW similarity, but BM25 increases volume.
    With 1 HNSW (sim=0.80) + 4 BM25: relevance=0.80, volume=5/5=1.0
    lit_score = 0.80*0.70 + 1.0*0.30 = 0.86
    vs. 1 HNSW only: volume=1/5=0.20
    lit_score_1_only = 0.80*0.70 + 0.20*0.30 = 0.62
    """
    hnsw_only = compute_lit_score([
        {"distance": 0.20, "similarity": 0.80, "retriever": "hnsw", "pmid": "1", "text": "x"}
    ])
    mixed = compute_lit_score([
        {"distance": 0.20, "similarity": 0.80, "retriever": "hnsw", "pmid": "1", "text": "x"},
        {"distance": 0.15, "similarity": 0.85, "retriever": "bm25",  "pmid": "2", "text": "x"},
        {"distance": 0.18, "similarity": 0.82, "retriever": "bm25",  "pmid": "3", "text": "x"},
        {"distance": 0.22, "similarity": 0.78, "retriever": "bm25",  "pmid": "4", "text": "x"},
        {"distance": 0.25, "similarity": 0.75, "retriever": "bm25",  "pmid": "5", "text": "x"},
    ])
    assert mixed > hnsw_only, "Adding BM25 abstracts must raise LitScore via volume"
    # Relevance unchanged: still uses only the HNSW abstract's similarity
    expected_relevance = 0.80
    expected_volume    = 1.0
    expected_mixed     = round(expected_relevance * 0.70 + expected_volume * 0.30, 4)
    assert mixed == expected_mixed, f"Expected {expected_mixed}, got {mixed}"


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
