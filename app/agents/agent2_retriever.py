"""
agent2_retriever.py — MedSignal Agent 2: Literature Retriever

Role in pipeline:
    Receives three GPT-4o generated PubMed search queries from Agent 1.
    Queries ChromaDB using all-MiniLM-L6-v2 embeddings (HNSW dense retrieval).
    Also searches using BM25 sparse retrieval for exact keyword matches.
    Fuses all results using Reciprocal Rank Fusion (RRF).
    Computes LitScore from similarity and volume of returned abstracts.
    Passes top-5 abstracts and LitScore to Agent 3.

Why hybrid retrieval (HNSW + BM25 + RRF):
    HNSW finds semantically similar papers — captures meaning and concept.
    BM25 finds exact keyword matches — captures precise medical terminology.
    They catch different papers:
        HNSW: "IL-4 receptor inhibitor eye disease" (semantically close to dupilumab conjunctivitis)
        BM25: "dupilumab conjunctivitis incidence" (exact keyword match)
    In POC testing, HNSW alone missed warfarin x skin necrosis entirely.
    BM25 found it immediately because the exact terms appeared in the abstract.
    RRF fuses both — papers appearing in multiple retriever results rank highest.

Why no LLM:
    Retrieval is a deterministic similarity search — no reasoning task here.
    Adding an LLM would introduce cost, latency, and non-reproducibility
    with zero benefit. ChromaDB + BM25 handle this better.

Owner: Prachi
"""

import os
import logging
from typing import Optional

from dotenv import load_dotenv

from app.agents.state import SignalState

load_dotenv()

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

# CRITICAL: must be identical to the model used in load_pubmed.py at index time.
# If this changes, stored embeddings become incompatible with query embeddings
# and retrieval silently returns wrong results.
MODEL_NAME = "all-MiniLM-L6-v2"

# Cosine similarity threshold — abstracts below this are too generic.
# Calibrated from POC: dupilumab x conjunctivitis top abstracts scored 0.61-0.76.
SIMILARITY_THRESHOLD = 0.60

# BM25 minimum score — below this the keyword match is too weak to be useful.
BM25_MIN_SCORE = 0.1

# Maximum abstracts to pass to Agent 3.
# More than 5 inflates the GPT-4o prompt beyond useful context.
MAX_ABSTRACTS = 5

# RRF constant — standard value from literature.
# Prevents top-ranked results from dominating the fusion score.
RRF_K = 60

# ChromaDB collection name — must match what load_pubmed.py created.
COLLECTION_NAME = "pubmed_abstracts"


# ── Lazy initialization ───────────────────────────────────────────────────────
# Nothing loads at import time.
# Model, ChromaDB, and BM25 index all load on first call to agent2_node.
# This allows unit tests to import pure logic functions without
# needing sentence_transformers, chromadb, or rank_bm25 installed.

_MODEL      = None
_CLIENT     = None
_COLLECTION = None
_BM25       = None
_BM25_DOCS  = None
_BM25_IDS   = None
_BM25_METAS = None


def _get_model():
    """
    Lazy loader for the SentenceTransformer model.
    Loads once on first call, reuses on subsequent calls.
    """
    global _MODEL
    if _MODEL is None:
        from sentence_transformers import SentenceTransformer
        log.info("Loading embedding model: %s", MODEL_NAME)
        _MODEL = SentenceTransformer(MODEL_NAME)
        log.info("Embedding model ready")
    return _MODEL


def _get_collection():
    """
    Lazy loader for ChromaDB collection.
    Uses shared chromadb_client utility which handles cloud vs local mode.
    Fails with clear error if collection is missing.
    """
    global _CLIENT, _COLLECTION
    if _COLLECTION is None:
        from app.utils.chromadb_client import get_client, get_collection
        _CLIENT     = get_client()
        _COLLECTION = get_collection(_CLIENT)
        log.info(
            "ChromaDB connected — collection=%s abstracts=%d",
            COLLECTION_NAME,
            _COLLECTION.count(),
        )
    return _COLLECTION


def _get_bm25():
    """
    Lazy loader for BM25 sparse index.

    Loads all documents from ChromaDB once and builds a BM25Okapi index.
    BM25 tokenises each document into words and builds an inverted index
    for fast keyword scoring.

    Why load all docs:
        BM25 needs the entire corpus to compute IDF (inverse document frequency).
        IDF measures how rare a word is across all documents.
        A word appearing in 1 of 1964 papers is more informative than one
        appearing in 1900 of 1964 papers.

    Returns:
        bm25   : BM25Okapi index over all documents
        docs   : list of document texts (parallel to bm25 index)
        ids    : list of ChromaDB IDs
        metas  : list of metadata dicts (drug_name, pmid, year)
    """
    global _BM25, _BM25_DOCS, _BM25_IDS, _BM25_METAS
    if _BM25 is None:
        from rank_bm25 import BM25Okapi

        log.info("Building BM25 sparse index from ChromaDB...")
        collection = _get_collection()

        # Load all documents from ChromaDB
        all_data   = collection.get(include=["documents", "metadatas"], limit=10000,)
        _BM25_DOCS  = all_data["documents"]
        _BM25_IDS   = all_data["ids"]
        _BM25_METAS = all_data["metadatas"]

        # Tokenise: lowercase + split on whitespace
        # Simple tokenisation is sufficient for biomedical abstracts
        tokenised = [doc.lower().split() for doc in _BM25_DOCS]
        _BM25     = BM25Okapi(tokenised)

        log.info("BM25 index built — %d documents indexed", len(_BM25_DOCS))

    return _BM25, _BM25_DOCS, _BM25_IDS, _BM25_METAS


# ── Step 1: HNSW dense retrieval ─────────────────────────────────────────────

def embed_query(query: str) -> list:
    """
    Convert a natural language query into a 384-dimensional vector.
    Uses all-MiniLM-L6-v2 — same model used at index time in load_pubmed.py.
    Returns plain Python list (ChromaDB requires list, not numpy array).
    """
    return _get_model().encode(query).tolist()


def hnsw_search(
    query: str,
    drug_key: str,
    n_results: int = 10,
) -> list:
    """
    Search ChromaDB using HNSW dense vector similarity.

    Filters by drug_name metadata so only abstracts for this drug are returned.
    Applies cosine similarity threshold — discards papers below 0.60.

    Returns:
        List of result dicts with pmid, text, distance, similarity, drug_name.
    """
    collection = _get_collection()
    embedding  = embed_query(query)

    results = collection.query(
        query_embeddings=[embedding],
        n_results=n_results,
        where={"drug_name": drug_key},
        include=["documents", "metadatas", "distances"],
    )

    docs      = results["documents"][0]
    metadatas = results["metadatas"][0]
    distances = results["distances"][0]

    filtered = []
    for doc, meta, dist in zip(docs, metadatas, distances):
        similarity = 1.0 - dist
        if similarity < SIMILARITY_THRESHOLD:
            continue
        filtered.append({
            "pmid"      : meta.get("pmid", "unknown"),
            "text"      : doc,
            "distance"  : float(dist),
            "similarity": round(float(similarity), 4),
            "drug_name" : drug_key,
            "retriever" : "hnsw",
        })

    return filtered


# ── Step 2: BM25 sparse retrieval ─────────────────────────────────────────────

def bm25_search(
    query: str,
    drug_key: str,
    n_results: int = 10,
) -> list:
    """
    Search using BM25 keyword matching.

    Why BM25 complements HNSW:
        HNSW finds papers semantically similar to the query vector.
        BM25 finds papers containing the exact query words.
        A short case report saying "dupilumab conjunctivitis: 3 cases"
        scores high on BM25 (exact match) but low on HNSW (too short,
        not semantically rich enough to be a close vector neighbor).

    Filters by drug_name to keep results drug-specific.
    Applies BM25_MIN_SCORE to discard weak keyword matches.

    BM25 has no distance concept — we normalize score to 0-1 range
    for consistent comparison in RRF.

    Returns:
        List of result dicts with pmid, text, similarity, drug_name.
    """
    bm25, docs, ids, metas = _get_bm25()

    tokens = query.lower().split()
    scores = bm25.get_scores(tokens)

    # Zip scores with metadata, filter by drug and minimum score
    candidates = [
        (score, doc, uid, meta)
        for score, doc, uid, meta in zip(scores, docs, ids, metas)
        if meta.get("drug_name") == drug_key and score >= BM25_MIN_SCORE
    ]

    # Sort by score descending
    candidates.sort(key=lambda x: x[0], reverse=True)

    results = []
    for score, doc, uid, meta in candidates[:n_results]:
        # Normalize BM25 score to 0-1 range for RRF compatibility
        # BM25 scores vary widely — dividing by 10 is a reasonable normalizer
        # for biomedical abstracts (typical max score is 5-15)
        normalized_similarity = min(score / 10.0, 1.0)

        results.append({
            "pmid"      : meta.get("pmid", "unknown"),
            "text"      : doc,
            "distance"  : float(1.0 - normalized_similarity),  # BM25 has no true distance, so we invert similarity
            "similarity": round(float(normalized_similarity), 4), # Higher BM25 score means more relevant
            "drug_name" : drug_key,
            "retriever" : "bm25",
        })

    return results


# ── Step 3: Reciprocal Rank Fusion ────────────────────────────────────────────

def reciprocal_rank_fusion(query_results: list) -> list:
    """
    Fuse results from multiple retrievers into a single ranked list.

    Works across both HNSW and BM25 results — the retriever type does not
    matter, only rank position matters.

    RRF formula for each appearance:
        score += 1 / (rank + RRF_K)

    Paper appearing in HNSW rank 1 AND BM25 rank 1:
        score = 1/(1+60) + 1/(1+60) = 0.0328  (highest possible)

    Paper appearing only in HNSW rank 1:
        score = 1/(1+60) = 0.0164

    Papers found by both retrievers are the most robustly relevant.

    Args:
        query_results : list of result lists — one per query per retriever.

    Returns:
        Unique abstracts sorted by RRF score descending.
    """
    fused: dict = {}

    for results in query_results:
        for rank, result in enumerate(results, start=1):
            pmid      = result["pmid"]
            rrf_score = 1.0 / (rank + RRF_K)

            if pmid not in fused:
                fused[pmid] = {**result, "rrf_score": rrf_score}
            else:
                fused[pmid]["rrf_score"] += rrf_score
                fused[pmid]["distance"]   = float(min(
                    fused[pmid]["distance"], result["distance"]
                ))
                fused[pmid]["similarity"] = round(
                    float(1.0 - fused[pmid]["distance"]), 4
                )

    return sorted(fused.values(), key=lambda x: x["rrf_score"], reverse=True)


# ── Step 4: LitScore computation ──────────────────────────────────────────────

def compute_lit_score(abstracts: list) -> float:
    """
    Compute LitScore in [0, 1] from retrieved abstracts.

    relevance_score : how close abstracts are on average (0.70 weight)
    volume_score    : how many abstracts found above threshold (0.30 weight)

    LitScore = (relevance × 0.70) + (volume × 0.30)

    Zero abstracts → LitScore = 0.0
    Signals to Agent 3 there is no literature support.
    Agent 3 assigns P2 or P4 — signal still reviewed by human.
    """
    if not abstracts:
        return 0.0

    avg_distance    = sum(float(a["distance"]) for  a in abstracts) / len(abstracts)
    relevance_score = max(0.0, 1.0 - (avg_distance / 1.5))
    volume_score    = min(len(abstracts) / MAX_ABSTRACTS, 1.0)

    return round((relevance_score * 0.70) + (volume_score * 0.30), 4)


# ── Step 5: LangGraph node ────────────────────────────────────────────────────

def agent2_node(state: SignalState) -> dict:
    """
    LangGraph node for Agent 2.

    Full hybrid retrieval flow:
        1. For each of 3 queries → HNSW search ChromaDB
        2. For each of 3 queries → BM25 keyword search
        3. Fuse all 6 result sets with RRF (3 HNSW + 3 BM25)
        4. Take top-5 fused results
        5. Compute LitScore
        6. Return abstracts + lit_score to state

    Error handling:
        Individual query failures are caught and logged.
        If all queries fail, returns empty abstracts and 0.0 LitScore.
        Agent 3 handles zero-literature case gracefully.
    """
    drug_key       = state["drug_key"]
    search_queries = state.get("search_queries") or []

    if not search_queries:
        log.warning("agent2: no search queries in state for %s", drug_key)
        return {"abstracts": [], "lit_score": 0.0}

    log.info("agent2_start drug=%s queries=%d", drug_key, len(search_queries))

    all_results = []

    for i, query in enumerate(search_queries, start=1):

        # HNSW dense retrieval
        try:
            hnsw_results = hnsw_search(query, drug_key)
            log.info(
                "  hnsw query_%d returned %d abstracts above threshold",
                i, len(hnsw_results),
            )
            all_results.append(hnsw_results)
        except Exception as e:
            log.error("agent2: HNSW query failed query=%d error=%s", i, e)
            all_results.append([])

        # BM25 sparse retrieval
        try:
            bm25_results = bm25_search(query, drug_key)
            log.info(
                "  bm25 query_%d returned %d abstracts above min score",
                i, len(bm25_results),
            )
            all_results.append(bm25_results)
        except Exception as e:
            log.error("agent2: BM25 query failed query=%d error=%s", i, e)
            all_results.append([])

    # Fuse all 6 result sets (3 HNSW + 3 BM25) with RRF
    fused = reciprocal_rank_fusion(all_results)

    log.info(
        "agent2_rrf_complete drug=%s unique_abstracts=%d",
        drug_key, len(fused),
    )

    top_abstracts = fused[:MAX_ABSTRACTS]
    lit_score     = compute_lit_score(top_abstracts)

    log.info(
        "agent2_complete drug=%s pt=%s abstracts=%d lit_score=%.4f",
        drug_key, state["pt"], len(top_abstracts), lit_score,
    )

    return {
        "abstracts": top_abstracts,
        "lit_score": lit_score,
    }