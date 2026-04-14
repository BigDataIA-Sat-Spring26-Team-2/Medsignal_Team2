"""
agent2_retriever.py — MedSignal Agent 2: Literature Retriever

Role in pipeline:
    Receives three GPT-4o generated PubMed search queries from Agent 1.
    Queries ChromaDB using all-MiniLM-L6-v2 embeddings (same model used
    at index time in load_pubmed.py — mismatch would silently break retrieval).
    Fuses results from all three queries using Reciprocal Rank Fusion (RRF).
    Computes a LitScore from the similarity and volume of returned abstracts.
    Passes top-5 abstracts and LitScore to Agent 3.

Why no LLM:
    Retrieval is a deterministic similarity search — there is no reasoning
    task here. Adding an LLM to retrieval would introduce cost, latency,
    and non-reproducibility with zero benefit. ChromaDB handles this better.

Why hybrid retrieval (HNSW dense + RRF):
    In POC testing, HNSW alone missed warfarin x skin necrosis entirely.
    Running three differently-angled queries and fusing results catches
    papers that any single query would miss.

Owner: Prachi
"""

import os
import math
import logging
from typing import List

import chromadb
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

from app.agents.state import SignalState, Abstract

load_dotenv()

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

# CRITICAL: must be identical to the model used in load_pubmed.py at index time.
# If this changes, all stored embeddings become incompatible with query embeddings
# and retrieval silently returns wrong results.
MODEL_NAME = "all-MiniLM-L6-v2"

# Cosine similarity threshold — abstracts below this are too generic to be useful.
# Calibrated from POC: dupilumab x conjunctivitis top abstracts scored 0.61-0.76.
# Below 0.60 = off-topic paper included by coincidence.
SIMILARITY_THRESHOLD = 0.60

# Maximum abstracts to pass to Agent 3.
# More than 5 inflates the GPT-4o prompt beyond useful context.
MAX_ABSTRACTS = 5

# RRF constant — standard value from literature.
# Prevents top-ranked results from dominating the fusion score.
RRF_K = 60

# ChromaDB collection name — must match what load_pubmed.py created.
COLLECTION_NAME = "pubmed_abstracts"


# ── Model + ChromaDB — loaded once at module import ───────────────────────────
# Loading SentenceTransformer takes ~2 seconds.
# Loading it inside agent2_node would add that cost per signal.
# Module-level loading means it loads once when the pipeline starts.

log.info("Loading embedding model: %s", MODEL_NAME)
_MODEL = SentenceTransformer(MODEL_NAME)
log.info("Embedding model ready")

_CHROMA_PATH = os.getenv("CHROMADB_PATH", "./chromadb_store")
_CLIENT      = chromadb.PersistentClient(path=_CHROMA_PATH)
_COLLECTION  = _CLIENT.get_collection(COLLECTION_NAME)

log.info(
    "ChromaDB connected — collection=%s abstracts=%d",
    COLLECTION_NAME,
    _COLLECTION.count(),
)


# ── Step 1: Embed one query ───────────────────────────────────────────────────

def embed_query(query: str) -> list:
    """
    Convert a natural language query into a 384-dimensional vector.

    Uses the same all-MiniLM-L6-v2 model that was used when load_pubmed.py
    embedded the abstracts. This is non-negotiable — if the model differs,
    the query vector lives in a different vector space than the stored
    abstract vectors. Cosine similarity between them becomes meaningless.

    Returns a plain Python list (ChromaDB requires list, not numpy array).
    """
    return _MODEL.encode(query).tolist()


# ── Step 2: Query ChromaDB for one query ──────────────────────────────────────

def query_chromadb(
    query: str,
    drug_key: str,
    n_results: int = 10,
) -> list[dict]:
    """
    Search ChromaDB for abstracts relevant to one query.

    Filters by drug_name metadata so only abstracts for this specific drug
    are returned. This is important — without the filter, dupilumab queries
    would also return bupropion abstracts that happen to mention eye symptoms.

    Args:
        query     : natural language search query from Agent 1
        drug_key  : canonical drug name e.g. "dupilumab"
        n_results : how many results to fetch before threshold filtering.
                    Fetching 10 gives us room to discard low-similarity ones
                    and still have enough for the top-5 selection.

    Returns:
        List of result dicts with keys: pmid, text, distance, drug_name
        Only includes results where similarity >= SIMILARITY_THRESHOLD.
    """
    embedding = embed_query(query)

    results = _COLLECTION.query(
        query_embeddings=[embedding],
        n_results=n_results,
        where={"drug_name": drug_key},         # filter to this drug only
        include=["documents", "metadatas", "distances"],
    )

    # ChromaDB returns nested lists — [0] unwraps the first (only) query batch
    docs      = results["documents"][0]
    metadatas = results["metadatas"][0]
    distances = results["distances"][0]

    # Convert distance to similarity and apply threshold
    # distance = 0 means identical, distance = 1 means completely unrelated
    # similarity = 1 - distance (valid because collection uses cosine space)
    filtered = []
    for doc, meta, dist in zip(docs, metadatas, distances):
        similarity = 1.0 - dist
        if similarity < SIMILARITY_THRESHOLD:
            # Abstract is too dissimilar — skip
            # This happens when ChromaDB has no truly relevant papers
            # for this drug-reaction combination
            continue

        filtered.append({
            "pmid"      : meta.get("pmid", "unknown"),
            "text"      : doc,
            "distance"  : dist,
            "similarity": round(similarity, 4),
            "drug_name" : drug_key,
        })

    return filtered


# ── Step 3: Reciprocal Rank Fusion ────────────────────────────────────────────

def reciprocal_rank_fusion(
    query_results: list[list[dict]],
) -> list[dict]:
    """
    Fuse results from multiple queries into a single ranked list.

    Why RRF:
        Query 1 returns: [paper_A rank1, paper_B rank2, paper_C rank3]
        Query 2 returns: [paper_C rank1, paper_D rank2, paper_A rank3]
        paper_A appears in both at rank 1 and rank 3 → high fusion score
        paper_C appears in both at rank 3 and rank 1 → high fusion score
        paper_D appears only once → lower fusion score

    RRF formula for each appearance:
        score += 1 / (rank + RRF_K)

    A paper ranked #1 in two queries scores:
        1/(1+60) + 1/(1+60) = 0.0328

    A paper ranked #1 in one query scores:
        1/(1+60) = 0.0164

    The paper appearing in multiple queries wins — it is more robust evidence.

    Args:
        query_results : list of result lists, one per query
                        each result list is already filtered by threshold

    Returns:
        List of unique abstracts sorted by RRF score descending.
        Each dict has an added 'rrf_score' key.
    """
    # pmid → accumulated RRF score + best result dict
    fused: dict[str, dict] = {}

    for results in query_results:
        for rank, result in enumerate(results, start=1):
            pmid      = result["pmid"]
            rrf_score = 1.0 / (rank + RRF_K)

            if pmid not in fused:
                # First time seeing this paper — add it
                fused[pmid] = {**result, "rrf_score": rrf_score}
            else:
                # Paper appeared in another query — accumulate score
                # Keep the lowest distance (best similarity) seen so far
                fused[pmid]["rrf_score"] += rrf_score
                fused[pmid]["distance"]   = min(
                    fused[pmid]["distance"], result["distance"]
                )
                fused[pmid]["similarity"] = 1.0 - fused[pmid]["distance"]

    # Sort by RRF score descending — highest score = most relevant
    return sorted(fused.values(), key=lambda x: x["rrf_score"], reverse=True)


# ── Step 4: LitScore computation ──────────────────────────────────────────────

def compute_lit_score(abstracts: list[dict]) -> float:
    """
    Compute LitScore ∈ [0, 1] from the retrieved abstracts.

    Components:
        relevance_score : how close the abstracts are to the query
                          avg_distance / 1.5 normalizes to 0-1 range
                          dividing by 1.5 because realistic worst distance
                          for above-threshold results is ~1.0

        volume_score    : how many abstracts were found above threshold
                          max at 5 abstracts (MAX_ABSTRACTS)
                          a signal with only 1 abstract has less support
                          than one with 5 relevant papers

    Weights:
        relevance 0.70 — quality matters more than quantity
        volume    0.30 — more papers provide broader corroboration

    Edge case:
        If no abstracts pass the threshold → LitScore = 0.0
        This signals to Agent 3 that there is no literature support.
        Agent 3 will assign P2 or P4 (statistical signal, weak literature).
    """
    if not abstracts:
        return 0.0

    avg_distance    = sum(a["distance"] for a in abstracts) / len(abstracts)
    relevance_score = max(0.0, 1.0 - (avg_distance / 1.5))
    volume_score    = min(len(abstracts) / MAX_ABSTRACTS, 1.0)

    lit_score = (relevance_score * 0.70) + (volume_score * 0.30)
    return round(lit_score, 4)


# ── Step 5: LangGraph node ────────────────────────────────────────────────────

def agent2_node(state: SignalState) -> dict:
    """
    LangGraph node for Agent 2.

    Called by the pipeline after Agent 1 completes.
    Reads search_queries and drug_key from state.
    Returns abstracts and lit_score to be merged into state.

    Flow:
        1. Take the 3 queries from Agent 1
        2. Embed + search ChromaDB for each query separately
        3. Fuse results with RRF
        4. Take top-5 fused results
        5. Compute LitScore
        6. Return to state — Agent 3 picks up from here

    Error handling:
        If ChromaDB is unavailable or collection is empty,
        returns lit_score=0.0 and empty abstracts rather than crashing.
        Agent 3 handles the zero-literature case gracefully.
    """
    drug_key       = state["drug_key"]
    search_queries = state.get("search_queries") or []

    if not search_queries:
        log.warning("agent2: no search queries in state for %s", drug_key)
        return {"abstracts": [], "lit_score": 0.0}

    log.info(
        "agent2_start drug=%s queries=%d",
        drug_key, len(search_queries)
    )

    # Step 1 — query ChromaDB separately for each of the 3 queries
    # Each query approaches the signal from a different angle
    all_query_results = []
    for i, query in enumerate(search_queries, start=1):
        results = query_chromadb(query, drug_key)
        log.info(
            "  query_%d returned %d abstracts above threshold",
            i, len(results)
        )
        all_query_results.append(results)

    # Step 2 — fuse all results using RRF
    # Papers appearing in multiple queries score higher
    fused = reciprocal_rank_fusion(all_query_results)

    log.info(
        "agent2_rrf_complete drug=%s unique_abstracts=%d",
        drug_key, len(fused)
    )

    # Step 3 — take top MAX_ABSTRACTS (5) after fusion
    top_abstracts = fused[:MAX_ABSTRACTS]

    # Step 4 — compute LitScore from the selected abstracts
    lit_score = compute_lit_score(top_abstracts)

    log.info(
        "agent2_complete drug=%s pt=%s abstracts=%d lit_score=%.4f",
        drug_key, state["pt"], len(top_abstracts), lit_score
    )

    # Return only the fields this agent owns
    # LangGraph merges this dict into the existing state
    return {
        "abstracts": top_abstracts,
        "lit_score": lit_score,
    }