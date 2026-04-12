"""
load_pubmed.py — Fetch PubMed abstracts and load into ChromaDB.

Fetches up to 200 abstracts per golden signal drug from NCBI PubMed,
embeds each one locally using all-MiniLM-L6-v2, and stores vectors
in ChromaDB (cloud or local) with drug_name and pmid metadata.

Agent 2 queries this collection at signal investigation time.

Run once before the agent pipeline:
    poetry run python scripts/load_pubmed.py

Takes 2-4 hours. Safe to interrupt and restart — already-loaded
abstracts are skipped automatically via PMID check.

Environment variables (.env):
    NCBI_EMAIL        your@northeastern.edu
    NCBI_API_KEY      from ncbi.nlm.nih.gov/account (raises limit 3→10 req/s)
    CHROMADB_MODE     cloud | local (default: local)
    CHROMA_TENANT     from trychroma.com dashboard (cloud only)
    CHROMA_DATABASE   medsignal (cloud only)
    CHROMA_API_KEY    from trychroma.com dashboard (cloud only)
    CHROMADB_PATH     ./chromadb_store (local only)
"""

import os
import sys
import time

from Bio import Entrez
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
from utils.chromadb_client import get_client, get_collection

load_dotenv()

# ── NCBI config ───────────────────────────────────────────────────────────
Entrez.email   = os.getenv("NCBI_EMAIL", "")
Entrez.api_key = os.getenv("NCBI_API_KEY", "")

if not Entrez.email:
    print("ERROR: NCBI_EMAIL not set in .env")
    sys.exit(1)

# ── ChromaDB setup ────────────────────────────────────────────────────────
client     = get_client()
collection = get_collection(client)

# ── Constants ─────────────────────────────────────────────────────────────
MODEL_NAME   = "all-MiniLM-L6-v2"   # 384-dim, local, no API cost
                                      # MUST match model used in Agent 2
THRESHOLD    = 0.60                   # cosine similarity threshold
                                      # calibrated from POC (proposal p33)
MAX_PER_DRUG = 200                    # PMIDs per drug (NCBI max for free)
BATCH_SIZE   = 20                     # EFetch batch size (NCBI recommended)
SLEEP_S      = 0.12                   # 8 req/s — safely under 10 req/s limit

# ── 10 Golden signal drugs ────────────────────────────────────────────────
# Source: proposal p30-31, Table: Golden Signal Validation Set
# Each has a documented FDA safety communication in 2023-2024
# These are the ONLY drugs the agent pipeline evaluates against
GOLDEN_DRUGS = [
    "dupilumab",      # Dupixent  — FDA Label Update Jan 2024
    "gabapentin",     # Neurontin — FDA Safety Comm  Dec 2023
    "pregabalin",     # Lyrica    — FDA Safety Comm  Dec 2023
    "levetiracetam",  # Keppra    — FDA Safety Comm  Nov 2023
    "tirzepatide",    # Mounjaro  — FDA Safety Comm  Sep 2023
    "semaglutide",    # Ozempic   — FDA Safety Comm  Sep 2023
    "empagliflozin",  # Jardiance — FDA Safety Comm  Aug 2023
    "bupropion",      # Wellbutrin— FDA Safety Comm  May 2023
    "dapagliflozin",  # Farxiga   — FDA Label Update May 2023
    "metformin",      # Glucophage— FDA Safety Comm  Apr 2023
]

# ── Embedding model ───────────────────────────────────────────────────────
# Loaded once, reused for all drugs.
# CRITICAL: must be identical at both index time (here)
# and query time (Agent 2) — mismatch silently breaks retrieval.
print(f"Loading embedding model: {MODEL_NAME}")
MODEL = SentenceTransformer(MODEL_NAME)
print("Model ready.\n")


# ── NCBI helpers ──────────────────────────────────────────────────────────
def esearch(drug: str) -> list[str]:
    """
    Search PubMed for abstracts relevant to a drug's adverse effects.
    Returns up to MAX_PER_DRUG PMIDs sorted by relevance.

    Query combines drug name with adverse event terms — focuses results
    on safety literature rather than general pharmacology.
    """
    query = (
        f"{drug} AND "
        f"(adverse[tiab] OR safety[tiab] OR toxicity[tiab] OR risk[tiab])"
    )
    handle = Entrez.esearch(
        db="pubmed",
        term=query,
        retmax=MAX_PER_DRUG,
        sort="relevance",
    )
    record = Entrez.read(handle)
    handle.close()
    return record["IdList"]


def efetch_batch(pmids: list[str]) -> list[dict]:
    """
    Fetch full XML records for a batch of PMIDs.
    Extracts title, abstract, year.
    Skips records with empty abstracts (editorials, conference papers).

    Batching in groups of 20 follows NCBI recommended practice
    to avoid timeouts on large requests.
    """
    handle  = Entrez.efetch(
        db="pubmed",
        id=",".join(pmids),
        rettype="xml",
        retmode="xml",
    )
    records = Entrez.read(handle)
    handle.close()

    abstracts = []
    for rec in records.get("PubmedArticle", []):
        try:
            article  = rec["MedlineCitation"]["Article"]
            abstract = " ".join(
                str(p)
                for p in article.get("Abstract", {}).get("AbstractText", [])
            )
            if not abstract.strip():
                continue            # skip empty — editorials, letters etc.

            pmid  = str(rec["MedlineCitation"]["PMID"])
            title = str(article.get("ArticleTitle", ""))
            year  = str(
                article["Journal"]["JournalIssue"]["PubDate"].get("Year", "")
            )
            abstracts.append({
                "pmid": pmid, "title": title,
                "abstract": abstract, "year": year,
            })
        except Exception:
            continue                # malformed record, skip silently
    return abstracts


# ── Per-drug loader ───────────────────────────────────────────────────────
def load_drug(drug_name: str) -> int:
    """
    Full pipeline for one drug:
    1. Check what is already in ChromaDB — skip if sufficiently loaded
    2. Search PubMed for PMIDs
    3. Fetch abstracts in batches of 20
    4. Embed each abstract locally with all-MiniLM-L6-v2
    5. Store vector + metadata in ChromaDB

    uid format is drug_name_pmid — unique per drug-paper combination.
    Same PMID can appear for multiple drugs (intentional — same paper
    may be relevant to dupilumab AND pregabalin for different reasons).

    Returns number of abstracts newly added this run.
    """
    existing   = collection.get(where={"drug_name": drug_name})
    loaded_ids = set(existing["ids"])

    if len(loaded_ids) >= 150:
        print(f"  {drug_name}: already loaded ({len(loaded_ids)} abstracts), skipping")
        return 0

    pmids = list(dict.fromkeys(esearch(drug_name)))
    print(f"  {drug_name}: {len(pmids)} PMIDs found")

    newly_loaded = 0

    for i in range(0, len(pmids), BATCH_SIZE):
        batch = pmids[i : i + BATCH_SIZE]

        try:
            records = efetch_batch(batch)
        except Exception as exc:
            print(f"    fetch error: {exc} — retrying in 5s")
            time.sleep(5)
            try:
                records = efetch_batch(batch)
            except Exception:
                print(f"    retry failed, skipping batch {i}–{i+BATCH_SIZE}")
                continue

        for rec in records:
            uid = f"{drug_name}_{rec['pmid']}"

            if uid in loaded_ids:
                continue

            text      = rec["title"] + " " + rec["abstract"]
            embedding = MODEL.encode(text).tolist()

            try:
                collection.add(
                    ids       =[uid],
                    embeddings=[embedding],
                    documents =[text],
                    metadatas =[{
                        "drug_name": drug_name,
                        "pmid"     : rec["pmid"],
                        "year"     : rec["year"],
                    }],
                )
                newly_loaded += 1
            except Exception:
                pass                # duplicate id, skip

        time.sleep(SLEEP_S)

    print(f"  {drug_name}: {newly_loaded} abstracts added")
    return newly_loaded


# ── Validation ────────────────────────────────────────────────────────────
def validate():
    """
    Test retrieval query to confirm ChromaDB works after loading.

    Uses dupilumab + conjunctivitis — a known golden signal pair.
    Proposal p33: top abstracts should score above 0.60 cosine similarity.
    M2 milestone check: at least 3 of 5 results above threshold.
    """
    print("\nRunning retrieval validation (dupilumab + conjunctivitis)...")
    query     = "dupilumab conjunctivitis adverse effects"
    embedding = MODEL.encode(query).tolist()

    results = collection.query(
        query_embeddings=[embedding],
        n_results=5,
        where={"drug_name": "dupilumab"},
        include=["documents", "distances"],
    )

    docs      = results["documents"][0]
    distances = results["distances"][0]
    passed    = 0

    for i, (doc, dist) in enumerate(zip(docs, distances)):
        similarity = 1 - dist
        status     = "PASS" if similarity >= THRESHOLD else "FAIL"
        if similarity >= THRESHOLD:
            passed += 1
        print(f"  [{i+1}] {status} sim={similarity:.3f} | {doc[:80]}...")

    print(f"\nValidation: {passed}/5 above threshold {THRESHOLD}")
    if passed >= 3:
        print("ChromaDB ready for Agent 2.")
    else:
        print("WARNING: low scores — check NCBI_EMAIL is set and abstracts loaded.")


# ── Main ──────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("MedSignal — PubMed → ChromaDB Loader")
    print("=" * 60)
    print(f"Mode    : {os.getenv('CHROMADB_MODE', 'local')}")
    print(f"Model   : {MODEL_NAME}")
    print(f"Drugs   : {len(GOLDEN_DRUGS)}")
    print(f"Max/drug: {MAX_PER_DRUG} PMIDs")
    print()

    total = 0
    for drug in GOLDEN_DRUGS:
        print(f"--- {drug} ---")
        total += load_drug(drug)

    print(f"\nDone. Abstracts added this run : {total}")
    print(f"Total in collection            : {collection.count()}")
    print(f"Target                         : 1,800–1,930")

    validate()


if __name__ == "__main__":
    main()