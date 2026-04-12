"""
chromadb_client.py — Shared ChromaDB client factory.

Used by:
    scripts/load_pubmed.py   — loads abstracts into ChromaDB
    agents/agent2_retriever.py — queries ChromaDB for literature
    agents/agent3_assessor.py  — reads retrieved abstracts

Everyone imports get_client() from here. Credentials come from .env.
"""

import os
import sys
import chromadb
from dotenv import load_dotenv

load_dotenv()

CHROMADB_MODE = os.getenv("CHROMADB_MODE", "local")


def get_client():
    if CHROMADB_MODE == "cloud":
        try:
            client = chromadb.HttpClient(
                ssl=True,
                host="api.trychroma.com",
                port=8000,
                tenant=os.getenv("CHROMA_TENANT"),
                database=os.getenv("CHROMA_DATABASE"),
                headers={
                    "x-chroma-token": os.getenv("CHROMA_API_KEY")
                },
            )
            client.heartbeat()
            return client
        except Exception as exc:
            print(f"ERROR: ChromaDB Cloud connection failed: {exc}")
            print("Check CHROMA_TENANT, CHROMA_DATABASE, CHROMA_API_KEY in .env")
            sys.exit(1)
    else:
        path = os.getenv("CHROMADB_PATH", "./chromadb_store")
        return chromadb.PersistentClient(path=path)


def get_collection(client=None):
    if client is None:
        client = get_client()

    collection = client.get_or_create_collection(
        name="pubmed_abstracts",
        metadata={"hnsw:space": "cosine"},
    )

    # Verify the collection is actually using cosine similarity.
    # get_or_create_collection silently ignores metadata if the
    # collection already exists — this catches any mismatch.
    actual_space = collection.metadata.get("hnsw:space", "l2")
    if actual_space != "cosine":
        print(
            f"ERROR: Collection 'pubmed_abstracts' uses '{actual_space}' distance "
            f"not 'cosine'. Similarity scores via '1 - dist' will be wrong.\n"
            f"Fix: delete the collection and rerun load_pubmed.py."
        )
        sys.exit(1)

    return collection