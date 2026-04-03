# MedSignal — Real-Time Drug Safety Signal Detection Platform

> **DAMG 7245 — Big Data and Intelligent Analytics | Northeastern University | Spring 2026**

MedSignal is a pharmacovigilance platform that detects drug safety signals from FDA FAERS adverse event data, retrieves supporting clinical literature, and generates citation-grounded safety briefs using a three-agent LLM pipeline — reducing a process that takes human analysts days to minutes.

---

## The Problem

The FDA receives over 2 million adverse event reports annually through FAERS. The average time from when a signal first appears in that data to when the FDA officially communicates a warning is **6–24 months**. During that window, patients continue to be exposed to preventable harm.

The Vioxx crisis (2004) is the canonical example: cardiovascular risk evidence existed in FAERS for years before the drug was withdrawn. An estimated 88,000–140,000 heart attacks occurred during the detection gap. The problem was not a lack of data — it was a lack of infrastructure to process it intelligently.

MedSignal is that infrastructure.

---

## What It Does

```
FDA FAERS (16M records) ──► Kafka ──► Spark PRR Engine ──► Flagged Signals
                                                                    │
PubMed (28K abstracts) ──► ChromaDB + BM25 ◄── Agent 2 (RAG) ◄────┤
                                                                    │
Reddit Health Forums ──────────────────────────► Agent 3 (SSS) ◄───┘
                                                      │
ClinicalTrials.gov ────────────────────────────────────┘
                                                      │
                                              SafetyBrief JSON
                                              + HITL Review Queue
                                              + React Dashboard
```

1. **Spark ingests and processes** 16M FAERS records — 7-file join, RxNorm drug normalisation, PRR/ROR disproportionality scoring across all drug-symptom pairs
2. **Pairs above threshold** (PRR ≥ 2.0, case count ≥ 3) are flagged and enter the LangGraph agent pipeline
3. **Agent 1** validates statistical significance and generates targeted PubMed search queries
4. **Agent 2** performs hybrid retrieval (ChromaDB HNSW + BM25, fused via RRF) over 28K PubMed abstracts
5. **Agent 3** synthesises all evidence into a structured SafetyBrief with a composite Signal Severity Score (SSS)
6. **HIGH and CRITICAL signals** are routed to a human reviewer before publication — the HITL gate is architecturally enforced, not optional

---

## Signal Severity Score (SSS)

MedSignal's core analytical contribution is a four-dimensional composite score combining independent evidence streams:

```
SSS = 0.40 × StatScore     ← PRR/ROR statistical signal strength
    + 0.30 × LitScore      ← PubMed RAG evidence quality (hybrid retrieval)
    + 0.20 × PatientScore  ← Reddit health forum mention frequency
    + 0.10 × TrialScore    ← ClinicalTrials.gov AE corroboration
```

| SSS Range | Severity | Action |
|-----------|----------|--------|
| 0.00 – 0.25 | WEAK | Auto-logged, monitor only |
| 0.26 – 0.50 | MODERATE | Logged, watch for trend |
| 0.51 – 0.75 | HIGH | Human review required within 24 hours |
| 0.76 – 1.00 | CRITICAL | Immediate escalation — HITL gate blocks publication |

**Non-compensatory rule:** StatScore = 0 caps SSS at WEAK regardless of other scores. A drug with strong Reddit mentions and literature support but no statistical signal is never published.

---

## Tech Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Message broker | Apache Kafka | Decouples FAERS ingestion from Spark; enables historical replay |
| Stream processing | Spark Structured Streaming | Distributed 7-file join + PRR aggregation across 16M records |
| NLP / NER | Spark NLP (BC5CDR) | Biomedical NER model for drug + disease entity extraction, runs as Spark UDF |
| Data warehouse | PostgreSQL | All structured data — signals, PRR scores, agent traces, HITL decisions |
| Vector store | ChromaDB + BM25 (hybrid) | Dense HNSW + sparse BM25 fused via RRF — zero retriever overlap confirmed in POC |
| Cache | Redis | Bloom filter deduplication (16M records), hot signal cache (60s TTL), RxNorm lookup cache |
| Orchestration | Airflow | FAERS batch DAG + PubMed embedding refresh DAG |
| LLM | GPT-4o (OpenAI) | Structured JSON output, long context for multi-paper synthesis |
| Agent framework | LangGraph | Typed Pydantic state machine — Signal Detector → Lit Retriever → Severity Assessor |
| Backend API | FastAPI | 12 REST endpoints, async-native, auto-generates Swagger docs |
| Frontend | React + Tailwind | Signal feed, HITL review queue, detection lag evaluation dashboard |
| Containerisation | Docker Compose | Single command starts entire stack — no cloud account required for dev/demo |

---

## Architecture

MedSignal is organized into five layers. Data flows left to right through the backend pipeline and into the agentic layer.

```
┌─────────────────────────────────────────────────────────────────┐
│  AGENTIC LAYER                                                  │
│  LangGraph State Machine                                        │
│  Agent 1 (Signal Detector) → Agent 2 (Lit Retriever) →         │
│  Agent 3 (Severity Assessor) → HITL Gate → React Dashboard     │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│  BACKEND PIPELINE LAYER                                         │
│                                                                 │
│  FAERS ZIPs ──► Kafka (faers_cases) ──► Spark ──► PostgreSQL    │
│                        │               ├── Branch A: BC5CDR NER │
│  Reddit PRAW ──► Kafka (reddit_posts)  ├── Branch B: RxNorm     │
│                                        └── Branch C: PRR/ROR    │
│                                                                 │
│  PubMed (28K) ──────────────────────────────► ChromaDB + BM25  │
│  ClinicalTrials.gov ◄──────────────── Agent 1 (live API call)  │
└─────────────────────────────────────────────────────────────────┘
```

### Layer-by-Layer

**Layer 1 — Data Sources**
FAERS quarterly ZIPs and Reddit posts publish to Kafka. ClinicalTrials.gov is called live by Agent 1 on demand. PubMed abstracts are processed through a one-time embedding pipeline directly to ChromaDB — they are a static RAG corpus, not a streaming source.

**Layer 2 — Ingestion (Kafka)**
Three topics: `faers_cases`, `reddit_posts`, `signals_flagged`. The FAERS producer joins DEMO + DRUG + REAC + OUTC files on `primaryid` and publishes one JSON message per case. Kafka decouples all producers from the Spark consumer.

**Layer 3 — Stream Processing (Spark)**
Three parallel branches: (A) BC5CDR NER extracts drug and symptom entities from free-text narratives, (B) RxNorm normalises drug name variants to canonical RxCUI identifiers, (C) PRR and ROR scores are computed over rolling 90-day windows. Pairs above threshold are published to `signals_flagged` and written to PostgreSQL.

**Layer 4 — Agent Pipeline (LangGraph)**
Three agents in a typed Pydantic state machine. Agent 1 validates statistical significance and generates PubMed search queries. Agent 2 performs hybrid retrieval over ChromaDB (up to 3 query reformulations if relevance falls below threshold). Agent 3 synthesises all evidence into a SafetyBrief. A direct conditional edge from Agent 3 back to Agent 2 triggers when confidence < 0.75.

**Layer 5 — Serving**
FastAPI exposes 12 REST endpoints. PostgreSQL stores all structured data. ChromaDB serves Agent 2 queries. Redis caches hot signals and deduplicates FAERS records.

---

## Hybrid Retrieval — Why BM25 + HNSW

Agent 2 uses hybrid retrieval rather than semantic search alone. POC validation on the real 28,014-abstract corpus confirmed **zero overlap** between HNSW (dense) and BM25 (sparse) retrievers across all test queries — the two methods find genuinely different evidence.

The warfarin × skin necrosis case is the clearest example: HNSW returned zero warfarin-specific results from the 500-abstract sample, while BM25 immediately surfaced *"Late-onset warfarin-induced skin necrosis"* at rank 1 through exact token matching. As the corpus grows from 28K to 300K abstracts and new drugs are continuously added, this gap widens.

```
Query → ChromaDB HNSW (dense, semantic)  ──► top-K dense results
      → BM25 (sparse, keyword)            ──► top-K sparse results
                          └──── RRF (k=60) ────► fused final ranking
```

**Reciprocal Rank Fusion (k=60):** Documents appearing in both lists accumulate scores from both retrievers. `BOTH` source tag = highest confidence. `DENSE` only = semantic match with no exact token overlap. `SPARSE` only = exact keyword match that embedding space missed.

---

## Data Sources

| Source | Volume | Role |
|--------|--------|------|
| FDA FAERS (2023 Q1 – 2024 Q4) | ~16M records, ~4–5GB uncompressed | Primary signal detection corpus |
| PubMed/MEDLINE | 28,014 abstracts (~150MB) | RAG knowledge base for Agent 2 |
| ClinicalTrials.gov | ~500 trials, ~10K AE records | TrialScore component of SSS |
| Reddit (r/pharmacy, r/ChronicPain, r/AskDocs) | Live stream via PRAW | PatientScore component of SSS |
| RxNorm (NIH) | Live API calls | Drug name normalisation |

---

## Guardrails and HITL

**Input guards**
- RxNorm normalisation unifies drug name variants before PRR computation ("Ozempic", "semaglutide", "Wegovy" → single RxCUI)
- PRR minimum case count (≥ 3) prevents single-report noise from entering the agent pipeline

**Output guards**
- All LLM outputs validated against Pydantic v2 schemas
- Citation validator checks every PMID against `pubmed_abstracts` table — ungrounded PMIDs cause brief rejection and agent re-run
- Confidence < 0.75 triggers automatic deeper retrieval (max 2 retries) before brief is finalised

**HITL Gate**

| Severity | HITL Required | SLA |
|----------|--------------|-----|
| WEAK | No — auto-logged | — |
| MODERATE | No — auto-logged | — |
| HIGH | Yes — pharmacist approval | 24 hours |
| CRITICAL | Yes — pharmacist approval | 4 hours |

HIGH and CRITICAL signals cannot be written to the published signals table without a row in `hitl_decisions`. This is enforced at the database level, not just application logic.

---

## Evaluation

### Primary — Detection Lag Study (Golden Set)

```
DLS = FDA_communication_date − MedSignal_first_flag_date  (days)
Target: Average DLS > 90 days across 10 historical signals
```

| Drug | Signal | FDA Communication |
|------|--------|-------------------|
| Finasteride | Major Depressive Disorder | 2022 label update |
| Rofecoxib (Vioxx) | Myocardial Infarction | Sep 2004 withdrawal |
| Ondansetron (Zofran) | QT Prolongation | Sep 2011 |
| Simvastatin | Myopathy / Rhabdomyolysis | Jun 2011 label update |
| Metformin | Lactic Acidosis | Apr 2016 label update |
| + 5 additional | TBD from FAERS literature | Various |

### KPIs

| KPI | Target |
|-----|--------|
| Average detection lag over FDA communication dates | > 90 days earlier |
| Golden signals detected earlier (of 10) | ≥ 7 of 10 |
| Signal detection precision | > 0.75 |
| NER F1 on drug entity extraction | > 0.80 |
| RAG Precision@5 | > 0.70 |
| Mean agent pipeline latency | < 60 seconds |
| Redis cache hit rate | > 40% |
| Daily GPT-4o cost during demo week | < $5/day |

---

## Getting Started

### Prerequisites

- Docker Desktop (16GB RAM recommended)
- Python 3.10+
- Node.js 18+
- OpenAI API key

### Quick Start

```bash
# Clone the repository
git clone https://github.com/your-org/medsignal.git
cd medsignal

# Copy environment template
cp .env.example .env
# Add your OPENAI_API_KEY to .env

# Start the full stack (Kafka, Spark, PostgreSQL, ChromaDB, Redis, Airflow)
docker compose up -d

# Verify all services are healthy
docker compose ps

# Load PubMed abstracts into ChromaDB (one-time, ~10 min on CPU)
python scripts/load_pubmed.py

# Replay FAERS historical data through Kafka
python scripts/replay_faers.py --quarters 2023Q1 2023Q2 2023Q3 2023Q4

# Access the dashboard
open http://localhost:3000
```

### Environment Variables

```env
OPENAI_API_KEY=sk-...
POSTGRES_URL=postgresql://medsignal:medsignal@localhost:5432/medsignal
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
REDIS_URL=redis://localhost:6379
CHROMADB_PATH=./chroma_store
NCBI_API_KEY=...          # Optional — raises PubMed rate limit from 3 to 10 req/sec
```

---

## Project Structure

```
medsignal/
├── pipeline/
│   ├── kafka/            # FAERS + Reddit Kafka producers
│   ├── spark/            # 7-file join, RxNorm, PRR/ROR computation
│   └── airflow/          # FAERS batch DAG + PubMed embedding refresh DAG
├── agents/
│   ├── agent1_detector.py     # Signal Detector — statistical validation
│   ├── agent2_retriever.py    # Literature Retriever — hybrid RAG
│   ├── agent3_assessor.py     # Severity Assessor — SSS + SafetyBrief
│   └── orchestrator.py        # LangGraph state machine
├── retrieval/
│   ├── chromadb_store.py      # HNSW dense retrieval
│   ├── bm25_index.py          # Sparse keyword retrieval
│   └── hybrid_search.py       # RRF fusion
├── api/                  # FastAPI — 12 REST endpoints
├── frontend/             # React + Tailwind dashboard
├── scripts/              # Data loading and replay utilities
└── tests/
    ├── unit/             # PRR, SSS, Pydantic, RxNorm, citation validator
    ├── integration/      # Pipeline, ChromaDB, agents, FastAPI, HITL
    ├── e2e/              # Full stack demo flow + PRR checkpoint
    └── eval/             # Golden set DLS, RAG Precision@5, NER F1
```

---

## Validation Checkpoint

After Spark processes FAERS 2023 Q1–Q4, this query must return PRR ≈ 3.14:

```sql
SELECT drug_name, meddra_reaction, prr_score, case_count_a
FROM   drug_symptom_pairs
WHERE  drug_name = 'finasteride'
AND    meddra_reaction ILIKE '%depress%'
ORDER  BY prr_score DESC
LIMIT  5;
-- Expected: prr_score ≈ 3.14, case_count_a ≈ 678
```

This is the most important validation in the project. It confirms the 7-file join, RxNorm normalisation, and PRR computation are all correct. No agent pipeline work begins until this passes.

---

## Team

| Member | Role |
|--------|------|
| Samiksha Rajesh Gupta | Data Engineering Lead — Kafka, Spark, FAERS pipeline, Airflow |
| Prachi Ganpatrao Pradhan | LLM / Agent Engineer — LangGraph, RAG, SSS, FastAPI signals endpoints |
| Siddharth Rakesh Shukla | Backend, Frontend & QA Lead — Docker, Redis, HITL service, React |

---

## Cost

| Item | Estimated Cost |
|------|---------------|
| OpenAI embeddings (28K abstracts, one-time) | ~$0.20 |
| GPT-4o development (GPT-4o-mini during dev) | ~$5.00 |
| GPT-4o demo week (500 signals × 3 agents) | ~$15–25 |
| **Total budget** | **< $50** |

Hard limit set in OpenAI dashboard on Day 1. All agent responses cached in Redis (24-hour TTL) to reduce repeat API calls. Token usage and cost tracked per agent in real time via the System Health dashboard.

---

## Limitations

- FAERS data is released quarterly — MedSignal replays historical records through Kafka to simulate streaming. This is explicitly a simulation, not true real-time ingestion.
- PRR statistical significance is not causation. MedSignal is a detection and triage tool — it accelerates human review, it does not replace it.
- The HITL gate exists because LLM-generated safety briefs require clinical judgment before publication.
- Non-English adverse event reports are out of scope.

---

## References

1. FDA Adverse Event Reporting System (FAERS) — https://www.fda.gov/drugs/surveillance/questions-and-answers-fdas-adverse-event-reporting-system-faers
2. Evans S.J.W. et al. (2001). Use of proportional reporting ratios (PRRs) for signal generation from spontaneous adverse drug reaction reports. *Pharmacoepidemiology and Drug Safety*, 10(6), 483–486.
3. LangGraph Documentation — https://langchain-ai.github.io/langgraph/
4. ChromaDB Documentation — https://docs.trychroma.com/
5. Cormack G.V. et al. (2009). Reciprocal rank fusion outperforms Condorcet and individual rank learning methods. *SIGIR '09*.
6. NCBI E-utilities API — https://www.ncbi.nlm.nih.gov/books/NBK25497/
