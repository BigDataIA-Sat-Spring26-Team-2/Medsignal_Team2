# MedSignal — CLAUDE.md
> Project context file for AI-assisted development.
> Load this into every session before asking questions or generating code.

---

## Project Identity

**Name:** MedSignal — Drug Safety Signal Detection Platform
**Course:** DAMG 7245 · Big Data and Intelligent Analytics · Northeastern University · Spring 2026
**Team:**
- Samiksha Rajesh Gupta (002310743) — Data Engineering Lead
- Prachi Ganpatrao Pradhan (002339613) — Data Processing & LLM Lead
- Siddharth Rakesh Shukla (002303230) — Infrastructure & Quality Lead

---

## Problem Statement

FDA FAERS receives 2M+ adverse event reports per year. Signals accumulate in the data weeks-to-months before the FDA formally communicates them. Existing tools stop at the statistical layer and do not automatically retrieve literature or generate clinically actionable summaries. MedSignal closes this gap.

---

## Architecture: 5 Layers

```
L1 Prep       faers_prep.py · rxnorm_cache_builder · load_pubmed.py
L2 Kafka      4 topics: faers_demo · faers_drug · faers_reac · faers_outc
L3 Spark      Branch 1 (clean+join) → Branch 2 (PRR computation)
L4 Agents     Agent 1 → Agent 2 → Agent 3  (LangGraph linear pipeline)
L5 UI         Streamlit: Signal Feed · Signal Detail · HITL Queue · Evaluation
```

---

## Data Sources

| Source | Purpose | Notes |
|--------|---------|-------|
| FDA FAERS 2023 | Primary — adverse event reports | 4 quarters, ASCII ZIP, `$` delimited, latin1 encoding |
| PubMed / NCBI Entrez | Secondary — safety literature | 200 PMIDs/drug, 10 golden drugs |
| NIH RxNorm REST API | Drug name normalization | Called once, cached in Snowflake |

### FAERS File Schema (4 of 7 files used)

| File | Q1 2023 Rows | Contents |
|------|-------------|---------|
| DEMO | ~432,144 | Patient demographics, report dates, `caseid`, `caseversion` |
| DRUG | ~1,899,503 | Drug records per case — `role_cod`, `prod_ai`, `drugname` |
| REAC | ~1,491,473 | MedDRA adverse reaction terms — `pt` (preferred term) |
| OUTC | ~309,217 | Outcome codes — `outc_cod`: DE, HO, LT |

**Full-year 2023 (all 4 quarters):**
- DEMO: 1,673,637 rows
- DRUG: 7,473,722 rows
- After Branch 1: ~5,012,904 drug-reaction pairs · 6,425 unique drugs · 16,381 unique MedDRA terms

**Files NOT used:** THER, INDI, RPSR (high missingness or not needed for PRR)

---

## Key Field Definitions

| Field | File | Meaning |
|-------|------|---------|
| `primaryid` | All | Unique report identifier — **the join key** |
| `caseid` | DEMO, DRUG | Logical case — multiple `primaryid` can share a `caseid` across quarters |
| `caseversion` | DEMO | Version number — keep **highest** per `caseid` (caseversion dedup) |
| `role_cod` | DRUG | Drug role: **PS** = Primary Suspect (keep only PS), SS/C/I = discard |
| `prod_ai` | DRUG | Active ingredient — primary normalization target |
| `drugname` | DRUG | Brand/trade name — fallback if `prod_ai` null |
| `pt` | REAC | MedDRA Preferred Term — the adverse reaction label |
| `outc_cod` | OUTC | **DE** = Death · **HO** = Hospitalization · **LT** = Life-Threatening |
| `fda_dt` | DEMO | Report date — format YYYYMMDD, **~38% missing** |
| `source_quarter` | Added | Synthetic tag added at Kafka publish time (e.g. `2023Q1`) |

---

## Branch 1: Transformation Pipeline

**7-step sequence:**

1. **Caseversion dedup** — Window function: `PARTITION BY caseid ORDER BY caseversion DESC`, keep `rn=1`
2. **PS filter** — `role_cod = 'PS'` only (~22.74% of DRUG rows)
3. **RxNorm normalize** — Hierarchy: `canonical_name` from cache → `prod_ai` lowercased → `drugname` lowercased
4. **REAC dedup** — `dropDuplicates(["primaryid", "pt"])` — one reaction per case
5. **OUTC aggregation** — `max()` per `primaryid` → `death_flag` (DE), `hosp_flag` (HO), `lt_flag` (LT)
6. **Four-file join** — `DRUG INNER REAC INNER DEMO LEFT OUTC` — **OUTC must be LEFT JOIN** (28% of cases have no outcome record); fill nulls with 0
7. **Pair-level dedup** — `dropDuplicates(["primaryid", "drug_key", "pt"])` → `drug_reaction_pairs`

**⚠ Cartesian Product Risk:** If joined row count > 10M, check join keys and REAC dedup before join.

**Output table:** `drug_reaction_pairs` — ~5M rows, ~4–6M expected for full 2023

---

## Branch 2: PRR Computation

### PRR Formula

```
PRR = (A / (A + B)) / (C / (C + D))

A = cases with drug X AND reaction Y
B = cases with drug X WITHOUT reaction Y
C = cases with any OTHER drug AND reaction Y
D = cases with any OTHER drug WITHOUT reaction Y
```

**Canonical reference:** Evans et al. (2001), *Pharmacoepidemiology and Drug Safety*, DOI: 10.1002/pds.677

### Threshold Filters

| Threshold | Production | POC (single quarter) |
|-----------|-----------|---------------------|
| A (case count) | ≥ 50 | ≥ 30 |
| C (other reaction count) | ≥ 200 | ≥ 100 |
| drug_total | ≥ 1,000 | ≥ 500 |
| PRR | ≥ 2.0 | ≥ 2.0 |

**Mode selection:** Triggered automatically when `total_rows < 1,000,000` (POC_THRESHOLD).

### Quality Filters (applied after threshold)

| Filter | Logic |
|--------|-------|
| Junk term filter | Remove administrative MedDRA terms: `drug ineffective`, `product use issue`, `off label use`, `drug interaction`, `no adverse event`, `product quality issue`, etc. |
| Single-quarter spike filter | Remove signals where > 70% of cases concentrated in one quarter |
| Late-surge filter | Remove signals where > 85% of cases in Q3+Q4 (`SPIKE_MAX_PCT=0.70`, `SURGE_LATE_PCT=0.85`) |

**Expected output:** 1,500–3,000 signals in `signals_flagged`

### PRR Validation Checkpoint

```sql
SELECT drug_key, pt, prr, case_count
FROM signals_flagged
WHERE drug_key ILIKE '%finasteride%'
  AND pt ILIKE '%depression%';
-- Expected: PRR >= 2.0, case_count = 67 (full-year 2023)
-- Current checkpoint in code: gabapentin × cardio-respiratory arrest
```

**Rule:** If checkpoint fails → stop pipeline, do not run agents.

---

## Scoring: StatScore

Computed deterministically by Agent 1. Range: [0, 1].

```python
prr_score    = min(prr / 4.0, 1.0)                           # weight 0.50
volume_score = min(log10(case_count) / log10(50), 1.0)        # weight 0.30
severity     = 1.0 if death  else  0.75 if lt  else  0.50 if hosp  else  0.0  # weight 0.20

StatScore = (prr_score * 0.50) + (volume_score * 0.30) + (severity * 0.20)
```

## Scoring: LitScore

Computed deterministically by Agent 2. Range: [0, 1].

```python
relevance_score = 1.0 - (avg_distance / 1.5)   # weight 0.70
volume_score    = min(len(docs) / 5.0, 1.0)     # weight 0.30

LitScore = (relevance_score * 0.70) + (volume_score * 0.30)
# LitScore = 0.0 when no abstracts above threshold
```

**StatScore and LitScore are NEVER combined.** Assigning weights between statistical and literature evidence requires pharmacovigilance domain expertise the team does not have. Both are presented independently to the human reviewer.

---

## Priority Tier Matrix

| Tier | Condition | Meaning |
|------|-----------|---------|
| P1 — Review First | StatScore ≥ 0.7 AND LitScore ≥ 0.5 | Strong statistical + literature support |
| P2 — Review Second | StatScore ≥ 0.7 AND LitScore < 0.5 | Strong statistical, weak literature |
| P3 — Review Third | StatScore < 0.7 AND LitScore ≥ 0.5 | Moderate statistical, good literature |
| P4 — Review Last | StatScore < 0.7 AND LitScore < 0.5 | Weak on both dimensions |

---

## LangGraph Agent Pipeline

**Structure:** Linear. Agent 1 → Agent 2 → Agent 3 → END. No loops. No supervisor nodes.

### Agent 1 — Signal Detector (owner: Samiksha)
- **Input:** `signals_flagged` row
- **Computes:** `StatScore` (deterministic)
- **LLM call:** GPT-4o — generates 3 PubMed search queries (mechanistic / epidemiological / clinical outcomes angle)
- **Why LLM here:** Template queries cannot capture biomedical terminology that varies by drug class and reaction type

### Agent 2 — Literature Retriever (owner: Prachi)
- **Input:** 3 search queries from Agent 1
- **Computes:** `LitScore` (deterministic, ChromaDB cosine similarity only)
- **No LLM used**
- **Retrieval:** HNSW dense + BM25 sparse → Reciprocal Rank Fusion (RRF)
- **Threshold:** cosine similarity ≥ 0.60
- **Returns:** Top-5 abstracts

### Agent 3 — Assessor (owner: Siddharth)
- **Input:** StatScore + LitScore + abstracts
- **Assigns:** Priority tier (rule-based matrix, no LLM)
- **LLM call:** GPT-4o — generates `SafetyBrief`
- **Validates:** Pydantic v2 before writing to DB
- **Retry:** 1 retry on Pydantic failure with stricter prompt
- **Citation guard:** Any PMID in `brief_text` not in retrieved set → flagged as hallucination, removed

### LangGraph State: `SignalState`

```python
# Stage 0 — input
drug_key, pt, prr, case_count, death_count, hosp_count

# Stage 1 — Agent 1 adds
stat_score, search_queries

# Stage 2 — Agent 2 adds
abstracts, lit_score

# Stage 3 — Agent 3 adds
priority, brief
```

---

## SafetyBrief Schema (Pydantic v2)

```python
class SafetyBrief(BaseModel):
    brief_text: str
    key_findings: List[str]
    pmids_cited: List[str]
    recommended_action: str
    drug_key: str
    pt: str
    stat_score: float = Field(ge=0.0, le=1.0)
    lit_score: float = Field(ge=0.0, le=1.0)
    priority: Literal["P1", "P2", "P3", "P4"]
    generated_at: str  # ISO datetime
```

---

## Database Schema

### `drug_reaction_pairs` (Branch 1 output)
```sql
PRIMARY KEY (primaryid, drug_key, pt)
-- ~5M rows, ~4–6M for full 2023
```
Columns: `primaryid`, `caseid`, `drug_key`, `rxcui`, `pt`, `fda_dt`, `death_flag`, `hosp_flag`, `lt_flag`, `source_quarter`

### `signals_flagged` (Branch 2 output)
```sql
PRIMARY KEY (drug_key, pt)
-- 1,500–3,000 rows
```
Columns: `drug_key`, `pt`, `prr`, `drug_reaction_count (A)`, `drug_no_reaction_count (B)`, `other_reaction_count (C)`, `other_no_reaction_count (D)`, `death_count`, `hosp_count`, `lt_count`, `drug_total`, `stat_score`

### `safety_briefs` (Agent pipeline output)
```sql
UNIQUE (drug_key, pt)
```
Columns: `drug_key`, `pt`, `stat_score`, `lit_score`, `priority`, `brief_text`, `key_findings` (JSONB), `pmids_cited` (JSONB), `recommended_action`, `model_used`, `generated_at`

### `hitl_decisions` (HITL — immutable log)
```sql
-- Never UPDATE. Always INSERT new row.
```
Columns: `id` (SERIAL), `drug_key`, `pt`, `decision` (approve/reject/escalate), `reviewer_note`, `decided_at`

### `rxnorm_cache`
```sql
PRIMARY KEY (prod_ai)
```
Columns: `prod_ai`, `rxcui`, `canonical_name`
~8,636 rows — built once via `rxnorm_service.py` / `rxnorm_snowflake_migration.py`

---

## ChromaDB / PubMed

- **Collection:** `pubmed_abstracts`
- **Distance metric:** `hnsw:space: cosine` ← enforced at get_or_create time
- **Embedding model:** `all-MiniLM-L6-v2` (HuggingFace, local, 384-dim)
- **Model MUST be identical at index time and query time**
- **Similarity threshold:** 0.60 (cosine similarity = 1 - distance)
- **Target corpus:** 1,800–1,930 abstracts across 10 golden drugs
- **uid format:** `{drug_name}_{pmid}` — same PMID stored separately per drug (intentional)

### Hybrid Retrieval
- Dense: ChromaDB HNSW
- Sparse: BM25 (`rank-bm25`)
- Fusion: Reciprocal Rank Fusion (RRF)
- **Justification:** POC showed zero overlap between retrievers for warfarin × skin necrosis — BM25 caught the correct paper at rank 1 while HNSW returned zero results

---

## Golden Signal Validation Set

10 drug-reaction pairs with documented FDA safety communications in 2023:

| Drug | Reaction | FDA Comm. Date | POC Lead Time |
|------|----------|---------------|---------------|
| dupilumab | skin fissures / eye inflammation | 2024-01-16 | ~291 days |
| gabapentin | cardiorespiratory arrest | 2023-12-19 | ~162 days |
| pregabalin | coma | 2023-12-19 | ~162 days |
| levetiracetam | tonic-clonic seizure | 2023-11-28 | ~140 days |
| tirzepatide | injection site / hunger | 2023-09-22 | ~80 days |
| semaglutide | increased appetite | 2023-09-22 | ~80 days |
| empagliflozin | HbA1c increased | 2023-08-01 | ~55 days |
| bupropion | seizure | 2023-05-11 | ~13 days |
| dapagliflozin | GFR decreased | 2023-05-09 | ~11 days |
| metformin | diabetic ketoacidosis | 2023-04-13 | ~13 days |

**POC lead times are preliminary.** Final values from full validated pipeline.
**Detection lead time = FDA comm. date − earliest quarter signal first exceeds PRR threshold.**
**Caveat:** Lead time is only meaningful alongside precision. Golden set was constructed from known signals → evaluate with selection bias in mind.

---

## HITL Design

- **All signals routed to HITL queue** — no automated approval path
- Queue sorted: P1 → P2 → P3 → P4, then StatScore descending within tier
- **Two decisions:** `approve` / `reject`
- **Decisions are immutable** — never UPDATE, always INSERT new row
- **Every decision logged** with timestamp in `hitl_decisions`

---

## Kafka & Infrastructure

- **4 topics:** `faers_demo`, `faers_drug`, `faers_reac`, `faers_outc`
- **Partitions:** 4 per topic (enables 4 parallel Spark tasks)
- **Replication:** 1 (single broker dev environment)
- **Listeners:** PLAINTEXT `kafka:29092` (internal) · PLAINTEXT_HOST `localhost:9092` (external)
- **Spark reads topics with:** `spark.read` (batch) — NOT `spark.readStream`
  - FAERS is a fixed quarterly dataset. No late-arriving data, no watermarking needed.
- **`docker compose` must be run from `docker/` subfolder**
- **Container names:** use hyphens (e.g. `medsignal-kafka`)

---

## Storage: Snowflake (current)

Migrated from Supabase/PostgreSQL. Key notes:
- `snowflake-connector-python` for direct queries
- Spark writes via Snowflake JDBC connector (`spark-snowflake_2.12`)
- `ON CONFLICT DO UPDATE` → replaced with Snowflake `MERGE` syntax
- Env vars: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_WAREHOUSE`

---

## RxNorm Normalization

Two API calls per drug name:
1. Name → RxCUI (`/rxcui.json`)
2. RxCUI → base ingredient TTY=IN (`/rxcui/{rxcui}/related.json?tty=IN`)

**Why TTY=IN:** Collapses salt forms to base ingredient:
- `bupropion hydrochloride` → `bupropion`
- `dapagliflozin propanediol` → `dapagliflozin`
- `gabapentin enacarbil` → `gabapentin`

**Rate limit:** 0.12s sleep between calls (~8 req/s, safely under 10 req/s with API key)

---

## LLM Usage

| Point | Model | Purpose | Tokens/call |
|-------|-------|---------|------------|
| Agent 1 | GPT-4o (mini in dev) | Generate 3 PubMed search queries | ~220 |
| Agent 3 | GPT-4o (mini in dev) | SafetyBrief synthesis | ~2,100 |

- Temperature: 0 (reproducibility)
- Hard spend limit: $10 on OpenAI account before any run
- Estimated total cost: ~$0.25 (GPT-4o mini dev + one GPT-4o final run)
- Agent 2: **no LLM** — ChromaDB cosine similarity only

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Ingestion | Apache Kafka 7.5.0 (Confluent) |
| Processing | Apache Spark 3.5.x |
| Relational DB | Snowflake (migrated from PostgreSQL 15) |
| Vector store | ChromaDB (local `PersistentClient`) |
| Embeddings | `all-MiniLM-L6-v2` (HuggingFace, 384-dim) |
| Agent framework | LangGraph |
| LLM | GPT-4o / GPT-4o mini (OpenAI) |
| Output validation | Pydantic v2 |
| Frontend | Streamlit |
| Container | Docker Compose |
| Observability | Prometheus |
| Drug normalization | NIH RxNorm REST API |
| Dev environment | Poetry |

---

## Streamlit Pages

| Page | Owner | Data source |
|------|-------|------------|
| 1 — Signal Feed | Samiksha | `signals_flagged` + `safety_briefs` via FastAPI |
| 2 — Signal Detail | Prachi | `safety_briefs` via FastAPI |
| 3 — HITL Queue | Siddharth | `signals_flagged` + HITL decision POST via FastAPI |
| 4 — Evaluation Dashboard | Samiksha | `lead_time_results.json` + `signals_flagged` |

**FastAPI backend:** port 8001. Streamlit never queries DB directly.

---

## Evaluation KPIs

| KPI | Target |
|-----|--------|
| Signal detection accuracy | ≥ 8/10 golden signals correctly flagged |
| Detection lead time | Positive for majority of golden signals |
| Spark runtime | Branch 1 + 2 < 3 hours on dev machine (local mode) |
| ChromaDB retrieval quality | ≥ 3/5 abstracts above 0.60 cosine for each golden drug |
| Citation validation pass rate | 0 fabricated PMIDs in any SafetyBrief |
| SafetyBrief quality | ≥ 7/10 quality pass against 4-criteria rubric |

### SafetyBrief Quality Rubric

| Criterion | Pass Condition |
|-----------|---------------|
| Signal identification | Brief correctly names drug and reaction |
| Literature grounding | Every claim traceable to a provided abstract |
| Citation accuracy | All PMIDs in `brief_text` appear in `pmids_cited` |
| Tier consistency | Recommended action consistent with priority tier |

---

## Prometheus Metrics

```
medsignal_kafka_records_total        Counter  [topic]
medsignal_spark_job_seconds          Histogram [branch]
medsignal_signals_flagged_total      Gauge
medsignal_agent_seconds              Histogram [agent]
medsignal_safety_briefs_total        Counter  [priority, status]
medsignal_hitl_queue_depth           Gauge
medsignal_hitl_decisions_total       Counter  [decision]
medsignal_llm_tokens_total           Counter  [agent, type]
```

Endpoint: `http://localhost:9090/metrics`

---

## Critical Gotchas

| Issue | Detail |
|-------|--------|
| OUTC join | **Must be LEFT JOIN** — 28% of cases have no outcome record. Inner join silently drops them. |
| Caseversion dedup | Applied to DEMO, not DRUG. Same `caseid` across quarters = follow-up update, keep highest version. |
| PS filter | Must apply **before** the join, not after. 78% of DRUG rows are non-PS — filtering first shrinks the join input dramatically. |
| Cartesian product | The #1 pipeline failure. If joined rows > 10M, check join key and REAC dedup before join. |
| HNSW distance vs similarity | `similarity = 1 - distance`. Only valid because collection uses `hnsw:space=cosine`. |
| Combination drugs | Backslash separator in `prod_ai` (e.g. `ACETAMINOPHEN\HYDROCODONE`) → split and take first component. |
| Special chars in passwords | URL-based connection strings break with special characters. Use individual env vars. |
| Docker compose path | Must run from `docker/` subfolder: `docker compose -f docker/docker-compose.yml up -d` |
| VARCHAR(500) | Insufficient for drug names — use TEXT. |
| Poetry env | Always activate with `poetry run` or `poetry shell` before executing project scripts. |
| Circular validation | PRR thresholds were calibrated to recover known golden signals. This is standard pharmacovigilance calibration, not a flaw — but acknowledge it openly. Mitigation: sensitivity analysis across A threshold values. |

---

## File Map

```
scripts/
  faers_prep.py                  Thin Kafka producer — no logic, just publish
  rxnorm_snowflake_migration.py  One-time migration Supabase → Snowflake

app/scripts/
  download_faers.py              Download FAERS ZIPs from FDA portal
  load_pubmed.py                 Fetch PubMed abstracts → ChromaDB

app/services/
  rxnorm_service.py              RxNorm cache builder (builds via NIH API)

app/utils/
  chromadb_client.py             Shared ChromaDB client factory

app/agents/
  state.py                       LangGraph SignalState TypedDict

pipelines/
  spark_branch1.py               Spark Branch 1: clean + join → drug_reaction_pairs
  branch2_prr.py                 Spark Branch 2: PRR computation → signals_flagged

docker/
  docker-compose.yml             All services: Kafka, Zookeeper, ChromaDB, Postgres
  kafka_topics.sh                Creates 4 Kafka topics (run once after compose up)

streamlit_app/
  pages/                         4 Streamlit pages

evaluation/
  lead_time.py                   Detection lead time computation
  rubric_scorer.py               SafetyBrief quality rubric
  hallucination_check.py         PMID fabrication detection
```

---

## Milestone Gates

| Milestone | Gate |
|-----------|------|
| M1 | PRR checkpoint passing · all 10 golden drugs in `signals_flagged` · row count 4M–6M |
| M2 | ChromaDB loaded 1,800+ abstracts · retrieval validation ≥ 0.60 for ≥ 8/10 drugs |
| M3 | All 3 agents producing Pydantic-validated SafetyBriefs for 10 golden drugs |
| M4 | All 4 Streamlit pages functional with real data |
| M5 | All evaluation metrics computed and documented |
| M6 | Full pipeline run on complete 2023 data · Prometheus live · demo rehearsed |

---

## Questions Every Team Member Must Answer

1. Why is the OUTC join a left join? What % of cases would be lost if inner?
2. What does caseversion dedup do and why is it on DEMO not DRUG?
3. What is the PRR formula? What do A, B, C, D represent?
4. Why is the RxNorm cache built before Spark runs, not during?
5. What does `role_cod = PS` mean and why discard SS, C, I?
6. Why does the pipeline use `spark.read` instead of `spark.readStream`?
7. What happens when Agent 3's Pydantic validation fails?
8. How does the citation validator catch hallucinated PMIDs?
9. Why are StatScore and LitScore not combined into one weighted score?
10. What is the junk term filter vs. the single-quarter spike filter?
11. What would you change to add a new quarterly FAERS release?
12. What is the finasteride-depression PRR checkpoint and why does it exist?

---

*Generated from MedSignal proposal v04062026 · Week 1–3 lab documents · codebase repomix*
