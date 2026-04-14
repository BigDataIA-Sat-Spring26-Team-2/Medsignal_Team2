CREATE DATABASE IF NOT EXISTS MEDSIGNAL;
USE DATABASE MEDSIGNAL;
CREATE SCHEMA IF NOT EXISTS PUBLIC;


-- =============================================================
-- 1. rxnorm_cache
-- Built once before Branch 1 runs.
-- Read by Branch 1 via snowflake-connector-python → pandas.
-- =============================================================
CREATE TABLE IF NOT EXISTS rxnorm_cache (
    prod_ai        VARCHAR(500) NOT NULL,
    rxcui          VARCHAR(50),
    canonical_name VARCHAR(500),
    PRIMARY KEY (prod_ai)
);


-- =============================================================
-- 2. drug_reaction_pairs
-- Output of Spark Branch 1.
-- One row per unique (primaryid, drug_key, pt) triple.
-- Expected volume: ~5 million rows across 2023 quarterly data.
-- =============================================================
CREATE TABLE IF NOT EXISTS drug_reaction_pairs (
    primaryid      NUMBER(18)   NOT NULL,
    caseid         NUMBER(18)   NOT NULL,
    drug_key       VARCHAR(500) NOT NULL,
    rxcui          VARCHAR(50),
    pt             VARCHAR(500) NOT NULL,
    fda_dt         DATE,
    death_flag     NUMBER(1)    NOT NULL DEFAULT 0,
    hosp_flag      NUMBER(1)    NOT NULL DEFAULT 0,
    lt_flag        NUMBER(1)    NOT NULL DEFAULT 0,
    source_quarter VARCHAR(10)  NOT NULL,
 
    PRIMARY KEY (primaryid, drug_key, pt),
 
    CONSTRAINT chk_death_flag CHECK (death_flag IN (0, 1)),
    CONSTRAINT chk_hosp_flag  CHECK (hosp_flag  IN (0, 1)),
    CONSTRAINT chk_lt_flag    CHECK (lt_flag     IN (0, 1))
);


-- =============================================================
-- 3. signals_flagged
-- Output of Spark Branch 2.
-- One row per (drug_key, pt) pair that cleared all PRR
-- thresholds and quality filters.
-- =============================================================
CREATE TABLE IF NOT EXISTS signals_flagged (
    drug_key                VARCHAR(500)   NOT NULL,
    pt                      VARCHAR(500)   NOT NULL,
    prr                     NUMBER(10, 4)  NOT NULL,
    drug_reaction_count     NUMBER(18)     NOT NULL,
    drug_no_reaction_count  NUMBER(18)     NOT NULL,
    other_reaction_count    NUMBER(18)     NOT NULL,
    other_no_reaction_count NUMBER(18)     NOT NULL,
    death_count             NUMBER(10)     NOT NULL DEFAULT 0,
    hosp_count              NUMBER(10)     NOT NULL DEFAULT 0,
    lt_count                NUMBER(10)     NOT NULL DEFAULT 0,
    drug_total              NUMBER(18)     NOT NULL,
    computed_at             TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    PRIMARY KEY (drug_key, pt)
);


-- =============================================================
-- 4. safety_briefs
-- Output of the LangGraph agent pipeline.
-- One row per flagged signal.
-- key_findings and pmids_cited stored as VARIANT (JSON arrays).
-- =============================================================
CREATE TABLE IF NOT EXISTS safety_briefs (
    brief_id           INT AUTOINCREMENT PRIMARY KEY,
    drug_key           VARCHAR(500)  NOT NULL,
    pt                 VARCHAR(500)  NOT NULL,
    stat_score         NUMBER(6, 4),
    lit_score          NUMBER(6, 4),
    priority           VARCHAR(2),
    brief_text         TEXT,
    key_findings       VARIANT,
    pmids_cited        VARIANT,
    recommended_action TEXT,
    model_used         VARCHAR(50),
    input_tokens       NUMBER(10),
    output_tokens      NUMBER(10),
    generation_error   BOOLEAN       NOT NULL DEFAULT FALSE,
    generated_at       TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (drug_key, pt)
        REFERENCES signals_flagged(drug_key, pt)
);


-- =============================================================
-- 5. hitl_decisions
-- Output of the Streamlit HITL reviewer interface.
-- Immutable append-only log — decisions are never updated.
-- One row per reviewer action.
-- =============================================================
CREATE TABLE IF NOT EXISTS hitl_decisions (
    drug_key      VARCHAR(500)  NOT NULL,
    pt            VARCHAR(500)  NOT NULL,
    brief_id      INT,
    decision      VARCHAR(10)   NOT NULL,
    reviewer_note TEXT,
    decided_at    TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (drug_key, pt, decided_at),
    FOREIGN KEY (drug_key, pt)
        REFERENCES signals_flagged(drug_key, pt),
    FOREIGN KEY (brief_id)
        REFERENCES safety_briefs(brief_id)
);


SHOW TABLES IN DATABASE MEDSIGNAL;