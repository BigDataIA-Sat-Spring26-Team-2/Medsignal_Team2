-- =============================================================
-- 1. rxnorm_cache
-- Built once before the Spark job runs.
-- Broadcast to all Spark executors as a read-only variable
-- during Branch 1 normalization.
-- No FK relationships — used by Spark Branch 1 at runtime only.
-- =============================================================
CREATE TABLE IF NOT EXISTS rxnorm_cache (
    prod_ai        VARCHAR(500) NOT NULL,
    rxcui          VARCHAR(50),
    canonical_name VARCHAR(500),

    PRIMARY KEY (prod_ai)
);

CREATE INDEX IF NOT EXISTS idx_rxc_rxcui
    ON rxnorm_cache(rxcui);

CREATE INDEX IF NOT EXISTS idx_rxc_canonical
    ON rxnorm_cache(canonical_name);


-- =============================================================
-- 2. drug_reaction_pairs
-- Output of Spark Branch 1.
-- One row per unique (primaryid, drug_key, pt) triple
-- after join, deduplication, PS filter, and normalization.
-- Expected volume: ~5 million rows across 2023 quarterly data.
--
-- NOTE: rxcui is informational only — no FK to rxnorm_cache
-- because multiple prod_ai values map to the same rxcui,
-- making rxcui non-unique in rxnorm_cache. The normalization
-- join is enforced by Spark Branch 1 logic, not DB constraints.
-- =============================================================
CREATE TABLE IF NOT EXISTS drug_reaction_pairs (
    primaryid      BIGINT       NOT NULL,
    caseid         BIGINT       NOT NULL,
    drug_key       VARCHAR(500) NOT NULL,
    rxcui          VARCHAR(50),
    pt             VARCHAR(500) NOT NULL,
    fda_dt         DATE,
    death_flag     SMALLINT     NOT NULL DEFAULT 0,
    hosp_flag      SMALLINT     NOT NULL DEFAULT 0,
    lt_flag        SMALLINT     NOT NULL DEFAULT 0,
    source_quarter VARCHAR(10)  NOT NULL,

    PRIMARY KEY (primaryid, drug_key, pt),

    CONSTRAINT chk_death_flag CHECK (death_flag IN (0, 1)),
    CONSTRAINT chk_hosp_flag  CHECK (hosp_flag  IN (0, 1)),
    CONSTRAINT chk_lt_flag    CHECK (lt_flag     IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_drp_drug_key
    ON drug_reaction_pairs(drug_key);

CREATE INDEX IF NOT EXISTS idx_drp_pt
    ON drug_reaction_pairs(pt);

CREATE INDEX IF NOT EXISTS idx_drp_drug_pt
    ON drug_reaction_pairs(drug_key, pt);

CREATE INDEX IF NOT EXISTS idx_drp_rxcui
    ON drug_reaction_pairs(rxcui);

CREATE INDEX IF NOT EXISTS idx_drp_fda_dt
    ON drug_reaction_pairs(fda_dt);

CREATE INDEX IF NOT EXISTS idx_drp_source_quarter
    ON drug_reaction_pairs(source_quarter);


-- =============================================================
-- 3. signals_flagged
-- Output of Spark Branch 2.
-- One row per (drug_key, pt) pair that cleared all PRR
-- thresholds and quality filters.
--
-- Contingency table columns:
--   drug_reaction_count     (A): drug X with reaction Y
--   drug_no_reaction_count  (B): drug X without reaction Y
--   other_reaction_count    (C): other drugs with reaction Y
--   other_no_reaction_count (D): other drugs without reaction Y
--
-- PRR = (drug_reaction_count / (drug_reaction_count + drug_no_reaction_count))
--     / (other_reaction_count / (other_reaction_count + other_no_reaction_count))
--
-- NOTE: Threshold values (PRR, A, C, drug_total) are pipeline
-- parameters subject to change during full pipeline validation.
-- Only non-negative guards are enforced at the schema level.
-- Threshold enforcement is handled by Spark Branch 2 logic.
-- =============================================================
CREATE TABLE IF NOT EXISTS signals_flagged (
    drug_key                VARCHAR(500)   NOT NULL,
    pt                      VARCHAR(500)   NOT NULL,
    prr                     NUMERIC(10, 4) NOT NULL,
    drug_reaction_count     BIGINT         NOT NULL,
    drug_no_reaction_count  BIGINT         NOT NULL,
    other_reaction_count    BIGINT         NOT NULL,
    other_no_reaction_count BIGINT         NOT NULL,
    death_count             INT            NOT NULL DEFAULT 0,
    hosp_count              INT            NOT NULL DEFAULT 0,
    lt_count                INT            NOT NULL DEFAULT 0,
    drug_total              BIGINT         NOT NULL,
    computed_at             TIMESTAMP      NOT NULL DEFAULT NOW(),

    PRIMARY KEY (drug_key, pt),

    -- Non-negative guards on PRR and contingency table
    CONSTRAINT chk_prr
        CHECK (prr > 0),
    CONSTRAINT chk_drug_reaction_count
        CHECK (drug_reaction_count >= 0),
    CONSTRAINT chk_drug_no_reaction_count
        CHECK (drug_no_reaction_count >= 0),
    CONSTRAINT chk_other_reaction_count
        CHECK (other_reaction_count >= 0),
    CONSTRAINT chk_other_no_reaction_count
        CHECK (other_no_reaction_count >= 0),
    CONSTRAINT chk_drug_total
        CHECK (drug_total >= 0),

    -- Outcome count non-negative constraints
    -- death_count, hosp_count, lt_count are aggregated sums
    -- of SMALLINT flags (0 or 1) and cannot logically be negative
    CONSTRAINT chk_death_count
        CHECK (death_count >= 0),
    CONSTRAINT chk_hosp_count
        CHECK (hosp_count >= 0),
    CONSTRAINT chk_lt_count
        CHECK (lt_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_sf_prr
    ON signals_flagged(prr DESC);

CREATE INDEX IF NOT EXISTS idx_sf_drug_total
    ON signals_flagged(drug_total DESC);

CREATE INDEX IF NOT EXISTS idx_sf_computed_at
    ON signals_flagged(computed_at DESC);


-- =============================================================
-- 4. safety_briefs
-- Output of the LangGraph agent pipeline.
-- One row per flagged signal.
-- Agent 1 writes stat_score.
-- Agent 2 writes lit_score.
-- Agent 3 writes priority, brief fields, and token counts.
--
-- When generation_error = TRUE:
--   brief_text, key_findings, pmids_cited,
--   recommended_action, priority may all be NULL.
-- When generation_error = FALSE:
--   priority must be one of P1-P4.
-- =============================================================
CREATE TABLE IF NOT EXISTS safety_briefs (
    brief_id           SERIAL         PRIMARY KEY,
    drug_key           VARCHAR(500)   NOT NULL,
    pt                 VARCHAR(500)   NOT NULL,
    stat_score         NUMERIC(6, 4),
    lit_score          NUMERIC(6, 4),
    priority           CHAR(2),
    brief_text         TEXT,
    key_findings       JSONB,
    pmids_cited        JSONB,
    recommended_action TEXT,
    model_used         VARCHAR(50),
    input_tokens       INT,
    output_tokens      INT,
    generation_error   BOOLEAN        NOT NULL DEFAULT FALSE,
    generated_at       TIMESTAMP      NOT NULL DEFAULT NOW(),

    FOREIGN KEY (drug_key, pt)
        REFERENCES signals_flagged(drug_key, pt)
        ON DELETE CASCADE,

    -- Priority must be set when brief was generated successfully
    CONSTRAINT chk_priority
        CHECK (
            generation_error = TRUE
            OR priority IN ('P1', 'P2', 'P3', 'P4')
        ),
    -- Priority cannot be NULL when generation succeeded
    CONSTRAINT chk_priority_required
        CHECK (
            generation_error = TRUE
            OR priority IS NOT NULL
        ),

    -- Scores are in [0, 1] when present
    CONSTRAINT chk_stat_score
        CHECK (stat_score IS NULL
            OR (stat_score >= 0.0 AND stat_score <= 1.0)),
    CONSTRAINT chk_lit_score
        CHECK (lit_score IS NULL
            OR (lit_score >= 0.0 AND lit_score <= 1.0)),

    -- Token counts are non-negative when present
    -- input_tokens and output_tokens are returned by the
    -- OpenAI API usage field and cannot be negative
    CONSTRAINT chk_input_tokens
        CHECK (input_tokens IS NULL OR input_tokens >= 0),
    CONSTRAINT chk_output_tokens
        CHECK (output_tokens IS NULL OR output_tokens >= 0)
);

CREATE INDEX IF NOT EXISTS idx_sb_drug_pt
    ON safety_briefs(drug_key, pt);

CREATE INDEX IF NOT EXISTS idx_sb_priority
    ON safety_briefs(priority);

CREATE INDEX IF NOT EXISTS idx_sb_generation_error
    ON safety_briefs(generation_error)
    WHERE generation_error = TRUE;

CREATE INDEX IF NOT EXISTS idx_sb_generated_at
    ON safety_briefs(generated_at DESC);


-- =============================================================
-- 5. hitl_decisions
-- Output of the Streamlit HITL reviewer interface.
-- Immutable append-only log — decisions are never updated.
-- One row per reviewer action.
--
-- Two FK paths back to the signal:
--   brief_id    → safety_briefs     (normal path)
--   drug_key+pt → signals_flagged   (generation_error path)
--
-- PK is (drug_key, pt, decided_at).
-- clock_timestamp() used instead of NOW() to ensure true
-- microsecond uniqueness even within a single transaction.
-- =============================================================
CREATE TABLE IF NOT EXISTS hitl_decisions (
    drug_key      VARCHAR(500) NOT NULL,
    pt            VARCHAR(500) NOT NULL,
    brief_id      INT          NULL
                  REFERENCES safety_briefs(brief_id)
                  ON DELETE SET NULL,
    decision      VARCHAR(10)  NOT NULL,
    reviewer_note TEXT,
    decided_at    TIMESTAMP(6) NOT NULL DEFAULT clock_timestamp(),

    PRIMARY KEY (drug_key, pt, decided_at),

    FOREIGN KEY (drug_key, pt)
        REFERENCES signals_flagged(drug_key, pt)
        ON DELETE CASCADE,

    CONSTRAINT chk_decision
        CHECK (decision IN ('approve', 'reject', 'escalate'))
);

CREATE INDEX IF NOT EXISTS idx_hd_drug_pt
    ON hitl_decisions(drug_key, pt);

CREATE INDEX IF NOT EXISTS idx_hd_brief_id
    ON hitl_decisions(brief_id);

CREATE INDEX IF NOT EXISTS idx_hd_decision
    ON hitl_decisions(decision);

CREATE INDEX IF NOT EXISTS idx_hd_decided_at
    ON hitl_decisions(decided_at DESC);


-- =============================================================
-- VERIFICATION QUERIES
-- Run after schema creation to confirm everything is in place.
-- =============================================================

-- Confirm all five tables exist
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN (
      'rxnorm_cache',
      'drug_reaction_pairs',
      'signals_flagged',
      'safety_briefs',
      'hitl_decisions'
  )
ORDER BY table_name;

-- Confirm all constraints are registered
SELECT
    tc.table_name,
    tc.constraint_name,
    tc.constraint_type,
    cc.check_clause
FROM information_schema.table_constraints tc
LEFT JOIN information_schema.check_constraints cc
    ON tc.constraint_name = cc.constraint_name
WHERE tc.table_schema = 'public'
  AND tc.table_name IN (
      'rxnorm_cache',
      'drug_reaction_pairs',
      'signals_flagged',
      'safety_briefs',
      'hitl_decisions'
  )
ORDER BY tc.table_name, tc.constraint_type, tc.constraint_name;

SELECT
    tc.table_name,
    kcu.column_name,
    ccu.table_name  AS references_table,
    ccu.column_name AS references_column
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage  AS kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema   = kcu.table_schema
JOIN information_schema.constraint_column_usage AS ccu
    ON ccu.constraint_name = tc.constraint_name
    AND ccu.table_schema   = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema    = 'public'
ORDER BY tc.table_name, kcu.column_name;