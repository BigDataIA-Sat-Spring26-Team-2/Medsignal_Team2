"""
tests/integration/test_spark_branch1_integration.py

Integration tests for Spark Branch 1.

Difference from unit tests:
    Unit tests — one function at a time, minimal data, isolated
    Integration tests — multiple functions chained in sequence,
    realistic data, verifies the full pipeline produces correct output

Strategy:
    All tests use spark.createDataFrame() — no Kafka, no Snowflake.
    Each test builds a small but realistic dataset and runs it through
    the same sequence of transformations as the real pipeline:

        parse → dedup_demo
        parse → filter_and_normalize_drug
        parse → dedup_reac
        parse → aggregate_outc
        all four → join → pair dedup → final output

    This catches bugs that unit tests miss — for example a function
    that works correctly in isolation but breaks when its output is
    fed into the next step.

Run:
    poetry run pytest tests/integration/test_spark_branch1_integration.py -v -s

No external dependencies — all tests run offline.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import json
import pytest


# SparkSession fixture

@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("test-branch1-integration")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


# Shared helpers

from pipelines.spark_branch1 import (
    dedup_demo,
    filter_and_normalize_drug,
    dedup_reac,
    aggregate_outc,
)


def make_rxnorm_cache(spark, entries=None):
    """Build RxNorm cache DataFrame with explicit schema."""
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("prod_ai_upper",  StringType(), True),
        StructField("rxcui",          StringType(), True),
        StructField("canonical_name", StringType(), True),
    ])
    data = [(e[0].upper(), e[1], e[2]) for e in (entries or [])]
    return spark.createDataFrame(data, schema)


def make_drug_df(spark, rows):
    """Build drug DataFrame with explicit schema."""
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("primaryid",      StringType(), True),
        StructField("drugname",       StringType(), True),
        StructField("prod_ai",        StringType(), True),
        StructField("role_cod",       StringType(), True),
        StructField("source_quarter", StringType(), True),
    ])
    return spark.createDataFrame(rows, schema)


# ══════════════════════════════════════════════════════════════════════════════
# Integration Test 1 — Full pipeline for one golden signal
# ══════════════════════════════════════════════════════════════════════════════

class TestFullPipelineGoldenSignal:
    """
    Runs a complete Branch 1 pipeline for the metformin × lactic acidosis
    golden signal using synthetic FAERS-style data.

    Pipeline sequence:
        demo_df   → dedup_demo
        drug_df   → filter_and_normalize_drug
        reac_df   → dedup_reac
        outc_df   → aggregate_outc
        all four  → four-file join → pair dedup

    Verifies that the final output contains the expected drug-reaction pair
    with the correct outcome flags — end to end without any external system.
    """

    def test_int_1_metformin_lactic_acidosis_survives_pipeline(self, spark):
        """
        A metformin case with lactic acidosis and a death outcome must
        produce exactly one row in the final output with death_flag = 1.

        This is the most important golden signal — PRR = 30.77 in 2023Q1.
        If this test fails, the PS filter, RxNorm join, or join logic is broken.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType

        # ── DEMO ──────────────────────────────────────────────────────────────
        demo_schema = StructType([
            StructField("primaryid",      StringType(),  True),
            StructField("caseid",         StringType(),  True),
            StructField("caseversion",    StringType(),  True),
            StructField("fda_dt",         StringType(),  True),
            StructField("source_quarter", StringType(),  True),
        ])
        demo_raw = spark.createDataFrame([
            ("1001", "100", "1", "20230301", "2023Q1"),
        ], demo_schema)
        demo_df = dedup_demo(demo_raw)

        # ── DRUG ──────────────────────────────────────────────────────────────
        cache = make_rxnorm_cache(spark, [
            ("METFORMIN", "6809", "metformin"),
        ])
        drug_raw = make_drug_df(spark, [
            ("1001", "GLUCOPHAGE", "METFORMIN", "PS", "2023Q1"),
        ])
        drug_df = filter_and_normalize_drug(drug_raw, cache, spark)

        # ── REAC ──────────────────────────────────────────────────────────────
        reac_schema = StructType([
            StructField("primaryid", StringType(), True),
            StructField("pt",        StringType(), True),
        ])
        reac_raw = spark.createDataFrame([
            ("1001", "lactic acidosis"),
        ], reac_schema)
        reac_df = dedup_reac(reac_raw)

        # ── OUTC ──────────────────────────────────────────────────────────────
        outc_raw = spark.createDataFrame([
            ("1001", "DE"),   # death
        ], ["primaryid", "outc_cod"])
        outc_df = aggregate_outc(outc_raw)

        # ── JOIN ──────────────────────────────────────────────────────────────
        pairs = (
            drug_df
            .join(reac_df, on="primaryid", how="inner")
            .join(demo_df.select("primaryid", "caseid", "fda_dt", "source_quarter"),
                  on="primaryid", how="inner")
            .join(outc_df, on="primaryid", how="left")
            .fillna({"death_flag": 0, "hosp_flag": 0, "lt_flag": 0})
            .dropDuplicates(["primaryid", "drug_key", "pt"])
        )

        # ── ASSERT ────────────────────────────────────────────────────────────
        assert pairs.count() == 1, (
            f"Expected 1 row for metformin x lactic acidosis, got {pairs.count()}"
        )
        row = pairs.collect()[0]
        assert row["drug_key"]   == "metformin",      f"drug_key: {row['drug_key']}"
        assert row["pt"]         == "lactic acidosis", f"pt: {row['pt']}"
        assert row["death_flag"] == 1,                 f"death_flag: {row['death_flag']}"
        assert row["hosp_flag"]  == 0
        assert row["lt_flag"]    == 0


# ══════════════════════════════════════════════════════════════════════════════
# Integration Test 2 — Caseversion dedup feeds correctly into drug join
# ══════════════════════════════════════════════════════════════════════════════

class TestCaseversionDedupWithJoin:
    """
    When the same caseid has two versions, only the highest version's
    primaryid should appear in the final output.

    This tests the handoff between dedup_demo and the four-file join —
    a bug here would cause old case versions to appear in drug_reaction_pairs.
    """

    def test_int_2_old_caseversion_excluded_from_output(self, spark):
        """
        caseid=100 has version 1 (primaryid=1001) and version 2 (primaryid=1002).
        The drug record belongs to primaryid=1001 (old version).
        After dedup_demo keeps only primaryid=1002, the drug record for 1001
        must be dropped by the inner join — it no longer has a matching demo row.
        """
        from pyspark.sql.types import StructType, StructField, StringType

        demo_schema = StructType([
            StructField("primaryid",      StringType(), True),
            StructField("caseid",         StringType(), True),
            StructField("caseversion",    StringType(), True),
            StructField("fda_dt",         StringType(), True),
            StructField("source_quarter", StringType(), True),
        ])
        demo_raw = spark.createDataFrame([
            ("1001", "100", "1", "20230101", "2023Q1"),  # old version
            ("1002", "100", "2", "20230201", "2023Q1"),  # new version — keep
        ], demo_schema)
        demo_df = dedup_demo(demo_raw)

        cache   = make_rxnorm_cache(spark)
        drug_raw = make_drug_df(spark, [
            ("1001", "metformin", "metformin", "PS", "2023Q1"),  # old primaryid
        ])
        drug_df = filter_and_normalize_drug(drug_raw, cache, spark)

        reac_schema = StructType([
            StructField("primaryid", StringType(), True),
            StructField("pt",        StringType(), True),
        ])
        reac_df = spark.createDataFrame([
            ("1001", "nausea"),
        ], reac_schema)
        reac_df = dedup_reac(reac_df)

        pairs = (
            drug_df
            .join(reac_df, on="primaryid", how="inner")
            .join(demo_df.select("primaryid"), on="primaryid", how="inner")
        )

        assert pairs.count() == 0, (
            "Drug record for old primaryid=1001 must be dropped after "
            "dedup_demo keeps only primaryid=1002 (version 2)"
        )


# ══════════════════════════════════════════════════════════════════════════════
# Integration Test 3 — RxNorm normalization groups brand names correctly
# ══════════════════════════════════════════════════════════════════════════════

class TestRxNormNormalizationGrouping:
    """
    Two cases reporting the same drug under different brand names
    (JARDIANCE and GLYXAMBI) should both normalize to 'empagliflozin'
    via the RxNorm cache and appear as the same drug_key in the output.

    This is critical for PRR correctness — brand name fragmentation would
    split one drug's cases into multiple drug_keys, each with too few
    cases to clear the A>=50 threshold.
    """

    def test_int_3_brand_names_normalize_to_same_drug_key(self, spark):
        """
        JARDIANCE and GLYXAMBI both contain empagliflozin.
        Both must produce drug_key = 'empagliflozin' after RxNorm join.
        The final output must have 2 rows both with the same drug_key.
        """
        cache = make_rxnorm_cache(spark, [
            ("EMPAGLIFLOZIN", "2200644", "empagliflozin"),
        ])

        drug_raw = make_drug_df(spark, [
            ("2001", "JARDIANCE",  "EMPAGLIFLOZIN", "PS", "2023Q1"),
            ("2002", "GLYXAMBI",   "EMPAGLIFLOZIN", "PS", "2023Q1"),
        ])
        drug_df = filter_and_normalize_drug(drug_raw, cache, spark)

        reac_schema = __import__("pyspark").sql.types.StructType([
            __import__("pyspark").sql.types.StructField("primaryid", __import__("pyspark").sql.types.StringType(), True),
            __import__("pyspark").sql.types.StructField("pt",        __import__("pyspark").sql.types.StringType(), True),
        ])
        reac_df = spark.createDataFrame([
            ("2001", "diabetic ketoacidosis"),
            ("2002", "diabetic ketoacidosis"),
        ], ["primaryid", "pt"])
        reac_df = dedup_reac(reac_df)

        pairs = (
            drug_df
            .join(reac_df, on="primaryid", how="inner")
            .dropDuplicates(["primaryid", "drug_key", "pt"])
        )

        assert pairs.count() == 2, (
            f"Expected 2 rows (one per case), got {pairs.count()}"
        )
        drug_keys = set(r["drug_key"] for r in pairs.collect())
        assert drug_keys == {"empagliflozin"}, (
            f"Both brand names must normalize to 'empagliflozin', got: {drug_keys}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# Integration Test 4 — Concomitant drugs excluded from PRR pairs
# ══════════════════════════════════════════════════════════════════════════════

class TestConcomitantDrugExclusion:
    """
    A patient taking metformin (PS) and aspirin (C — concomitant) and
    reporting lactic acidosis should produce only one drug-reaction pair:
    metformin × lactic acidosis.

    aspirin must be excluded by the PS filter. If concomitant drugs leaked
    through, aspirin × lactic acidosis would appear and inflate aspirin's
    case count — producing false PRR signals for aspirin.
    """

    def test_int_4_concomitant_drug_excluded_from_pairs(self, spark):
        """
        One patient, two drugs (PS + C), one reaction.
        Final output must have only the PS drug paired with the reaction.
        """
        cache   = make_rxnorm_cache(spark)
        drug_raw = make_drug_df(spark, [
            ("3001", "metformin", "metformin", "PS", "2023Q1"),  # keep
            ("3001", "aspirin",   "aspirin",   "C",  "2023Q1"),  # drop
        ])
        drug_df = filter_and_normalize_drug(drug_raw, cache, spark)

        reac_df = dedup_reac(
            spark.createDataFrame([("3001", "lactic acidosis")], ["primaryid", "pt"])
        )

        pairs = (
            drug_df
            .join(reac_df, on="primaryid", how="inner")
            .dropDuplicates(["primaryid", "drug_key", "pt"])
        )

        assert pairs.count() == 1, (
            f"Expected 1 pair (metformin only), got {pairs.count()}"
        )
        row = pairs.collect()[0]
        assert row["drug_key"] == "metformin", (
            f"Expected metformin but got {row['drug_key']}"
        )
        assert row["pt"] == "lactic acidosis"


# ══════════════════════════════════════════════════════════════════════════════
# Integration Test 5 — Duplicate reactions don't inflate case count
# ══════════════════════════════════════════════════════════════════════════════

class TestReactionDedupInPipeline:
    """
    If the same reaction is reported twice for one patient (e.g. duplicated
    REAC record), dedup_reac should collapse it to one before the join.
    The final output must have one row not two.

    Without this dedup, A (numerator of PRR) would be inflated by duplicate
    reactions — producing artificially high PRR values.
    """

    def test_int_5_duplicate_reaction_produces_one_pair(self, spark):
        """
        One patient, one drug, same reaction twice → one output row.
        """
        cache    = make_rxnorm_cache(spark)
        drug_raw = make_drug_df(spark, [
            ("4001", "dupilumab", "dupilumab", "PS", "2023Q1"),
        ])
        drug_df = filter_and_normalize_drug(drug_raw, cache, spark)

        reac_df = dedup_reac(
            spark.createDataFrame([
                ("4001", "conjunctivitis"),
                ("4001", "conjunctivitis"),  # duplicate
            ], ["primaryid", "pt"])
        )

        pairs = (
            drug_df
            .join(reac_df, on="primaryid", how="inner")
            .dropDuplicates(["primaryid", "drug_key", "pt"])
        )

        assert pairs.count() == 1, (
            f"Duplicate reaction must not inflate pair count. Got {pairs.count()}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# Integration Test 6 — Outcome flags flow correctly through join
# ══════════════════════════════════════════════════════════════════════════════

class TestOutcomeFlagsInPipeline:
    """
    Outcome flags aggregated by aggregate_outc must flow through the
    left join correctly and appear in the final output.

    Two patients — one with death, one with no outcome.
    Both must appear in the output but with different flag values.
    """

    def test_int_6_outcome_flags_correct_after_join(self, spark):
        """
        Patient 5001: dupilumab + conjunctivitis + death → death_flag=1
        Patient 5002: dupilumab + conjunctivitis + no outcome → death_flag=0
        Both must appear in output (left join on OUTC).
        """
        cache    = make_rxnorm_cache(spark)
        drug_raw = make_drug_df(spark, [
            ("5001", "dupilumab", "dupilumab", "PS", "2023Q1"),
            ("5002", "dupilumab", "dupilumab", "PS", "2023Q1"),
        ])
        drug_df = filter_and_normalize_drug(drug_raw, cache, spark)

        reac_df = dedup_reac(
            spark.createDataFrame([
                ("5001", "conjunctivitis"),
                ("5002", "conjunctivitis"),
            ], ["primaryid", "pt"])
        )

        outc_df = aggregate_outc(
            spark.createDataFrame([
                ("5001", "DE"),   # death for patient 5001 only
            ], ["primaryid", "outc_cod"])
        )

        pairs = (
            drug_df
            .join(reac_df, on="primaryid", how="inner")
            .join(outc_df, on="primaryid", how="left")
            .fillna({"death_flag": 0, "hosp_flag": 0, "lt_flag": 0})
            .dropDuplicates(["primaryid", "drug_key", "pt"])
        )

        assert pairs.count() == 2

        rows = {r["primaryid"]: r for r in pairs.collect()}

        # Patient with death
        assert rows["5001"]["death_flag"] == 1, "Patient 5001 should have death_flag=1"
        assert rows["5001"]["hosp_flag"]  == 0

        # Patient without outcome
        assert rows["5002"]["death_flag"] == 0, "Patient 5002 should have death_flag=0"
        assert rows["5002"]["hosp_flag"]  == 0
        assert rows["5002"]["lt_flag"]    == 0


# ══════════════════════════════════════════════════════════════════════════════
# Integration Test 7 — Multi-drug, multi-reaction patient
# ══════════════════════════════════════════════════════════════════════════════

class TestMultiDrugMultiReaction:
    """
    A patient taking two PS drugs and reporting two reactions should produce
    four drug-reaction pairs (cartesian product of drugs × reactions).

    This is the expected behavior for PRR — each drug is assessed against
    each reaction independently.
    """

    def test_int_7_two_drugs_two_reactions_produce_four_pairs(self, spark):
        """
        Patient 6001: dupilumab + metformin, conjunctivitis + nausea
        Expected output: 4 pairs
            dupilumab × conjunctivitis
            dupilumab × nausea
            metformin × conjunctivitis
            metformin × nausea
        """
        cache = make_rxnorm_cache(spark, [
            ("DUPILUMAB", "1876381", "dupilumab"),
            ("METFORMIN",  "6809",   "metformin"),
        ])
        drug_raw = make_drug_df(spark, [
            ("6001", "DUPIXENT",  "DUPILUMAB", "PS", "2023Q1"),
            ("6001", "GLUCOPHAGE","METFORMIN",  "PS", "2023Q1"),
        ])
        drug_df = filter_and_normalize_drug(drug_raw, cache, spark)

        reac_df = dedup_reac(
            spark.createDataFrame([
                ("6001", "conjunctivitis"),
                ("6001", "nausea"),
            ], ["primaryid", "pt"])
        )

        pairs = (
            drug_df
            .join(reac_df, on="primaryid", how="inner")
            .dropDuplicates(["primaryid", "drug_key", "pt"])
        )

        assert pairs.count() == 4, (
            f"Expected 4 drug-reaction pairs, got {pairs.count()}"
        )

        pair_set = {(r["drug_key"], r["pt"]) for r in pairs.collect()}
        assert ("dupilumab", "conjunctivitis") in pair_set
        assert ("dupilumab", "nausea")         in pair_set
        assert ("metformin", "conjunctivitis") in pair_set
        assert ("metformin", "nausea")         in pair_set


# ══════════════════════════════════════════════════════════════════════════════
# Integration Test 8 — Multi-quarter data, single quarter filter
# ══════════════════════════════════════════════════════════════════════════════

class TestMultiQuarterFilter:
    """
    When Kafka contains data from Q1 and Q2, running with --quarter 2023Q1
    must produce only Q1 drug-reaction pairs in the output.

    This is the most critical integration test for the quarter argument —
    mixing quarters would inflate PRR numerators and produce false signals.
    """

    def test_int_8_quarter_filter_isolates_single_quarter(self, spark):
        """
        Drug records from both Q1 and Q2 in Kafka.
        Running with quarter="2023Q1" must include only Q1 cases.
        Q2 case must not appear in the output.
        """
        import json as json_mod
        from pyspark.sql.types import StructType, StructField, StringType
        from pipelines.spark_branch1 import parse_drug, parse_reac

        raw_schema = StructType([StructField("value", StringType(), True)])

        # Drug records — two quarters
        drug_rows = [
            {"primaryid": "7001", "caseid": "700", "role_cod": "PS",
             "prod_ai": "metformin", "drugname": "metformin",
             "source_quarter": "2023Q1"},
            {"primaryid": "7002", "caseid": "800", "role_cod": "PS",
             "prod_ai": "metformin", "drugname": "metformin",
             "source_quarter": "2023Q2"},  # must be excluded
        ]
        raw_drug = spark.createDataFrame(
            [(json_mod.dumps(r),) for r in drug_rows], raw_schema
        )
        drug_df = parse_drug(spark, raw_drug, quarter="2023Q1")
        drug_df = filter_and_normalize_drug(
            drug_df, make_rxnorm_cache(spark), spark
        )

        # Reaction records — both quarters
        reac_rows = [
            {"primaryid": "7001", "pt": "lactic acidosis",
             "source_quarter": "2023Q1"},
            {"primaryid": "7002", "pt": "lactic acidosis",
             "source_quarter": "2023Q2"},
        ]
        raw_reac = spark.createDataFrame(
            [(json_mod.dumps(r),) for r in reac_rows], raw_schema
        )
        reac_df = parse_reac(spark, raw_reac, quarter="2023Q1")
        reac_df = dedup_reac(reac_df)

        pairs = (
            drug_df
            .join(reac_df, on="primaryid", how="inner")
            .dropDuplicates(["primaryid", "drug_key", "pt"])
        )

        assert pairs.count() == 1, (
            f"Quarter filter must exclude Q2 data. Expected 1 row, got {pairs.count()}"
        )
        row = pairs.collect()[0]
        assert str(row["primaryid"]) == "7001", (
            f"Only Q1 primaryid=7001 should appear, got {row['primaryid']}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# Integration Test 9 — PRR readiness: drug_key and pt are non-null and lowercase
# ══════════════════════════════════════════════════════════════════════════════

class TestOutputReadyForPRR:
    """
    Branch 2 PRR computation requires:
        - drug_key is non-null, lowercase, trimmed
        - pt is non-null, lowercase, trimmed
        - No duplicate (primaryid, drug_key, pt) triples

    These properties must hold after the full Branch 1 pipeline.
    """

    def test_int_9_output_drug_keys_lowercase_and_non_null(self, spark):
        """
        All drug_key values in final output must be lowercase and non-null.
        Branch 2 groups by drug_key — uppercase variants would fragment counts.
        """
        cache = make_rxnorm_cache(spark, [
            ("EMPAGLIFLOZIN", "2200644", "empagliflozin"),
        ])
        drug_raw = make_drug_df(spark, [
            ("8001", "JARDIANCE", "EMPAGLIFLOZIN", "PS", "2023Q1"),
            ("8002", "JARDIANCE", "EMPAGLIFLOZIN", "PS", "2023Q1"),
        ])
        drug_df = filter_and_normalize_drug(drug_raw, cache, spark)

        reac_df = dedup_reac(
            spark.createDataFrame([
                ("8001", "diabetic ketoacidosis"),
                ("8002", "DIABETIC KETOACIDOSIS"),  # uppercase — must be lowercased
            ], ["primaryid", "pt"])
        )

        pairs = (
            drug_df
            .join(reac_df, on="primaryid", how="inner")
            .dropDuplicates(["primaryid", "drug_key", "pt"])
        )

        rows = pairs.collect()
        for row in rows:
            assert row["drug_key"] is not None,  "drug_key must not be null"
            assert row["pt"]       is not None,  "pt must not be null"
            assert row["drug_key"] == row["drug_key"].lower(), (
                f"drug_key must be lowercase: {row['drug_key']}"
            )
            assert row["pt"] == row["pt"].lower(), (
                f"pt must be lowercase: {row['pt']}"
            )

    def test_int_10_no_duplicate_pairs_in_output(self, spark):
        """
        Final output must have no duplicate (primaryid, drug_key, pt) triples.
        Duplicates would inflate the PRR numerator A.
        """
        cache    = make_rxnorm_cache(spark)
        drug_raw = make_drug_df(spark, [
            ("9001", "metformin", "metformin", "PS", "2023Q1"),
            ("9001", "metformin", "metformin", "PS", "2023Q1"),  # exact duplicate
        ])
        drug_df = filter_and_normalize_drug(drug_raw, cache, spark)

        reac_df = dedup_reac(
            spark.createDataFrame([
                ("9001", "lactic acidosis"),
                ("9001", "lactic acidosis"),  # exact duplicate
            ], ["primaryid", "pt"])
        )

        pairs = (
            drug_df
            .join(reac_df, on="primaryid", how="inner")
            .dropDuplicates(["primaryid", "drug_key", "pt"])
        )

        assert pairs.count() == 1, (
            f"Duplicate pairs must be collapsed to 1 row, got {pairs.count()}"
        )


# Runner 

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])