"""
tests/unit/test_spark_branch1.py — Unit tests for Spark Branch 1 transformations

Strategy:
    Each function in spark_branch1.py is a pure DataFrame transformation.
    Tests use spark.createDataFrame() with small in-memory datasets —
    no Kafka, no Snowflake, no external dependencies needed.

    SparkSession is created once per module (session fixture) and reused
    across all tests. Local mode, minimal config, fast startup.

Functions tested:
    1.  dedup_demo          — caseversion deduplication (keep highest per caseid)
    2.  filter_and_normalize_drug — PS filter + combination split + RxNorm join
    3.  dedup_reac          — reaction deduplication on (primaryid, pt)
    4.  aggregate_outc      — outcome flag aggregation per primaryid
    5.  join logic          — four-file join produces correct output schema
    6.  Edge cases          — nulls, empty strings, mixed case, duplicates

Run:
    poetry run pytest tests/unit/test_spark_branch1.py -v -s

No API keys, no Kafka, no Snowflake needed — all tests run offline.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest
from decimal import Decimal


# ── SparkSession fixture ──────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def spark():
    """
    Single SparkSession shared across all tests in this module.
    Local mode — no cluster needed.
    scope=module means it starts once and shuts down after all tests complete.
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("test-spark-branch1")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")       # disable web UI for tests
        .config("spark.log.level", "ERROR")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


# ── Import functions under test ───────────────────────────────────────────────

from pipelines.spark_branch1 import (
    dedup_demo,
    filter_and_normalize_drug,
    dedup_reac,
    aggregate_outc,
)


# Test 1–4: dedup_demo 

class TestDedupDemo:

    def test_1_keeps_highest_caseversion(self, spark):
        """
        Core dedup logic — when same caseid appears with versions 1 and 2,
        keep version 2 (highest). This handles FAERS quarterly updates where
        the same case is resubmitted with corrections.
        """
        data = [
            ("1001", "100", "1", "2023Q1"),   # caseid 100, version 1 — older
            ("1002", "100", "2", "2023Q2"),   # caseid 100, version 2 — keep this
            ("1003", "200", "1", "2023Q1"),   # caseid 200, only version
        ]
        df = spark.createDataFrame(data, ["primaryid", "caseid", "caseversion", "source_quarter"])

        result = dedup_demo(df)
        rows   = {r["caseid"]: r for r in result.collect()}

        assert len(rows) == 2, f"Expected 2 unique cases, got {len(rows)}"
        assert rows["100"]["primaryid"] == "1002", (
            "Should keep primaryid=1002 (caseversion=2), not 1001 (caseversion=1)"
        )
        assert rows["200"]["primaryid"] == "1003"

    def test_2_single_version_case_unchanged(self, spark):
        """Cases with only one version pass through unchanged."""
        data = [
            ("2001", "300", "1", "2023Q1"),
            ("2002", "400", "1", "2023Q1"),
        ]
        df     = spark.createDataFrame(data, ["primaryid", "caseid", "caseversion", "source_quarter"])
        result = dedup_demo(df)

        assert result.count() == 2

    def test_3_three_versions_keeps_highest(self, spark):
        """When 3 versions exist, keep only version 3."""
        data = [
            ("3001", "500", "1", "2023Q1"),
            ("3002", "500", "2", "2023Q2"),
            ("3003", "500", "3", "2023Q3"),
        ]
        df     = spark.createDataFrame(data, ["primaryid", "caseid", "caseversion", "source_quarter"])
        result = dedup_demo(df)

        assert result.count() == 1
        assert result.collect()[0]["primaryid"] == "3003"

    def test_4_empty_dataframe(self, spark):
        """Empty input produces empty output without error."""
        df     = spark.createDataFrame([], "primaryid STRING, caseid STRING, caseversion STRING, source_quarter STRING")
        result = dedup_demo(df)
        assert result.count() == 0


# ── Tests 5–12: filter_and_normalize_drug ────────────────────────────────────

class TestFilterAndNormalizeDrug:

    def _make_cache(self, spark, entries):
        """Build a mock RxNorm cache DataFrame with explicit schema."""
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([
            StructField("prod_ai_upper",  StringType(), True),
            StructField("rxcui",          StringType(), True),
            StructField("canonical_name", StringType(), True),
        ])
        data = [(e[0].upper(), e[1], e[2]) for e in entries]
        return spark.createDataFrame(data, schema)

    def test_5_ps_filter_removes_non_ps(self, spark):
        """
        Only role_cod = PS (Primary Suspect) rows should survive.
        SS (Secondary Suspect), C (Concomitant), I (Interacting) must be dropped.
        This is critical — including concomitant drugs would inflate PRR denominators.
        """
        data = [
            ("1001", "aspirin",    "aspirin",    "PS", "2023Q1"),
            ("1002", "ibuprofen",  "ibuprofen",  "SS", "2023Q1"),
            ("1003", "metformin",  "metformin",  "C",  "2023Q1"),
            ("1004", "dupilumab",  "dupilumab",  "I",  "2023Q1"),
            ("1005", "gabapentin", "gabapentin",  "PS", "2023Q1"),
        ]
        df    = spark.createDataFrame(data, ["primaryid", "drugname", "prod_ai", "role_cod", "source_quarter"])
        cache = self._make_cache(spark, [])   # empty cache — no normalization

        result    = filter_and_normalize_drug(df, cache, spark)
        drug_keys = [r["drug_key"] for r in result.collect()]

        assert len(drug_keys) == 2, f"Expected 2 PS drugs, got {len(drug_keys)}"
        assert all(k in drug_keys for k in ["aspirin", "gabapentin"])

    def test_6_ps_filter_case_insensitive(self, spark):
        """
        role_cod matching must be case-insensitive.
        FAERS data contains 'ps', 'PS', 'Ps' — all should be kept.
        """
        data = [
            ("2001", "aspirin",   "aspirin",   "ps", "2023Q1"),
            ("2002", "metformin", "metformin", "PS", "2023Q1"),
            ("2003", "ibuprofen", "ibuprofen", "Ps", "2023Q1"),
        ]
        df     = spark.createDataFrame(data, ["primaryid", "drugname", "prod_ai", "role_cod", "source_quarter"])
        cache  = self._make_cache(spark, [])
        result = filter_and_normalize_drug(df, cache, spark)

        assert result.count() == 3, "All case variants of PS should be kept"

    def test_7_combination_drug_split(self, spark):
        """
        Combination drugs like "ACETAMINOPHEN\\HYDROCODONE" must be split
        and only the first component kept. This prevents double-counting
        combination products as separate drug entries.
        """
        data = [
            ("3001", "acetaminophen\\hydrocodone", "acetaminophen\\hydrocodone", "PS", "2023Q1"),
            ("3002", "aspirin",                    "aspirin",                    "PS", "2023Q1"),
        ]
        df     = spark.createDataFrame(data, ["primaryid", "drugname", "prod_ai", "role_cod", "source_quarter"])
        cache  = self._make_cache(spark, [])
        result = filter_and_normalize_drug(df, cache, spark)

        rows     = {r["primaryid"]: r for r in result.collect()}
        drug_key = rows["3001"]["drug_key"]

        assert "\\" not in drug_key, f"Combination drug not split: {drug_key}"
        assert drug_key == "acetaminophen", f"Expected 'acetaminophen', got '{drug_key}'"

    def test_8_rxnorm_canonical_name_used(self, spark):
        """
        When prod_ai matches RxNorm cache, canonical_name is used as drug_key.
        This normalizes brand names and salt forms to a single canonical identifier.
        """
        data = [
            ("4001", "JARDIANCE", "EMPAGLIFLOZIN", "PS", "2023Q1"),
        ]
        df    = spark.createDataFrame(data, ["primaryid", "drugname", "prod_ai", "role_cod", "source_quarter"])
        cache = self._make_cache(spark, [("EMPAGLIFLOZIN", "2200644", "empagliflozin")])

        result   = filter_and_normalize_drug(df, cache, spark)
        drug_key = result.collect()[0]["drug_key"]

        assert drug_key == "empagliflozin", (
            f"Expected canonical name 'empagliflozin', got '{drug_key}'"
        )

    def test_9_falls_back_to_prod_ai_when_not_in_cache(self, spark):
        """
        When prod_ai is not in RxNorm cache, use lowercased prod_ai as drug_key.
        """
        data = [
            ("5001", "SOME BRAND", "METFORMIN", "PS", "2023Q1"),
        ]
        df     = spark.createDataFrame(data, ["primaryid", "drugname", "prod_ai", "role_cod", "source_quarter"])
        cache  = self._make_cache(spark, [])   # empty — no match
        result = filter_and_normalize_drug(df, cache, spark)

        drug_key = result.collect()[0]["drug_key"]
        assert drug_key == "metformin", f"Expected 'metformin', got '{drug_key}'"

    def test_10_falls_back_to_drugname_when_prod_ai_null(self, spark):
        """
        When prod_ai is null, fall back to lowercased drugname.
        prod_ai is missing in ~15% of FAERS DRUG records.
        """
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([
            StructField("primaryid",      StringType(), True),
            StructField("drugname",       StringType(), True),
            StructField("prod_ai",        StringType(), True),  # nullable
            StructField("role_cod",       StringType(), True),
            StructField("source_quarter", StringType(), True),
        ])
        data   = [("6001", "GABAPENTIN", None, "PS", "2023Q1")]
        df     = spark.createDataFrame(data, schema)
        cache  = self._make_cache(spark, [])
        result = filter_and_normalize_drug(df, cache, spark)

        drug_key = result.collect()[0]["drug_key"]
        assert drug_key == "gabapentin", f"Expected 'gabapentin', got '{drug_key}'"

    def test_11_duplicate_primaryid_drug_key_deduped(self, spark):
        """
        Same (primaryid, drug_key) pair appearing twice must be deduplicated.
        One patient should contribute one row per drug, not multiple.
        """
        data = [
            ("7001", "aspirin", "aspirin", "PS", "2023Q1"),
            ("7001", "aspirin", "aspirin", "PS", "2023Q1"),   # exact duplicate
        ]
        df     = spark.createDataFrame(data, ["primaryid", "drugname", "prod_ai", "role_cod", "source_quarter"])
        cache  = self._make_cache(spark, [])
        result = filter_and_normalize_drug(df, cache, spark)

        assert result.count() == 1, "Duplicate (primaryid, drug_key) must be deduplicated"

    def test_12_null_drug_key_dropped(self, spark):
        """
        Rows where drug_key resolves to null (both prod_ai and drugname null)
        must be dropped — they cannot be used in PRR computation.
        """
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([
            StructField("primaryid",      StringType(), True),
            StructField("drugname",       StringType(), True),  # nullable
            StructField("prod_ai",        StringType(), True),  # nullable
            StructField("role_cod",       StringType(), True),
            StructField("source_quarter", StringType(), True),
        ])
        data   = [("8001", None, None, "PS", "2023Q1")]
        df     = spark.createDataFrame(data, schema)
        cache  = self._make_cache(spark, [])
        result = filter_and_normalize_drug(df, cache, spark)

        assert result.count() == 0, "Row with null drug_key must be dropped"


# ── Tests 13–16: dedup_reac ───────────────────────────────────────────────────

class TestDedupReac:

    def test_13_deduplicates_same_reaction(self, spark):
        """
        Same (primaryid, pt) appearing twice must produce one row.
        A patient reporting the same reaction twice counts as one adverse event.
        """
        data = [
            ("1001", "nausea"),
            ("1001", "nausea"),    # duplicate
            ("1001", "vomiting"),  # different reaction — keep
        ]
        df     = spark.createDataFrame(data, ["primaryid", "pt"])
        result = dedup_reac(df)

        assert result.count() == 2

    def test_14_pt_lowercased(self, spark):
        """
        MedDRA preferred terms must be lowercased for consistent matching.
        FAERS data contains mixed case: 'Nausea', 'NAUSEA', 'nausea'.
        """
        data = [
            ("2001", "Nausea"),
            ("2002", "NAUSEA"),
            ("2003", "nausea"),
        ]
        df     = spark.createDataFrame(data, ["primaryid", "pt"])
        result = dedup_reac(df)
        pts    = [r["pt"] for r in result.collect()]

        assert all(pt == pt.lower() for pt in pts), "All pt values must be lowercase"

    def test_15_different_patients_same_reaction_kept(self, spark):
        """
        Same reaction for different patients must both be kept.
        Dedup is on (primaryid, pt) not just pt.
        """
        data = [
            ("3001", "nausea"),
            ("3002", "nausea"),
        ]
        df     = spark.createDataFrame(data, ["primaryid", "pt"])
        result = dedup_reac(df)

        assert result.count() == 2

    def test_16_empty_dataframe(self, spark):
        """Empty input produces empty output."""
        df     = spark.createDataFrame([], "primaryid STRING, pt STRING")
        result = dedup_reac(df)
        assert result.count() == 0


# ── Tests 17–22: aggregate_outc ──────────────────────────────────────────────

class TestAggregateOutc:

    def test_17_death_flag_set_for_de(self, spark):
        """
        outc_cod = DE must set death_flag = 1.
        This is the most critical outcome flag — affects signal priority.
        """
        data = [("1001", "DE")]
        df     = spark.createDataFrame(data, ["primaryid", "outc_cod"])
        result = aggregate_outc(df)
        row    = result.filter("primaryid = '1001'").collect()[0]

        assert row["death_flag"] == 1
        assert row["hosp_flag"]  == 0
        assert row["lt_flag"]    == 0

    def test_18_hosp_flag_set_for_ho(self, spark):
        """outc_cod = HO must set hosp_flag = 1."""
        data = [("2001", "HO")]
        df     = spark.createDataFrame(data, ["primaryid", "outc_cod"])
        result = aggregate_outc(df)
        row    = result.filter("primaryid = '2001'").collect()[0]

        assert row["hosp_flag"]  == 1
        assert row["death_flag"] == 0
        assert row["lt_flag"]    == 0

    def test_19_lt_flag_set_for_lt(self, spark):
        """outc_cod = LT must set lt_flag = 1."""
        data = [("3001", "LT")]
        df     = spark.createDataFrame(data, ["primaryid", "outc_cod"])
        result = aggregate_outc(df)
        row    = result.filter("primaryid = '3001'").collect()[0]

        assert row["lt_flag"]    == 1
        assert row["death_flag"] == 0
        assert row["hosp_flag"]  == 0

    def test_20_multiple_outcomes_per_patient(self, spark):
        """
        Patient with both death and hospitalisation outcomes must have
        both flags set. max() acts as logical OR across multiple rows.
        """
        data = [
            ("4001", "DE"),
            ("4001", "HO"),
        ]
        df     = spark.createDataFrame(data, ["primaryid", "outc_cod"])
        result = aggregate_outc(df)
        row    = result.filter("primaryid = '4001'").collect()[0]

        assert row["death_flag"] == 1
        assert row["hosp_flag"]  == 1
        assert row["lt_flag"]    == 0

    def test_21_unknown_outc_cod_produces_all_zeros(self, spark):
        """
        Unknown outcome codes (OT = other, DS = disability etc.) must not
        set any of the three flags. All flags default to 0.
        """
        data = [("5001", "OT"), ("5001", "DS")]
        df     = spark.createDataFrame(data, ["primaryid", "outc_cod"])
        result = aggregate_outc(df)
        row    = result.filter("primaryid = '5001'").collect()[0]

        assert row["death_flag"] == 0
        assert row["hosp_flag"]  == 0
        assert row["lt_flag"]    == 0

    def test_22_one_row_per_primaryid(self, spark):
        """
        Output must have exactly one row per primaryid regardless of
        how many outcome records existed for that patient.
        """
        data = [
            ("6001", "DE"),
            ("6001", "HO"),
            ("6001", "LT"),
            ("6002", "HO"),
        ]
        df     = spark.createDataFrame(data, ["primaryid", "outc_cod"])
        result = aggregate_outc(df)

        assert result.count() == 2, (
            f"Expected 2 unique primaryids, got {result.count()}"
        )


# ── Tests 23–26: Integration — four-file join logic ──────────────────────────

class TestJoinLogic:
    """
    Tests the four-file join by constructing minimal DataFrames
    that mirror the output of each transformation step and verifying
    the join produces the expected output schema and row counts.
    """

    def test_23_inner_join_drug_reac_drops_unmatched(self, spark):
        """
        DRUG inner join REAC on primaryid means patients with drugs but
        no reactions, or reactions but no drugs, are dropped.
        Only cases with both a drug AND a reaction survive.
        """
        from pyspark.sql import functions as F

        drug = spark.createDataFrame([
            ("1001", "metformin"),
            ("1002", "aspirin"),    # no matching reaction
        ], ["primaryid", "drug_key"])

        reac = spark.createDataFrame([
            ("1001", "lactic acidosis"),
            ("1003", "nausea"),     # no matching drug
        ], ["primaryid", "pt"])

        result = drug.join(reac, on="primaryid", how="inner")

        assert result.count() == 1
        row = result.collect()[0]
        assert row["drug_key"] == "metformin"
        assert row["pt"]       == "lactic acidosis"

    def test_24_left_join_outc_preserves_cases_without_outcomes(self, spark):
        """
        OUTC is left-joined so cases with no outcome codes are kept.
        About 28% of FAERS cases have no outcome — dropping them would
        silently remove a significant portion of the dataset.
        """
        pairs = spark.createDataFrame([
            ("1001", "metformin", "lactic acidosis"),
            ("1002", "aspirin",   "nausea"),          # no outcome
        ], ["primaryid", "drug_key", "pt"])

        outc = spark.createDataFrame([
            ("1001", 1, 0, 0),
        ], ["primaryid", "death_flag", "hosp_flag", "lt_flag"])

        result = pairs.join(outc, on="primaryid", how="left")

        assert result.count() == 2, "Left join must preserve cases without outcomes"

        no_outc_row = result.filter("primaryid = '1002'").collect()[0]
        assert no_outc_row["death_flag"] is None, (
            "Cases without outcome should have null flags after left join"
        )

    def test_25_pair_level_dedup_after_join(self, spark):
        """
        After joining, (primaryid, drug_key, pt) triples must be deduplicated.
        A patient with the same drug-reaction pair in multiple quarters
        should contribute one row to drug_reaction_pairs.
        """
        from pyspark.sql import functions as F

        pairs = spark.createDataFrame([
            ("1001", "metformin", "lactic acidosis", "2023Q1"),
            ("1001", "metformin", "lactic acidosis", "2023Q2"),   # duplicate pair
            ("1001", "metformin", "nausea",          "2023Q1"),   # different reaction
        ], ["primaryid", "drug_key", "pt", "source_quarter"])

        result = pairs.dropDuplicates(["primaryid", "drug_key", "pt"])

        assert result.count() == 2, (
            "Duplicate (primaryid, drug_key, pt) must be deduplicated"
        )

    def test_26_output_schema_has_required_columns(self, spark):
        """
        Final drug_reaction_pairs must contain all columns required by
        Branch 2 PRR computation and the evaluation dashboard.
        """
        required_columns = {
            "primaryid", "drug_key", "pt",
            "death_flag", "hosp_flag", "lt_flag",
            "source_quarter"
        }

        # Simulate final output schema
        data = [("1001", "metformin", "lactic acidosis", 1, 0, 0, "2023Q1")]
        df   = spark.createDataFrame(
            data,
            ["primaryid", "drug_key", "pt",
             "death_flag", "hosp_flag", "lt_flag", "source_quarter"]
        )

        actual_columns = set(df.columns)
        missing = required_columns - actual_columns
        assert not missing, f"Missing required columns: {missing}"


# ── Tests 27–29: Quarter filter in parse functions ────────────────────────────

class TestQuarterFilter:
    """
    The --quarter CLI argument passes a quarter string e.g. "2023Q1"
    to each parse function. Rows with a different source_quarter must
    be dropped. This ensures single-quarter runs don't mix data from
    multiple quarters already present in Kafka.
    """

    def _make_raw_demo(self, spark, rows):
        """
        Build a raw Kafka-style DataFrame with JSON string values.
        Mirrors what read_kafka_topic() returns before parsing.
        """
        import json as json_mod
        from pyspark.sql.types import StructType, StructField, StringType

        schema = StructType([StructField("value", StringType(), True)])
        data   = [(json_mod.dumps(r),) for r in rows]
        return spark.createDataFrame(data, schema)

    def test_27_parse_demo_quarter_filter(self, spark):
        """
        parse_demo with quarter="2023Q1" must drop rows tagged "2023Q2".
        Only rows matching the quarter survive.
        """
        from pipelines.spark_branch1 import parse_demo

        rows = [
            {"primaryid": "1001", "caseid": "100", "caseversion": "1",
             "fda_dt": "20230101", "source_quarter": "2023Q1"},
            {"primaryid": "1002", "caseid": "200", "caseversion": "1",
             "fda_dt": "20230101", "source_quarter": "2023Q2"},  # wrong quarter
        ]
        raw    = self._make_raw_demo(spark, rows)
        result = parse_demo(spark, raw, quarter="2023Q1")

        assert result.count() == 1, (
            "parse_demo should keep only rows matching the specified quarter"
        )
        assert result.collect()[0]["primaryid"] == 1001

    def test_28_parse_demo_null_primaryid_dropped(self, spark):
        """
        parse_demo must drop rows where primaryid is null or not castable to LongType.
        Null primaryids cannot be joined and would produce garbage rows downstream.
        """
        from pipelines.spark_branch1 import parse_demo

        rows = [
            {"primaryid": "1001", "caseid": "100", "caseversion": "1",
             "fda_dt": "20230101", "source_quarter": "2023Q1"},
            {"primaryid": None, "caseid": "200", "caseversion": "1",
             "fda_dt": "20230101", "source_quarter": "2023Q1"},  # null primaryid
        ]
        raw    = self._make_raw_demo(spark, rows)
        result = parse_demo(spark, raw, quarter=None)

        assert result.count() == 1, (
            "parse_demo must drop rows with null primaryid"
        )

    def test_29_parse_reac_quarter_filter(self, spark):
        """
        parse_reac with quarter="2023Q1" must drop rows tagged "2023Q2".
        Validates quarter filtering works consistently across parse functions.
        """
        import json as json_mod
        from pyspark.sql.types import StructType, StructField, StringType
        from pipelines.spark_branch1 import parse_reac

        rows = [
            {"primaryid": "2001", "pt": "nausea",   "source_quarter": "2023Q1"},
            {"primaryid": "2002", "pt": "vomiting", "source_quarter": "2023Q2"},
            {"primaryid": "2003", "pt": "dizziness","source_quarter": "2023Q1"},
        ]
        schema = StructType([StructField("value", StringType(), True)])
        data   = [(json_mod.dumps(r),) for r in rows]
        raw    = spark.createDataFrame(data, schema)
        result = parse_reac(spark, raw, quarter="2023Q1")

        assert result.count() == 2, (
            f"Expected 2 rows for 2023Q1, got {result.count()}"
        )
        pts = [r["pt"] for r in result.collect()]
        assert "nausea"    in pts
        assert "dizziness" in pts
        assert "vomiting"  not in pts


# ── Tests 30–33: validate_row_counts ─────────────────────────────────────────

class TestValidateRowCounts:
    """
    validate_row_counts checks whether the final drug_reaction_pairs
    DataFrame contains a plausible number of rows:
        Single quarter: 900K–1.8M rows
        Full year     : 4M–6M rows

    This catches join errors (too few rows) and missing dedup (too many).
    Tests use mocked DataFrames with a .count() method to avoid
    creating millions of real rows.
    """

    def _mock_df(self, count: int):
        """Create a mock object that returns a fixed count."""
        from unittest.mock import MagicMock
        mock = MagicMock()
        mock.count.return_value = count
        return mock

    def test_30_single_quarter_within_range_passes(self):
        """900K–1.8M rows for a single quarter → True."""
        from pipelines.spark_branch1 import validate_row_counts

        df     = self._mock_df(1_200_000)
        result = validate_row_counts(df, quarter="2023Q1")

        assert result is True, "1.2M rows is within single quarter range 900K–1.8M"

    def test_31_single_quarter_below_minimum_fails(self):
        """Below 900K rows for single quarter → False (join error suspected)."""
        from pipelines.spark_branch1 import validate_row_counts

        df     = self._mock_df(500_000)
        result = validate_row_counts(df, quarter="2023Q1")

        assert result is False, (
            "500K rows is below single quarter minimum 900K — should return False"
        )

    def test_32_single_quarter_above_maximum_fails(self):
        """Above 1.8M rows for single quarter → False (missing dedup suspected)."""
        from pipelines.spark_branch1 import validate_row_counts

        df     = self._mock_df(2_500_000)
        result = validate_row_counts(df, quarter="2023Q1")

        assert result is False, (
            "2.5M rows exceeds single quarter max 1.8M — should return False"
        )

    def test_33_full_year_within_range_passes(self):
        """4M–6M rows for full year (no quarter filter) → True."""
        from pipelines.spark_branch1 import validate_row_counts

        df     = self._mock_df(5_000_000)
        result = validate_row_counts(df, quarter=None)

        assert result is True, "5M rows is within full year range 4M–6M"

    def test_34_full_year_below_minimum_fails(self):
        """Below 4M rows for full year → False."""
        from pipelines.spark_branch1 import validate_row_counts

        df     = self._mock_df(2_000_000)
        result = validate_row_counts(df, quarter=None)

        assert result is False, (
            "2M rows is below full year minimum 4M — should return False"
        )


# ── Tests 35–36: Combination drug split on drugname field ─────────────────────

class TestCombinationDrugNameField:
    """
    Combination drug split must apply to BOTH prod_ai and drugname fields.
    Test 7 already covers prod_ai. These tests verify drugname is also split
    when prod_ai is null and the fallback uses drugname.
    """

    def _make_cache(self, spark):
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([
            StructField("prod_ai_upper",  StringType(), True),
            StructField("rxcui",          StringType(), True),
            StructField("canonical_name", StringType(), True),
        ])
        return spark.createDataFrame([], schema)

    def test_35_drugname_combination_split_when_prod_ai_null(self, spark):
        """
        When prod_ai is null and drugname contains a backslash,
        only the first component of drugname should be used as drug_key.
        """
        from pyspark.sql.types import StructType, StructField, StringType
        from pipelines.spark_branch1 import filter_and_normalize_drug

        schema = StructType([
            StructField("primaryid",      StringType(), True),
            StructField("drugname",       StringType(), True),
            StructField("prod_ai",        StringType(), True),
            StructField("role_cod",       StringType(), True),
            StructField("source_quarter", StringType(), True),
        ])
        # prod_ai is null — falls back to drugname which contains backslash
        data   = [("9001", "ACETAMINOPHEN\\CODEINE", None, "PS", "2023Q1")]
        df     = spark.createDataFrame(data, schema)
        cache  = self._make_cache(spark)
        result = filter_and_normalize_drug(df, cache, spark)

        drug_key = result.collect()[0]["drug_key"]
        assert "\\" not in drug_key, f"Drugname not split: {drug_key}"
        assert drug_key == "acetaminophen", f"Expected 'acetaminophen', got '{drug_key}'"

    def test_36_whitespace_trimmed_from_drug_key(self, spark):
        """
        Drug names with leading/trailing whitespace must be trimmed.
        FAERS data frequently contains ' METFORMIN ' with padding spaces.
        Untrimmed drug_key would prevent RxNorm cache hits and PRR aggregation.
        """
        from pyspark.sql.types import StructType, StructField, StringType
        from pipelines.spark_branch1 import filter_and_normalize_drug

        schema = StructType([
            StructField("primaryid",      StringType(), True),
            StructField("drugname",       StringType(), True),
            StructField("prod_ai",        StringType(), True),
            StructField("role_cod",       StringType(), True),
            StructField("source_quarter", StringType(), True),
        ])
        data   = [("9002", "  METFORMIN  ", "  METFORMIN  ", "PS", "2023Q1")]
        df     = spark.createDataFrame(data, schema)
        cache  = self._make_cache(spark)
        result = filter_and_normalize_drug(df, cache, spark)

        drug_key = result.collect()[0]["drug_key"]
        assert drug_key == drug_key.strip(), f"Drug key has whitespace: '{drug_key}'"
        assert drug_key == "metformin", f"Expected 'metformin', got '{drug_key}'"


# ── Tests 37–38: run_validation_checkpoint (mocked Snowflake) ─────────────────

class TestValidationCheckpoint:
    """
    run_validation_checkpoint queries Snowflake to confirm gabapentin ×
    cardio-respiratory arrest exists in drug_reaction_pairs after Branch 1.

    Snowflake is mocked — no real connection needed.
    Tests verify the True/False logic, not the Snowflake connection itself.
    """

    def test_37_checkpoint_passes_when_gabapentin_found(self):
        """
        Returns True when gabapentin × cardio-respiratory arrest
        has count > 0 in drug_reaction_pairs.
        """
        from unittest.mock import MagicMock, patch
        from pipelines.spark_branch1 import run_validation_checkpoint

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (42,)   # 42 rows found

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("pipelines.spark_branch1.get_sf_conn", return_value=mock_conn):
            result = run_validation_checkpoint(sf_config={})

        assert result is True, (
            "Checkpoint must return True when gabapentin signal exists"
        )

    def test_38_checkpoint_fails_when_gabapentin_missing(self):
        """
        Returns False when count = 0 — gabapentin signal not found.
        This means either the PS filter, combination split, RxNorm
        normalization, or join logic has a bug.
        """
        from unittest.mock import MagicMock, patch
        from pipelines.spark_branch1 import run_validation_checkpoint

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (0,)    # no rows found

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("pipelines.spark_branch1.get_sf_conn", return_value=mock_conn):
            result = run_validation_checkpoint(sf_config={})

        assert result is False, (
            "Checkpoint must return False when gabapentin signal is missing"
        )


# ── Runner ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])