"""
PR-23: Unit tests for the UberEats pipeline components.
Practiced on: 2026-04-03

Run (VPS):
  cd /tmp/pr23
  python3 -m venv venv && venv/bin/pip install pytest pyspark
  venv/bin/pytest tests/test_pipeline.py -v

Key lessons baked in:
  - scope="session": one SparkSession shared across ALL tests (startup cost ~5s, not per-test)
  - getOrCreate(): safe to call many times, won't duplicate the session
  - yield + stop(): fixture teardown — always stop after all tests finish
  - IEEE 754 float trap: NEVER filter FloatType with exact ==; use < or > instead
    e.g. average_rating == 3.8 → 0 rows (stored as 3.7999999...)
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Add src to path so imports work from tests/
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pr23_transformers import (
    clean_phone_numbers,
    add_rating_category,
    normalize_cuisine,
    calculate_order_metrics,
)
from pr23_validators import validate_no_nulls, validate_row_count, validate_schema


# ── Shared SparkSession fixture ────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """
    One SparkSession for the entire test suite.
    scope="session" = created once, reused across all test files/classes.
    local[1] = single thread, no cluster needed.
    """
    session = SparkSession.builder \
        .appName("PR23-Tests") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    yield session
    session.stop()


# ── Test data fixtures ─────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def restaurants_df(spark):
    return spark.createDataFrame([
        (1, "Pizza Palace", "Italian",  "Sao Paulo", 4.5, 120),
        (2, "Burger Boss",  "American", "Sao Paulo", 3.8, 200),
        (3, "Sushi Star",   "Japanese", "Rio",       4.9,  80),
        (4, "Taco Town",    "Mexican",  "Rio",       4.1, 150),
        (5, "Pasta Place",  "Italian",  "Curitiba",  4.7,  90),
        (6, "BBQ Barn",     "American", "Curitiba",  3.5, 300),
    ], ["restaurant_id", "name", "cuisine_type", "city", "average_rating", "monthly_orders"])


@pytest.fixture(scope="session")
def orders_df(spark):
    return spark.createDataFrame([
        (1, 1, 45.90), (2, 1, 89.50), (3, 2, 32.00),
        (4, 2, 67.80), (5, 3, 55.00), (6, 3, 92.30),
    ], ["order_id", "restaurant_id", "amount"])


# ── clean_phone_numbers ────────────────────────────────────────────────────────

class TestCleanPhoneNumbers:
    def test_removes_parens_and_hyphen(self, spark):
        df = spark.createDataFrame([(1, "(51) 4463-9821")], ["id", "phone"])
        result = clean_phone_numbers(df, "phone").collect()[0].phone
        assert result == "51 44639821"

    def test_removes_plus_sign(self, spark):
        df = spark.createDataFrame([(1, "+55 11 9999-1234")], ["id", "phone"])
        result = clean_phone_numbers(df, "phone").collect()[0].phone
        assert result == "55 11 99991234"

    def test_plain_number_unchanged(self, spark):
        df = spark.createDataFrame([(1, "51999991234")], ["id", "phone"])
        result = clean_phone_numbers(df, "phone").collect()[0].phone
        assert result == "51999991234"

    def test_null_stays_null(self, spark):
        df = spark.createDataFrame([(1, None)], ["id", "phone"])
        result = clean_phone_numbers(df, "phone").collect()[0].phone
        assert result is None


# ── add_rating_category ────────────────────────────────────────────────────────

class TestAddRatingCategory:
    def test_excellent_at_4_5(self, spark):
        df = spark.createDataFrame([(1, 4.5)], ["id", "average_rating"])
        result = add_rating_category(df).collect()[0].rating_category
        assert result == "Excellent"

    def test_good_at_4_0(self, spark):
        df = spark.createDataFrame([(1, 4.0)], ["id", "average_rating"])
        result = add_rating_category(df).collect()[0].rating_category
        assert result == "Good"

    def test_average_below_4_0(self, spark):
        # ⚠️ IEEE 754: never filter FloatType with == 3.8 (stored as 3.7999...)
        # Use < 4.0 instead — this is the correct pattern
        df = spark.createDataFrame([(1, 3.8), (2, 3.5)], ["id", "average_rating"])
        result = add_rating_category(df).filter(F.col("average_rating") < 4.0)
        categories = [r.rating_category for r in result.collect()]
        assert all(c == "Average" for c in categories)

    def test_boundary_4_5_is_excellent_not_good(self, spark):
        df = spark.createDataFrame([(1, 4.5)], ["id", "average_rating"])
        result = add_rating_category(df).collect()[0].rating_category
        assert result != "Good"


# ── normalize_cuisine ──────────────────────────────────────────────────────────

class TestNormalizeCuisine:
    def test_lowercase_becomes_uppercase(self, spark):
        df = spark.createDataFrame([(1, "italian")], ["id", "cuisine_type"])
        result = normalize_cuisine(df).collect()[0].cuisine_type
        assert result == "ITALIAN"

    def test_strips_whitespace(self, spark):
        df = spark.createDataFrame([(1, "  japanese  ")], ["id", "cuisine_type"])
        result = normalize_cuisine(df).collect()[0].cuisine_type
        assert result == "JAPANESE"

    def test_null_becomes_unknown(self, spark):
        df = spark.createDataFrame([(1, None)], ["id", "cuisine_type"])
        result = normalize_cuisine(df).collect()[0].cuisine_type
        assert result == "UNKNOWN"

    def test_mixed_case_normalized(self, spark):
        df = spark.createDataFrame([(1, "Mexican")], ["id", "cuisine_type"])
        result = normalize_cuisine(df).collect()[0].cuisine_type
        assert result == "MEXICAN"


# ── validators ─────────────────────────────────────────────────────────────────

class TestValidators:
    def test_validate_no_nulls_passes(self, spark):
        df = spark.createDataFrame([(1, "test"), (2, "data")], ["id", "name"])
        assert validate_no_nulls(df, ["name"]) is True

    def test_validate_no_nulls_fails(self, spark):
        df = spark.createDataFrame([(1, "test"), (2, None)], ["id", "name"])
        assert validate_no_nulls(df, ["name"]) is False

    def test_validate_row_count_passes(self, spark):
        df = spark.createDataFrame([(1,), (2,)], ["id"])
        assert validate_row_count(df, min_rows=2) is True

    def test_validate_row_count_fails(self, spark):
        df = spark.createDataFrame([(1,)], ["id"])
        assert validate_row_count(df, min_rows=5) is False

    def test_validate_schema_passes(self, spark):
        df = spark.createDataFrame([(1, "a")], ["id", "name"])
        assert validate_schema(df, ["id", "name"]) is True

    def test_validate_schema_fails_missing_col(self, spark):
        df = spark.createDataFrame([(1,)], ["id"])
        assert validate_schema(df, ["id", "email"]) is False


# ── calculate_order_metrics integration ───────────────────────────────────────

class TestOrderMetrics:
    def test_returns_rows(self, orders_df, restaurants_df):
        result = calculate_order_metrics(orders_df, restaurants_df)
        assert result.count() > 0

    def test_has_expected_columns(self, orders_df, restaurants_df):
        result = calculate_order_metrics(orders_df, restaurants_df)
        assert "total_revenue" in result.columns
        assert "total_orders" in result.columns
        assert "avg_order_value" in result.columns
