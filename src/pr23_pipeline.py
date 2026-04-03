"""
PR-23: End-to-End Pipeline with Tests — Pipeline (ETL core)

Pattern: Template Method
  run() = extract() → validate() → transform() → load()

Each stage is its own method so it can be unit-tested independently
and swapped without touching the orchestration logic.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from pr23_config import AppConfig
from pr23_transformers import (
    clean_phone_numbers,
    add_rating_category,
    normalize_cuisine,
    calculate_order_metrics,
)
from pr23_validators import validate_no_nulls, validate_row_count, validate_schema


class UberEatsPipeline:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    # ── Extract ────────────────────────────────────────────────────────────────

    def extract(self):
        """Read seed data (in-memory for practice; swap to real sources in prod)."""
        restaurants_df = self.spark.createDataFrame([
            (1, "Pizza Palace", "Italian",  "Sao Paulo", 4.5, 120),
            (2, "Burger Boss",  "American", "Sao Paulo", 3.8, 200),
            (3, "Sushi Star",   "Japanese", "Rio",       4.9,  80),
            (4, "Taco Town",    "Mexican",  "Rio",       4.1, 150),
            (5, "Pasta Place",  "Italian",  "Curitiba",  4.7,  90),
            (6, "BBQ Barn",     "American", "Curitiba",  3.5, 300),
            (7, "Ramen Road",   "Japanese", "Sao Paulo", 4.3, 110),
            (8, "Curry Club",   "Indian",   "Rio",       4.6,  70),
        ], ["restaurant_id", "name", "cuisine_type", "city", "average_rating", "monthly_orders"])

        users_df = self.spark.createDataFrame([
            (1, "ana@email.com",    "(51) 4463-9821"),
            (2, "bob@email.com",    "(11) 9999-1234"),
            (3, "carol@email.com",  "(21) 8888-5678"),
        ], ["user_id", "email", "phone_number"])

        orders_df = self.spark.createDataFrame([
            (1,  1, 45.90), (2,  1, 89.50), (3,  2, 32.00),
            (4,  2, 67.80), (5,  3, 55.00), (6,  3, 92.30),
            (7,  4, 41.20), (8,  5, 78.60), (9,  6, 33.40),
            (10, 7, 95.10), (11, 8, 62.70), (12, 1, 88.00),
        ], ["order_id", "restaurant_id", "amount"])

        return users_df, restaurants_df, orders_df

    # ── Validate ───────────────────────────────────────────────────────────────

    def validate(self, users_df, restaurants_df, orders_df):
        """Fail-fast checks before spending compute on bad data."""
        assert validate_schema(users_df, AppConfig.CRITICAL_COLUMNS_USERS), \
            "Users schema invalid"
        assert validate_schema(restaurants_df, AppConfig.CRITICAL_COLUMNS_RESTAURANTS), \
            "Restaurants schema invalid"
        assert validate_row_count(restaurants_df, AppConfig.MIN_ROW_COUNT), \
            "Restaurants table is empty"
        assert validate_no_nulls(users_df, ["user_id", "email"]), \
            "Nulls found in users critical columns"

    # ── Transform ──────────────────────────────────────────────────────────────

    def transform(self, users_df, restaurants_df, orders_df):
        users_df        = clean_phone_numbers(users_df, "phone_number")
        restaurants_df  = normalize_cuisine(restaurants_df)
        restaurants_df  = add_rating_category(restaurants_df)
        order_metrics   = calculate_order_metrics(orders_df, restaurants_df)
        return users_df, restaurants_df, order_metrics

    # ── Load ───────────────────────────────────────────────────────────────────

    def load(self, users_df, restaurants_df, order_metrics):
        """Print results to console (swap write.parquet() for production)."""
        print("\n=== Users (cleaned phones) ===")
        users_df.show(truncate=False)

        print("\n=== Restaurants (normalized + rating categories) ===")
        restaurants_df.select(
            "restaurant_id", "name", "cuisine_type", "city",
            "average_rating", "rating_category",
        ).show(truncate=False)

        print("\n=== Order Metrics by Cuisine + City ===")
        order_metrics.show(truncate=False)

    # ── Orchestrate ────────────────────────────────────────────────────────────

    def run(self):
        print("\n--- Extract ---")
        users_df, restaurants_df, orders_df = self.extract()
        print(f"Loaded: {users_df.count()} users, "
              f"{restaurants_df.count()} restaurants, "
              f"{orders_df.count()} orders")

        print("\n--- Validate ---")
        self.validate(users_df, restaurants_df, orders_df)
        print("Validation passed.")

        print("\n--- Transform ---")
        users_df, restaurants_df, order_metrics = self.transform(
            users_df, restaurants_df, orders_df)

        print("\n--- Load ---")
        self.load(users_df, restaurants_df, order_metrics)

        print("\n=== Pipeline complete! ===")
