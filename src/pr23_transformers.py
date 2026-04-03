"""
PR-23: End-to-End Pipeline with Tests — Transformers
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_phone_numbers(df: DataFrame, col_name: str) -> DataFrame:
    """Remove all non-digit characters except spaces from phone number column."""
    return df.withColumn(col_name,
        F.regexp_replace(F.col(col_name), r"[()\-+]", ""))


def add_rating_category(df: DataFrame) -> DataFrame:
    """Classify average_rating into Excellent / Good / Average."""
    return df.withColumn("rating_category",
        F.when(F.col("average_rating") >= 4.5, "Excellent")
         .when(F.col("average_rating") >= 4.0, "Good")
         .otherwise("Average"))


def normalize_cuisine(df: DataFrame) -> DataFrame:
    """Uppercase + strip cuisine_type; replace NULL with UNKNOWN."""
    return df.withColumn("cuisine_type",
        F.upper(F.trim(F.col("cuisine_type")))
    ).fillna({"cuisine_type": "UNKNOWN"})


def calculate_order_metrics(orders_df: DataFrame, restaurants_df: DataFrame) -> DataFrame:
    """
    Join orders with restaurants (broadcast small side) and aggregate
    total revenue + order count by cuisine and city.
    """
    joined = orders_df.join(F.broadcast(restaurants_df), on="restaurant_id")
    return joined.groupBy("cuisine_type", "city").agg(
        F.round(F.sum("amount"), 2).alias("total_revenue"),
        F.count("*").alias("total_orders"),
        F.round(F.avg("amount"), 2).alias("avg_order_value"),
    ).orderBy("total_revenue", ascending=False)
