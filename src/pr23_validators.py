"""
PR-23: End-to-End Pipeline with Tests — Validators

Fail-fast validation: call these immediately after extract(),
before expensive transformations waste compute on bad data.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_no_nulls(df: DataFrame, columns: list) -> bool:
    """Return False if any of the given columns contain NULL values."""
    for col in columns:
        if df.filter(F.col(col).isNull()).count() > 0:
            return False
    return True


def validate_row_count(df: DataFrame, min_rows: int = 1) -> bool:
    """Return False if DataFrame has fewer rows than min_rows."""
    return df.count() >= min_rows


def validate_schema(df: DataFrame, expected_columns: list) -> bool:
    """Return False if any expected column is missing from the DataFrame."""
    actual = set(df.columns)
    for col in expected_columns:
        if col not in actual:
            return False
    return True
