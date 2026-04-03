"""
PR-26: Error Handling, Retry, Validation, Custom Exceptions
Practiced on: 2026-04-03
VPS: will@31.97.162.147

Concepts:
  1. Custom Exceptions  — PipelineError hierarchy (DataQuality, Schema, Processing)
  2. Decorators        — @handle_spark_exceptions, @retry_operation, @log_execution
  3. Fail-Fast         — validate schema, row count, nulls immediately after read
  4. Retry w/ Backoff  — linear backoff: wait N * delay_seconds on each attempt

Run:
  docker cp /tmp/pr26_error_handling.py spark-master:/opt/spark/jobs/pr26_error_handling.py
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    --conf spark.executor.extraClassPath=/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    /opt/spark/jobs/pr26_error_handling.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from functools import wraps
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("PR26")

# ══════════════════════════════════════════════════════════════════════════════
# CONCEPT 1: CUSTOM EXCEPTIONS
# Hierarchy lets you catch at the right level:
#   except DataQualityError → alert data team
#   except ProcessingError  → retry or page on-call
#   except PipelineError    → catch-all for any pipeline issue
# ══════════════════════════════════════════════════════════════════════════════
class PipelineError(Exception): pass
class DataQualityError(PipelineError): pass
class SchemaValidationError(PipelineError): pass
class ProcessingError(PipelineError): pass

# ══════════════════════════════════════════════════════════════════════════════
# CONCEPT 2: DECORATORS — cross-cutting concerns without polluting business logic
# ══════════════════════════════════════════════════════════════════════════════
def handle_spark_exceptions(func):
    """Converts Spark exceptions into meaningful pipeline errors"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except AnalysisException as e:
            raise ProcessingError(f"Spark Analysis Error in {func.__name__}: {e}")
        except Exception as e:
            raise ProcessingError(f"Unexpected error in {func.__name__}: {e}")
    return wrapper

def retry_operation(max_retries=3, delay_seconds=2):
    """Retries on failure with linear backoff: wait N * delay_seconds"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    if attempt > 1:
                        log.info(f"Retry {attempt}/{max_retries} for {func.__name__}")
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        log.error(f"All {max_retries} attempts failed for {func.__name__}")
                        raise
                    wait = delay_seconds * attempt
                    log.warning(f"Attempt {attempt} failed: {e}. Waiting {wait}s...")
                    time.sleep(wait)
        return wrapper
    return decorator

def log_execution(func):
    """Logs start, end, and wall-clock duration of any function"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        log.info(f">>> Starting: {func.__name__}")
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = round(time.time() - start, 2)
        log.info(f"<<< Finished: {func.__name__} in {elapsed}s")
        return result
    return wrapper

# ══════════════════════════════════════════════════════════════════════════════
# CONCEPT 3: FAIL-FAST VALIDATORS — catch bad data immediately after read
# Rule: validate right after extract(), before any expensive transformation
# ══════════════════════════════════════════════════════════════════════════════
def validate_schema(df, required_columns, name):
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise SchemaValidationError(f"[{name}] Missing columns: {missing}")
    log.info(f"[{name}] Schema OK — {len(df.columns)} columns")

def validate_row_count(df, min_rows, name):
    count = df.count()
    if count < min_rows:
        raise DataQualityError(f"[{name}] Expected >={min_rows} rows, got {count}")
    log.info(f"[{name}] Row count OK — {count} rows")
    return count

def validate_no_nulls(df, columns, name):
    for col in columns:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            raise DataQualityError(f"[{name}] Column '{col}' has {null_count} nulls")
    log.info(f"[{name}] Null check OK on columns: {columns}")

def log_data_profile(df, name):
    count = df.count()
    log.info(f"[{name}] Profile: {count} rows x {len(df.columns)} cols | columns: {df.columns}")

# ══════════════════════════════════════════════════════════════════════════════
# EXTRACTOR: validation + retry + error handling stacked via decorators
# ══════════════════════════════════════════════════════════════════════════════
class DataExtractor:
    def __init__(self, spark): self.spark = spark

    @log_execution
    @handle_spark_exceptions
    @retry_operation(max_retries=3, delay_seconds=1)
    def extract_restaurants(self):
        schema = StructType([
            StructField("restaurant_id", IntegerType()),
            StructField("name", StringType()),
            StructField("cuisine_type", StringType()),
            StructField("city", StringType()),
            StructField("average_rating", FloatType()),
            StructField("phone", StringType()),
        ])
        data = [
            (1, "Pizza Palace", "Italian",  "Sao Paulo", 4.5, "(11) 1234-5678"),
            (2, "Burger Boss",  "American", "Sao Paulo", 3.8, "(11) 8765-4321"),
            (3, "Sushi Star",   "Japanese", "Rio",       4.9, "(21) 1111-2222"),
            (4, "Taco Town",    None,       "Rio",       4.1, "(21) 3333-4444"),  # null cuisine
            (5, "Pasta Place",  "Italian",  "Curitiba",  4.7, "(41) 5555-6666"),
            (6, "BBQ Barn",     "American", "Curitiba",  3.5, None),              # null phone
            (7, "Ramen Road",   "Japanese", "Sao Paulo", 4.3, "(11) 7777-8888"),
            (8, "Curry Club",   "Indian",   "Rio",       4.6, "(21) 9999-0000"),
        ]
        df = self.spark.createDataFrame(data, schema)
        validate_schema(df, ["restaurant_id", "name", "cuisine_type", "average_rating"], "restaurants")
        validate_row_count(df, min_rows=5, name="restaurants")
        log_data_profile(df, "restaurants")
        return df

# ══════════════════════════════════════════════════════════════════════════════
# TRANSFORMER: cleaning + enrichment
# ══════════════════════════════════════════════════════════════════════════════
class DataTransformer:
    def __init__(self, spark): self.spark = spark

    @log_execution
    @handle_spark_exceptions
    def clean_restaurants(self, df):
        return df \
            .withColumn("phone", F.regexp_replace(F.col("phone"), r"[() \-]", "")) \
            .fillna({"cuisine_type": "Unknown", "phone": "N/A"}) \
            .withColumn("rating_category",
                F.when(F.col("average_rating") >= 4.5, "Excellent")
                 .when(F.col("average_rating") >= 4.0, "Good")
                 .otherwise("Average")
            )

    @log_execution
    @handle_spark_exceptions
    def analyze_by_cuisine(self, df):
        return df.groupBy("cuisine_type").agg(
            F.count("*").alias("total"),
            F.round(F.avg("average_rating"), 2).alias("avg_rating"),
            F.max("average_rating").alias("best_rating")
        ).orderBy("avg_rating", ascending=False)

# ══════════════════════════════════════════════════════════════════════════════
# LOADER: retry on I/O failures
# ══════════════════════════════════════════════════════════════════════════════
class DataLoader:
    def __init__(self, spark): self.spark = spark

    @log_execution
    @handle_spark_exceptions
    @retry_operation(max_retries=3, delay_seconds=1)
    def save_to_minio(self, df, path):
        df.write.format("parquet").mode("overwrite").save(path)
        log.info(f"Saved to {path}")

    @log_execution
    def show_results(self, df, label):
        print(f"\n=== {label} ===")
        df.show()

# ══════════════════════════════════════════════════════════════════════════════
# DEMO: show what validation failures look like
# ══════════════════════════════════════════════════════════════════════════════
def demo_validation_failure(spark):
    print("\n=== DEMO: SchemaValidationError ===")
    bad_df = spark.createDataFrame([(1, "test")], ["id", "name"])
    try:
        validate_schema(bad_df, ["id", "name", "missing_col"], "bad_df")
    except SchemaValidationError as e:
        log.error(f"Caught SchemaValidationError: {e}")

    print("\n=== DEMO: DataQualityError (too few rows) ===")
    tiny_df = spark.createDataFrame([(1, "only one row")], ["id", "name"])
    try:
        validate_row_count(tiny_df, min_rows=10, name="tiny_df")
    except DataQualityError as e:
        log.error(f"Caught DataQualityError: {e}")

    print("\n=== DEMO: Retry decorator ===")
    attempt_counter = {"n": 0}

    @retry_operation(max_retries=3, delay_seconds=1)
    def flaky_function():
        attempt_counter["n"] += 1
        if attempt_counter["n"] < 3:
            raise ConnectionError(f"Simulated network error (attempt {attempt_counter['n']})")
        return "Success on attempt 3!"

    result = flaky_function()
    print(f"Result: {result}")

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("PR26-ErrorHandling") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    extractor   = DataExtractor(spark)
    transformer = DataTransformer(spark)
    loader      = DataLoader(spark)

    print("\n=== PR-26: Error Handling Pipeline ===")

    # Happy path
    restaurants = extractor.extract_restaurants()
    clean       = transformer.clean_restaurants(restaurants)
    analysis    = transformer.analyze_by_cuisine(clean)

    loader.show_results(clean,    "Cleaned Restaurants (nulls filled, phone normalized)")
    loader.show_results(analysis, "Cuisine KPI Analysis")
    loader.save_to_minio(analysis, "s3a://ubereats-datalake/gold/cuisine_analysis_pr26")

    # Demo failure scenarios
    demo_validation_failure(spark)

    print("\n=== PR-26 Complete! ===")
    spark.stop()
