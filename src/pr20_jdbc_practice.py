"""
PR-20: Spark + PostgreSQL via JDBC
Practiced on: 2026-04-02
VPS: will@31.97.162.147
Cluster: apache/spark:3.5.1 (spark-master + spark-worker-1)
JDBC Driver: postgresql-42.7.3.jar (required for PostgreSQL 15 SCRAM-SHA-256 auth)

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.executor.extraClassPath=/opt/spark/jars/postgresql-42.7.3.jar \
    --jars /opt/spark/jars/postgresql-42.7.3.jar \
    /opt/spark/jobs/pr20_jdbc_practice.py
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

spark = SparkSession.builder \
    .appName("PR20-JDBC-Practice") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

JDBC_URL = "jdbc:postgresql://postgres:5432/ubereats"
PROPS = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

# ── Seed Data ──────────────────────────────────────────────────────────────────
print("\n=== Seeding data ===")
schema = StructType([
    StructField("restaurant_id", IntegerType()),
    StructField("name", StringType()),
    StructField("cuisine_type", StringType()),
    StructField("city", StringType()),
    StructField("average_rating", FloatType()),
])
data = [
    (1, "Pizza Palace", "Italian",  "Sao Paulo", 4.5),
    (2, "Burger Boss",  "American", "Sao Paulo", 3.8),
    (3, "Sushi Star",   "Japanese", "Rio",       4.9),
    (4, "Taco Town",    "Mexican",  "Rio",       4.1),
    (5, "Pasta Place",  "Italian",  "Curitiba",  4.7),
    (6, "BBQ Barn",     "American", "Curitiba",  3.5),
    (7, "Ramen Road",   "Japanese", "Sao Paulo", 4.3),
    (8, "Curry Club",   "Indian",   "Rio",       4.6),
]
df_seed = spark.createDataFrame(data, schema)
df_seed.write.jdbc(url=JDBC_URL, table="restaurants", mode="overwrite", properties=PROPS)
print("OK - table restaurants created")

# ── Exercise 1: Basic Read ─────────────────────────────────────────────────────
print("\n=== Exercise 1: Basic Read ===")
df = spark.read.jdbc(url=JDBC_URL, table="restaurants", properties=PROPS)
df.show()

# ── Exercise 2: Predicate Pushdown ─────────────────────────────────────────────
# Filter runs inside PostgreSQL — less data transferred over network
print("\n=== Exercise 2: Predicate Pushdown (filter in PostgreSQL) ===")
df_top = spark.read.jdbc(
    url=JDBC_URL, table="restaurants", properties=PROPS,
    predicates=["average_rating > 4.3"]
)
df_top.show()
print(f"Rows with rating > 4.3: {df_top.count()}")

# ── Exercise 3: Parallel Read ──────────────────────────────────────────────────
# Spark launches numPartitions simultaneous queries, each reading a range of IDs
print("\n=== Exercise 3: Parallel Read (4 partitions) ===")
df_par = spark.read.jdbc(
    url=JDBC_URL, table="restaurants", properties=PROPS,
    column="restaurant_id", lowerBound=1, upperBound=8, numPartitions=4
)
print(f"Num partitions: {df_par.rdd.getNumPartitions()}")
df_par.show()

# ── Exercise 4: Custom SQL Query ───────────────────────────────────────────────
# Aggregation pushed to PostgreSQL — Spark only receives the result
print("\n=== Exercise 4: Custom SQL Query ===")
query = """(
    SELECT cuisine_type, city,
           ROUND(AVG(average_rating)::numeric, 2) AS avg_rating,
           COUNT(*) AS total
    FROM restaurants
    GROUP BY cuisine_type, city
    ORDER BY avg_rating DESC
) AS subq"""
spark.read.jdbc(url=JDBC_URL, table=query, properties=PROPS).show()

# ── Exercise 5: Write Modes ────────────────────────────────────────────────────
print("\n=== Exercise 5: Write Modes ===")
df.filter(F.col("cuisine_type") == "Italian").write.jdbc(
    url=JDBC_URL, table="test_write", mode="overwrite", properties=PROPS)
print("overwrite OK")

df.filter(F.col("cuisine_type") == "Japanese").write.jdbc(
    url=JDBC_URL, table="test_write", mode="append", properties=PROPS)
print("append OK")
spark.read.jdbc(url=JDBC_URL, table="test_write", properties=PROPS).show()

# ── Exercise 6: Bulk Loading ───────────────────────────────────────────────────
# batchsize + reWriteBatchedInserts = significantly faster writes for large datasets
print("\n=== Exercise 6: Bulk Loading ===")
bulk = {**PROPS, "batchsize": "10000", "reWriteBatchedInserts": "true"}
df.write.jdbc(url=JDBC_URL, table="restaurants_bulk", mode="overwrite", properties=bulk)
print(f"Bulk loaded: {spark.read.jdbc(url=JDBC_URL, table='restaurants_bulk', properties=PROPS).count()} rows")

print("\n=== PR-20 Complete! ===")
spark.stop()
