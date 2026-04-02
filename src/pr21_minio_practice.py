"""
PR-21: Spark + MinIO (Object Storage / S3)
Practiced on: 2026-04-02
VPS: will@31.97.162.147
Cluster: apache/spark:3.5.1 (spark-master + spark-worker-1)
JARs needed: hadoop-aws-3.3.4.jar, aws-java-sdk-bundle-1.12.262.jar

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    --conf spark.executor.extraClassPath=/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    /opt/spark/jobs/pr21_minio_practice.py

MinIO setup:
  docker run -d --name minio --network spark-net \
    -p 9000:9000 -p 9001:9001 \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    minio/minio server /data --console-address ":9001"
  docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
  docker exec minio mc mb local/ubereats-datalake
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

spark = SparkSession.builder \
    .appName("PR21-MinIO-Practice") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

LAKE = "s3a://ubereats-datalake"

# ── Seed Data ──────────────────────────────────────────────────────────────────
r_schema = StructType([
    StructField("restaurant_id", IntegerType()),
    StructField("name", StringType()),
    StructField("cuisine_type", StringType()),
    StructField("city", StringType()),
    StructField("average_rating", FloatType()),
])
restaurants = [
    (1, "Pizza Palace", "Italian",  "Sao Paulo", 4.5),
    (2, "Burger Boss",  "American", "Sao Paulo", 3.8),
    (3, "Sushi Star",   "Japanese", "Rio",       4.9),
    (4, "Taco Town",    "Mexican",  "Rio",       4.1),
    (5, "Pasta Place",  "Italian",  "Curitiba",  4.7),
    (6, "BBQ Barn",     "American", "Curitiba",  3.5),
    (7, "Ramen Road",   "Japanese", "Sao Paulo", 4.3),
    (8, "Curry Club",   "Indian",   "Rio",       4.6),
]

o_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("restaurant_id", IntegerType()),
    StructField("amount", FloatType()),
    StructField("order_date", StringType()),
    StructField("year", IntegerType()),
    StructField("month", IntegerType()),
])
orders = [
    (1, 1, 45.90, "2024-01-15", 2024, 1),
    (2, 3, 89.50, "2024-01-20", 2024, 1),
    (3, 2, 32.00, "2024-02-10", 2024, 2),
    (4, 5, 67.80, "2024-02-14", 2024, 2),
    (5, 1, 55.00, "2024-03-01", 2024, 3),
    (6, 8, 92.30, "2024-03-22", 2024, 3),
]

df_restaurants = spark.createDataFrame(restaurants, r_schema)
df_orders = spark.createDataFrame(orders, o_schema)

# ── Exercise 1: Write formats ──────────────────────────────────────────────────
print("\n=== Exercise 1: Write Parquet, ORC, JSON to MinIO ===")
df_restaurants.write.format("parquet").mode("overwrite").save(f"{LAKE}/bronze/restaurants")
print("Parquet OK")
df_restaurants.write.format("orc").mode("overwrite").save(f"{LAKE}/bronze/restaurants_orc")
print("ORC OK")
df_restaurants.write.format("json").mode("overwrite").save(f"{LAKE}/bronze/restaurants_json")
print("JSON OK")

# ── Exercise 2: Partitioned write ─────────────────────────────────────────────
# Spark creates folder structure: year=2024/month=1/, year=2024/month=2/, etc.
print("\n=== Exercise 2: Partitioned Write (year/month) ===")
df_orders.write.format("parquet") \
    .partitionBy("year", "month") \
    .mode("overwrite") \
    .save(f"{LAKE}/bronze/orders")
print("Partitioned orders written")

# ── Exercise 3: Read back + partition pruning ──────────────────────────────────
# filter(month==1) only scans the month=1 folder — skips month=2 and month=3
print("\n=== Exercise 3: Read Parquet + Partition Pruning ===")
df_r = spark.read.parquet(f"{LAKE}/bronze/restaurants")
df_r.show()
print(f"Total restaurants: {df_r.count()}")

df_o = spark.read.parquet(f"{LAKE}/bronze/orders")
df_jan = df_o.filter(F.col("month") == 1)
print(f"Orders in January (partition pruning): {df_jan.count()}")
df_jan.show()

# ── Exercise 4: Medallion Silver layer ────────────────────────────────────────
print("\n=== Exercise 4: Silver Layer (cleaned + enriched) ===")
df_silver = df_r.withColumn("rating_category",
    F.when(F.col("average_rating") >= 4.5, "Excellent")
     .when(F.col("average_rating") >= 4.0, "Good")
     .otherwise("Average")
)
df_silver.write.format("parquet").mode("overwrite").save(f"{LAKE}/silver/restaurants")
spark.read.parquet(f"{LAKE}/silver/restaurants").show()

# ── Exercise 5: Gold layer (KPIs) ─────────────────────────────────────────────
print("\n=== Exercise 5: Gold Layer (KPIs by cuisine) ===")
df_gold = df_silver.groupBy("cuisine_type").agg(
    F.count("*").alias("total_restaurants"),
    F.round(F.avg("average_rating"), 2).alias("avg_rating"),
    F.max("average_rating").alias("best_rating")
).orderBy("avg_rating", ascending=False)

df_gold.write.format("parquet") \
    .partitionBy("cuisine_type") \
    .mode("overwrite") \
    .save(f"{LAKE}/gold/cuisine_performance")

spark.read.parquet(f"{LAKE}/gold/cuisine_performance").show()

# ── Exercise 6: Read different formats ────────────────────────────────────────
print("\n=== Exercise 6: Read ORC and JSON ===")
spark.read.format("orc").load(f"{LAKE}/bronze/restaurants_orc").show(3)
spark.read.format("json").load(f"{LAKE}/bronze/restaurants_json").show(3)

print("\n=== PR-21 Complete! ===")
spark.stop()
