"""
PR-29: PySpark Deep Dive — The 5 S's of Optimization
Practiced on: 2026-04-03
VPS: will@31.97.162.147

The 5 S's framework for diagnosing slow Spark jobs:
  S1: Shuffle       — reduce data movement, use Broadcast Join
  S2: Skew          — salting to spread hot keys across partitions
  S3: Spill         — cache/persist strategy, storage levels
  S4: Storage       — repartition vs coalesce
  S5: Serialization — built-in functions vs Python UDFs

Key observations from this run:
  - BroadcastHashJoin confirmed in explain() — no shuffle for small table join
  - Salting spread restaurant_id=1 (4000 rows) evenly across 5 buckets (~1000 each)
  - Cache overhead > recompute cost for small data (5K rows) — don't cache blindly
  - F.when() was 1.4x faster than Python UDF on 5K rows (10-50x on large datasets)
  - explain() read bottom-up: Scan → Filter → BroadcastHashJoin → HashAggregate → Exchange

Run:
  docker cp /tmp/pr29_optimization.py spark-master:/opt/spark/jobs/pr29_optimization.py
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/pr29_optimization.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.storagelevel import StorageLevel
import time
import random

spark = SparkSession.builder \
    .appName("PR29-5S-Optimization") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Seed Data ──────────────────────────────────────────────────────────────────
restaurants = spark.createDataFrame([
    (1,"Pizza Palace","Italian","Sao Paulo",4.5),
    (2,"Burger Boss","American","Sao Paulo",3.8),
    (3,"Sushi Star","Japanese","Rio",4.9),
    (4,"Taco Town","Mexican","Rio",4.1),
    (5,"Pasta Place","Italian","Curitiba",4.7),
    (6,"BBQ Barn","American","Curitiba",3.5),
    (7,"Ramen Road","Japanese","Sao Paulo",4.3),
    (8,"Curry Club","Indian","Rio",4.6),
], ["restaurant_id","name","cuisine_type","city","average_rating"])

random.seed(42)
orders_data = [(i, random.randint(1,8), round(random.uniform(20,200),2),
                2024, random.randint(1,12)) for i in range(1, 5001)]
orders = spark.createDataFrame(orders_data,
    ["order_id","restaurant_id","amount","year","month"])

print(f"Orders: {orders.count()} rows | Restaurants: {restaurants.count()} rows")

# ══════════════════════════════════════════════════════════════════════════════
# S1: SHUFFLE — Broadcast Join vs Sort-Merge Join
# Rule: if one side < ~10MB → broadcast it → eliminates shuffle entirely
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== S1: SHUFFLE — Broadcast Join ===")

print("\n--- Sort-Merge Join (default, shuffles both sides) ---")
start = time.time()
result_smj = orders.join(restaurants, on="restaurant_id")
result_smj.count()
print(f"Sort-Merge Join: {round(time.time()-start, 2)}s")
result_smj.explain()

print("\n--- Broadcast Join (no shuffle — restaurants sent to each executor) ---")
start = time.time()
result_bj = orders.join(F.broadcast(restaurants), on="restaurant_id")
result_bj.count()
print(f"Broadcast Join: {round(time.time()-start, 2)}s")
result_bj.explain()

print(f"\nCurrent shuffle.partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# ══════════════════════════════════════════════════════════════════════════════
# S2: SKEW — Salting
# Problem: one key has 80% of data → one slow task blocks the whole job
# Solution: add random salt to spread hot key across N buckets
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== S2: SKEW — Salting ===")

skewed_data = [(i, 1 if i <= 4000 else random.randint(2,8),
                round(random.uniform(20,200),2)) for i in range(1, 5001)]
skewed_orders = spark.createDataFrame(skewed_data,
    ["order_id","restaurant_id","amount"])

print("Partition distribution BEFORE salting:")
skewed_orders.groupBy("restaurant_id").count().orderBy("count", ascending=False).show(5)

NUM_BUCKETS = 5
salted_orders = skewed_orders.withColumn("salt",
    (F.rand() * NUM_BUCKETS).cast("int"))
salted_restaurants = restaurants.withColumn("salt",
    F.explode(F.array([F.lit(i) for i in range(NUM_BUCKETS)])))

result_salted = salted_orders.join(
    salted_restaurants, on=["restaurant_id", "salt"]
).drop("salt")

print(f"Salted join result: {result_salted.count()} rows")
print("Distribution AFTER salting (even across buckets):")
salted_orders.groupBy("salt").count().orderBy("salt").show()

# ══════════════════════════════════════════════════════════════════════════════
# S3: SPILL — Cache/Persist
# Key insight: cache only pays off for large DataFrames used multiple times
# On small data, cache overhead > recompute cost (proven in this run!)
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== S3: SPILL — Storage Levels & Persist ===")

expensive_df = orders.join(F.broadcast(restaurants), on="restaurant_id") \
    .withColumn("revenue_category",
        F.when(F.col("amount") > 150, "High")
         .when(F.col("amount") > 75, "Medium")
         .otherwise("Low"))

start = time.time()
expensive_df.count()
expensive_df.groupBy("cuisine_type").agg(F.sum("amount")).count()
print(f"Without cache: {round(time.time()-start, 2)}s for 2 actions")

expensive_df.cache()
start = time.time()
expensive_df.count()
expensive_df.groupBy("cuisine_type").agg(F.sum("amount")).count()
print(f"With cache:    {round(time.time()-start, 2)}s for 2 actions")
print("NOTE: on small data cache can be SLOWER due to overhead!")
expensive_df.unpersist()

print("\nStorage levels:")
print("  MEMORY_ONLY         → fastest, OOMs if not enough RAM")
print("  MEMORY_AND_DISK     → spills to disk if needed (safest)")
print("  MEMORY_ONLY_SER     → serialized, less RAM, slower reads")
print("  DISK_ONLY           → never OOMs, slowest")

# ══════════════════════════════════════════════════════════════════════════════
# S4: STORAGE — Repartition vs Coalesce
# coalesce:     reduce partitions, NO shuffle (merges existing)
# repartition:  change partitions, WITH shuffle (even distribution)
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== S4: STORAGE — Repartition vs Coalesce ===")

print(f"Default partitions: {orders.rdd.getNumPartitions()}")
print(f"After coalesce(2):  {orders.coalesce(2).rdd.getNumPartitions()} partitions — no shuffle")
print(f"After repartition(20): {orders.repartition(20, 'restaurant_id').rdd.getNumPartitions()} partitions — with shuffle")
print("\nRule: coalesce() before save, repartition() before join")

# ══════════════════════════════════════════════════════════════════════════════
# S5: SERIALIZATION — Python UDF vs Built-in Functions
# Python UDF: JVM → serialize → Python process → deserialize → JVM (slow)
# Built-in:   stays in JVM the whole time (fast)
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== S5: SERIALIZATION — Python UDF vs Built-ins ===")

@F.udf(StringType())
def categorize_slow(amount):
    if amount is None: return "Unknown"
    if amount > 150: return "High"
    if amount > 75:  return "Medium"
    return "Low"

start = time.time()
orders.withColumn("category", categorize_slow(F.col("amount"))).count()
slow_time = round(time.time()-start, 2)
print(f"Python UDF:      {slow_time}s  ← crosses JVM↔Python boundary")

start = time.time()
orders.withColumn("category",
    F.when(F.col("amount") > 150, "High")
     .when(F.col("amount") > 75, "Medium")
     .otherwise("Low")
).count()
fast_time = round(time.time()-start, 2)
print(f"Built-in F.when: {fast_time}s  ← stays in JVM")
print(f"Speedup: {round(slow_time/fast_time, 1)}x (on 500M rows this is 10-50x)")

# ══════════════════════════════════════════════════════════════════════════════
# BONUS: Reading explain() plans
# Read BOTTOM-UP: data flows from leaf nodes up to root
# Key nodes: BroadcastHashJoin=good, SortMergeJoin=shuffle, Exchange=shuffle
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== BONUS: explain() — Read Bottom-Up ===")
print("BroadcastHashJoin → good, no shuffle")
print("SortMergeJoin     → shuffle on both sides")
print("Exchange          → shuffle (check if avoidable)")
print("FileScan/RDDScan  → reading source data")
print("Filter            → predicate pushdown working")

final = orders.join(F.broadcast(restaurants), "restaurant_id") \
    .groupBy("cuisine_type") \
    .agg(F.round(F.sum("amount"),2).alias("total_revenue"),
         F.count("*").alias("total_orders")) \
    .orderBy("total_revenue", ascending=False)

final.show()
final.explain()

print("\n=== PR-29 Complete! ===")
spark.stop()
