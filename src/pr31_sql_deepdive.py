"""
PR-31: Spark SQL Deep Dive — CUBE, ROLLUP, Join Strategies, Window Functions
Practiced on: 2026-04-03
VPS: will@31.97.162.147

Concepts:
  1. ROLLUP     — hierarchical subtotals (cuisine → city → grand total)
  2. CUBE       — every possible combination of grouping dimensions
  3. GROUPING SETS — manual control: pick exactly which combinations you want
  4. Join Hints — /*+ BROADCAST(t) */, /*+ MERGE(t) */ in pure SQL
  5. Window Functions — ROW_NUMBER, RANK, DENSE_RANK, LAG, NTILE, PERCENT_RANK
  6. CACHE TABLE — SQL-level caching for repeated queries
  7. Window vs Correlated Subquery — always prefer window (single pass vs O(N))

Key rules:
  - GROUPING(col) = 1 means "this is a subtotal row" (col is NULL = all values)
  - Always use COALESCE(col, 'ALL') to label subtotal NULLs in output
  - Window functions ALWAYS need PARTITION BY — without it, all data goes to 1 partition (OOM risk)
  - ROLLUP < CUBE in number of rows — use ROLLUP for hierarchical, CUBE for pivot-style

Run:
  docker cp /tmp/pr31_sql_deepdive.py spark-master:/opt/spark/jobs/pr31_sql_deepdive.py
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/pr31_sql_deepdive.py
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("PR31-SQL-DeepDive") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Seed Data ──────────────────────────────────────────────────────────────────
restaurants = spark.createDataFrame([
    (1,"Pizza Palace","Italian","Sao Paulo",4.5,120),
    (2,"Burger Boss","American","Sao Paulo",3.8,200),
    (3,"Sushi Star","Japanese","Rio",4.9,80),
    (4,"Taco Town","Mexican","Rio",4.1,150),
    (5,"Pasta Place","Italian","Curitiba",4.7,90),
    (6,"BBQ Barn","American","Curitiba",3.5,300),
    (7,"Ramen Road","Japanese","Sao Paulo",4.3,110),
    (8,"Curry Club","Indian","Rio",4.6,70),
], ["restaurant_id","name","cuisine_type","city","average_rating","monthly_orders"])

orders = spark.createDataFrame([
    (1,1,45.90),(2,1,89.50),(3,2,32.00),(4,2,67.80),
    (5,3,55.00),(6,3,92.30),(7,4,41.20),(8,5,78.60),
    (9,6,33.40),(10,7,95.10),(11,8,62.70),(12,1,88.00),
], ["order_id","restaurant_id","amount"])

restaurants.createOrReplaceTempView("restaurants")
orders.createOrReplaceTempView("orders")

# ══════════════════════════════════════════════════════════════════════════════
# ROLLUP: hierarchical subtotals
# Produces: (cuisine+city), (cuisine only), (grand total)
# NULL in a column = "all values of that column"
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== ROLLUP: Hierarchical Subtotals ===")
spark.sql("""
    SELECT
        COALESCE(cuisine_type, '** ALL CUISINES **') AS cuisine_type,
        COALESCE(city, '** ALL CITIES **')           AS city,
        COUNT(*)                                      AS total_restaurants,
        ROUND(AVG(average_rating), 2)                 AS avg_rating,
        SUM(monthly_orders)                           AS total_orders,
        GROUPING(cuisine_type)                        AS is_cuisine_subtotal,
        GROUPING(city)                                AS is_city_subtotal
    FROM restaurants
    GROUP BY ROLLUP(cuisine_type, city)
    ORDER BY cuisine_type NULLS LAST, city NULLS LAST
""").show(20, truncate=False)

# ══════════════════════════════════════════════════════════════════════════════
# CUBE: every possible combination
# Produces: (cuisine+city), (cuisine only), (city only), (grand total)
# More rows than ROLLUP — use for pivot-table-style reporting
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== CUBE: Every Combination ===")
spark.sql("""
    SELECT
        COALESCE(cuisine_type, '** ALL **') AS cuisine_type,
        COALESCE(city, '** ALL **')          AS city,
        COUNT(*)                             AS total_restaurants,
        ROUND(AVG(average_rating), 2)        AS avg_rating,
        GROUPING(cuisine_type)               AS g_cuisine,
        GROUPING(city)                       AS g_city
    FROM restaurants
    GROUP BY CUBE(cuisine_type, city)
    ORDER BY g_cuisine DESC, g_city DESC, cuisine_type, city
""").show(25, truncate=False)

# ══════════════════════════════════════════════════════════════════════════════
# GROUPING SETS: pick exactly which combinations you want
# More flexible than ROLLUP/CUBE — skip city-only subtotal if not needed
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== GROUPING SETS: Manual Combinations ===")
spark.sql("""
    SELECT
        COALESCE(cuisine_type, '** ALL **') AS cuisine_type,
        COALESCE(city, '** ALL **')          AS city,
        COUNT(*)                             AS total
    FROM restaurants
    GROUP BY GROUPING SETS (
        (cuisine_type, city),  -- detail level
        (cuisine_type),        -- cuisine subtotal only
        ()                     -- grand total (skip city-only subtotal)
    )
    ORDER BY cuisine_type NULLS LAST, city NULLS LAST
""").show(20, truncate=False)

# ══════════════════════════════════════════════════════════════════════════════
# JOIN HINTS in SQL — same as F.broadcast() but in pure SQL
# /*+ BROADCAST(t) */ → BroadcastHashJoin, no shuffle
# /*+ MERGE(t) */     → SortMergeJoin, shuffles both sides
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== Join Hints: BROADCAST ===")
result_broadcast = spark.sql("""
    SELECT /*+ BROADCAST(restaurants) */
        o.order_id, r.name, r.cuisine_type, o.amount
    FROM orders o
    JOIN restaurants r ON o.restaurant_id = r.restaurant_id
    WHERE r.average_rating > 4.0
""")
result_broadcast.show()
result_broadcast.explain()

print("\n=== Join Hints: MERGE (forces SortMergeJoin) ===")
spark.sql("""
    SELECT /*+ MERGE(restaurants) */
        o.order_id, r.name, o.amount
    FROM orders o
    JOIN restaurants r ON o.restaurant_id = r.restaurant_id
""").explain()

# ══════════════════════════════════════════════════════════════════════════════
# WINDOW FUNCTIONS — always use PARTITION BY
# No PARTITION BY = all data in 1 partition = OOM on large tables
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== Window Functions: ROW_NUMBER, RANK, DENSE_RANK ===")
spark.sql("""
    SELECT
        name, cuisine_type, city, average_rating,
        ROW_NUMBER() OVER (PARTITION BY cuisine_type ORDER BY average_rating DESC) AS row_num,
        RANK()       OVER (PARTITION BY cuisine_type ORDER BY average_rating DESC) AS rank,
        DENSE_RANK() OVER (PARTITION BY cuisine_type ORDER BY average_rating DESC) AS dense_rank
    FROM restaurants
    ORDER BY cuisine_type, rank
""").show(truncate=False)

print("\n=== Window Functions: Running Total, LAG, diff ===")
spark.sql("""
    SELECT
        name, cuisine_type, monthly_orders,
        SUM(monthly_orders) OVER (PARTITION BY cuisine_type ORDER BY monthly_orders DESC) AS running_total,
        AVG(average_rating) OVER (PARTITION BY cuisine_type)                              AS cuisine_avg,
        LAG(monthly_orders, 1) OVER (PARTITION BY cuisine_type ORDER BY monthly_orders DESC) AS prev_orders,
        monthly_orders - LAG(monthly_orders, 1) OVER (
            PARTITION BY cuisine_type ORDER BY monthly_orders DESC)                        AS diff_from_prev
    FROM restaurants
    ORDER BY cuisine_type, monthly_orders DESC
""").show(truncate=False)

print("\n=== Window Functions: NTILE quartiles + PERCENT_RANK ===")
spark.sql("""
    SELECT
        name, average_rating,
        NTILE(4)       OVER (ORDER BY average_rating DESC) AS quartile,
        PERCENT_RANK() OVER (ORDER BY average_rating)      AS pct_rank
    FROM restaurants
    ORDER BY average_rating DESC
""").show()

# ══════════════════════════════════════════════════════════════════════════════
# CACHE TABLE — SQL-level caching for repeated queries on same table
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== CACHE TABLE ===")
spark.sql("CACHE TABLE restaurants")
spark.sql("SELECT cuisine_type, COUNT(*) FROM restaurants GROUP BY cuisine_type").show()
spark.sql("SELECT city, AVG(average_rating) FROM restaurants GROUP BY city").show()
spark.sql("UNCACHE TABLE restaurants")
print("Uncached.")

# ══════════════════════════════════════════════════════════════════════════════
# CORRELATED SUBQUERY vs WINDOW FUNCTION
# Subquery: O(N) — one subquery per row
# Window:   single pass — Catalyst optimizes it
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== BAD: Correlated Subquery — O(N) ===")
spark.sql("""
    SELECT name, cuisine_type, average_rating,
        (SELECT ROUND(AVG(r2.average_rating),2) FROM restaurants r2
         WHERE r2.cuisine_type = r.cuisine_type) AS cuisine_avg
    FROM restaurants r
    ORDER BY cuisine_type
""").show()

print("\n=== GOOD: Window Function — single pass ===")
spark.sql("""
    SELECT name, cuisine_type, average_rating,
        ROUND(AVG(average_rating) OVER (PARTITION BY cuisine_type), 2)        AS cuisine_avg,
        ROUND(average_rating - AVG(average_rating) OVER (PARTITION BY cuisine_type), 2) AS diff_from_avg
    FROM restaurants
    ORDER BY cuisine_type, average_rating DESC
""").show()

print("\n=== PR-31 Complete! ===")
spark.stop()
