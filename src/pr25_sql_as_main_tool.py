"""
PR-25: Spark SQL como Ferramenta Principal — Hybrid SQL + Python Patterns
Practiced on: 2026-04-03
VPS: will@31.97.162.147

Core insight:
  SQL, DataFrame API, and expr() ALL compile to the same Catalyst physical plan.
  The choice is about readability and team fit, NOT performance.

  SQL      → best for analytics, reporting, complex aggregations
  DF API   → best for programmatic ETL, dynamic column lists, unit tests
  expr()   → best for mixing SQL expressions inside Python API calls

3 Hybrid Patterns demonstrated:
  1. SQL first  → Python API   (filter in SQL, programmatic transforms in Python)
  2. Python first → temp view  → SQL   (build DF dynamically, query it with SQL)
  3. expr() inside agg/select  (SQL expressions embedded in Python calls)

Plus:
  - When to use temp view vs permanent table
  - explain() to prove all patterns produce the same physical plan
  - Decision framework: SQL vs DF API

Run:
  docker cp /tmp/pr25_sql_as_main_tool.py spark-master:/opt/spark/jobs/pr25_sql_as_main_tool.py
  docker exec spark-master /opt/spark/bin/spark-submit \\
    --master spark://spark-master:7077 \\
    /opt/spark/jobs/pr25_sql_as_main_tool.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import expr

spark = SparkSession.builder \
    .appName("PR25-SQL-As-Main-Tool") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Seed data ──────────────────────────────────────────────────────────────────
restaurants_df = spark.createDataFrame([
    (1, "Pizza Palace", "Italian",  "Sao Paulo", 4.5, 120, 1200.0),
    (2, "Burger Boss",  "American", "Sao Paulo", 3.8, 200,  980.0),
    (3, "Sushi Star",   "Japanese", "Rio",       4.9,  80, 1800.0),
    (4, "Taco Town",    "Mexican",  "Rio",       4.1, 150,  760.0),
    (5, "Pasta Place",  "Italian",  "Curitiba",  4.7,  90, 1400.0),
    (6, "BBQ Barn",     "American", "Curitiba",  3.5, 300,  540.0),
    (7, "Ramen Road",   "Japanese", "Sao Paulo", 4.3, 110, 1050.0),
    (8, "Curry Club",   "Indian",   "Rio",       4.6,  70, 1600.0),
], ["restaurant_id", "name", "cuisine_type", "city", "average_rating",
    "monthly_orders", "monthly_revenue"])

orders_df = spark.createDataFrame([
    (1,  1, 45.90), (2,  1, 89.50), (3,  2, 32.00),
    (4,  2, 67.80), (5,  3, 55.00), (6,  3, 92.30),
    (7,  4, 41.20), (8,  5, 78.60), (9,  6, 33.40),
    (10, 7, 95.10), (11, 8, 62.70), (12, 1, 88.00),
], ["order_id", "restaurant_id", "amount"])

restaurants_df.createOrReplaceTempView("restaurants")
orders_df.createOrReplaceTempView("orders")

# ══════════════════════════════════════════════════════════════════════════════
# PATTERN 1: SQL first → Python API
#
# Use case: SQL analyst writes the filter/join/aggregation.
#           Python engineer applies dynamic post-processing.
# Rule: SQL owns the "what" (business logic), Python owns the "how" (structure).
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== PATTERN 1: SQL first → Python API ===")

# Step 1: SQL does the heavy lifting — filter + join + group
top_cuisines_sql = spark.sql("""
    SELECT
        r.cuisine_type,
        r.city,
        COUNT(o.order_id)         AS total_orders,
        ROUND(SUM(o.amount), 2)   AS total_revenue,
        ROUND(AVG(r.average_rating), 2) AS avg_rating
    FROM restaurants r
    LEFT JOIN orders o ON r.restaurant_id = o.restaurant_id
    WHERE r.average_rating > 3.5
    GROUP BY r.cuisine_type, r.city
""")

# Step 2: Python API adds programmatic logic on top
# (e.g. dynamic column based on a config variable — hard to do in pure SQL)
REVENUE_THRESHOLD = 100.0  # could come from a config file or CLI arg

result_p1 = top_cuisines_sql \
    .withColumn("is_high_revenue",
        F.when(F.col("total_revenue") > REVENUE_THRESHOLD, "Yes").otherwise("No")) \
    .orderBy("total_revenue", ascending=False)

result_p1.show(truncate=False)

# ══════════════════════════════════════════════════════════════════════════════
# PATTERN 2: Python API first → createOrReplaceTempView → SQL
#
# Use case: you build a DataFrame programmatically (dynamic columns, loops,
#           UDFs) and then hand it to SQL analysts for reporting.
# Rule: Python owns the "construction", SQL owns the "reporting".
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== PATTERN 2: Python API → Temp View → SQL ===")

# Step 1: Python builds an enriched DataFrame (harder to do cleanly in pure SQL)
enriched_df = restaurants_df \
    .withColumn("revenue_per_order",
        F.round(F.col("monthly_revenue") / F.col("monthly_orders"), 2)) \
    .withColumn("performance_tier",
        F.when(F.col("average_rating") >= 4.5, "Tier 1 - Star")
         .when(F.col("average_rating") >= 4.0, "Tier 2 - Good")
         .otherwise("Tier 3 - Standard")) \
    .withColumn("cuisine_type", F.upper(F.trim(F.col("cuisine_type"))))

# Step 2: expose as temp view so SQL can query it
enriched_df.createOrReplaceTempView("enriched_restaurants")

# Step 3: SQL analyst writes the report query
spark.sql("""
    SELECT
        performance_tier,
        COUNT(*)                              AS restaurant_count,
        ROUND(AVG(revenue_per_order), 2)      AS avg_revenue_per_order,
        ROUND(AVG(average_rating), 2)         AS avg_rating,
        COLLECT_LIST(name)                    AS restaurants
    FROM enriched_restaurants
    GROUP BY performance_tier
    ORDER BY performance_tier
""").show(truncate=False)

# ══════════════════════════════════════════════════════════════════════════════
# PATTERN 3: expr() — SQL expressions inside Python API
#
# Use case: most of your pipeline is Python API, but one complex expression
#           is easier to write in SQL syntax (CASE, REGEXP, date math, etc).
# Rule: don't switch contexts just for one expression — use expr().
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== PATTERN 3: expr() — SQL inside Python API ===")

result_p3 = restaurants_df.select(
    "name", "cuisine_type", "city", "average_rating", "monthly_orders",

    # expr(): SQL CASE expression inside Python API call — no context switch
    expr("CASE WHEN average_rating >= 4.5 THEN 'Excellent' "
         "     WHEN average_rating >= 4.0 THEN 'Good' "
         "     ELSE 'Standard' END AS rating_label"),

    # expr(): SQL arithmetic + ROUND
    expr("ROUND(monthly_revenue / monthly_orders, 2) AS revenue_per_order"),

    # expr(): SQL string function
    expr("CONCAT(UPPER(LEFT(cuisine_type, 1)), LOWER(SUBSTRING(cuisine_type, 2))) AS cuisine_capitalized"),
).groupBy("rating_label").agg(
    # expr() inside agg() — cleaner than F.round(F.avg(...), 2).alias(...)
    expr("ROUND(AVG(average_rating), 2) AS avg_rating"),
    expr("COUNT(*) AS total"),
    expr("ROUND(AVG(revenue_per_order), 2) AS avg_rev_per_order"),
).orderBy("avg_rating", ascending=False)

result_p3.show(truncate=False)

# ══════════════════════════════════════════════════════════════════════════════
# PROVE IT: all 3 patterns compile to the same Catalyst plan
#
# This is the key interview point: SQL, DF API, expr() = identical performance.
# The physical plan for the same query is the same regardless of API used.
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== PROVE IT: Same query, two APIs — compare plans ===")

print("\n--- Via SQL ---")
sql_version = spark.sql("""
    SELECT cuisine_type, ROUND(AVG(average_rating), 2) AS avg_rating, COUNT(*) AS total
    FROM restaurants
    WHERE average_rating > 4.0
    GROUP BY cuisine_type
    ORDER BY avg_rating DESC
""")
sql_version.explain(mode="simple")

print("\n--- Via DataFrame API ---")
api_version = restaurants_df \
    .filter(F.col("average_rating") > 4.0) \
    .groupBy("cuisine_type") \
    .agg(
        F.round(F.avg("average_rating"), 2).alias("avg_rating"),
        F.count("*").alias("total")
    ) \
    .orderBy("avg_rating", ascending=False)
api_version.explain(mode="simple")

print("\nNotice: both plans show the same nodes (Filter → HashAggregate → Sort)")
print("Performance is IDENTICAL — choose based on readability and team skills.")

# ══════════════════════════════════════════════════════════════════════════════
# TEMP VIEW vs PERMANENT TABLE — when to use which
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== Temp View vs Permanent Table ===")
print("""
TEMP VIEW (createOrReplaceTempView):
  - Lives only in the current SparkSession
  - Dropped when session ends
  - No data written to disk — just a pointer to the DataFrame
  - Use for: intermediate pipeline steps, passing data between SQL and Python

GLOBAL TEMP VIEW (createGlobalTempView):
  - Visible across SparkSessions in the same JVM
  - Access via: spark.sql("SELECT * FROM global_temp.view_name")
  - Use for: shared intermediate results across multiple jobs in same app

PERMANENT TABLE (spark.sql("CREATE TABLE ...")):
  - Persisted to the Hive metastore (or Unity Catalog in Databricks)
  - Survives session restart
  - Actual data written to disk (Parquet/Delta)
  - Use for: production outputs, bronze/silver/gold layers
""")

# ══════════════════════════════════════════════════════════════════════════════
# DECISION FRAMEWORK — SQL vs DataFrame API
# ══════════════════════════════════════════════════════════════════════════════
print("=== Decision Framework: SQL vs DataFrame API ===")
print("""
Use SQL when:
  ✓ Query is mostly SELECT / GROUP BY / JOIN / WINDOW → SQL is cleaner
  ✓ SQL analysts will maintain it
  ✓ Complex aggregations (ROLLUP, CUBE, GROUPING SETS)
  ✓ You want to store it in a .sql file for version control

Use DataFrame API when:
  ✓ Dynamic column lists (loop over config)
  ✓ Reusable transformation functions (unit-testable)
  ✓ Complex conditional logic that needs Python variables
  ✓ Chaining many transforms where SQL would require many nested CTEs

Use expr() when:
  ✓ 90% of your pipeline is Python API but one expression is cleaner in SQL
  ✓ CASE WHEN, REGEXP, date arithmetic inside .select() or .agg()
  ✓ You want SQL readability without switching to spark.sql()

All three: SAME performance. Choose for your team.
""")

print("=== PR-25 Complete! ===")
spark.stop()
