"""
PR-28: SQL End-to-End Modular Pipeline with Chained CTEs
Practiced on: 2026-04-03
VPS: will@31.97.162.147

Architecture:
  In production you'd split SQL into separate .sql files:
    sql/
      create_views.sql   → clean + normalize raw data
      restaurants.sql    → CTE: rank by cuisine, top performers
      orders.sql         → CTE: revenue per day, running totals
      final_report.sql   → chains all CTEs into one executive summary

  Here each SQL block is a named function that mirrors what a .sql file contains.
  The pipeline class loads and executes them in order — same pattern as reading files.

Key concepts:
  1. CTE chaining  — each WITH clause feeds the next (no temp tables needed)
  2. Modular SQL   — one function/file = one responsibility
  3. _execute()    — splits on ';', runs each statement (same as reading .sql files)
  4. ROLLUP        — hierarchical subtotals in the final report
  5. Window ranks  — RANK() OVER (PARTITION BY cuisine ORDER BY rating DESC)
  6. Scalar subquery in SELECT — pct = count / (SELECT COUNT(*) FROM ...) * 100

Interview angles:
  - "Why CTEs over subqueries?" → readable, reusable within the same query, Catalyst
    collapses them into one plan (no extra shuffle)
  - "Why modular SQL files?" → each analyst owns one file, git diffs are clean,
    easy to unit-test individual CTEs
  - "CTE vs temp view?" → CTE lives only inside the query; temp view persists in
    the session — use temp view when multiple queries need the same clean layer

Run:
  docker cp /tmp/pr28_sql_modular.py spark-master:/opt/spark/jobs/pr28_sql_modular.py
  docker exec spark-master /opt/spark/bin/spark-submit \\
    --master spark://spark-master:7077 \\
    /opt/spark/jobs/pr28_sql_modular.py
"""
from pyspark.sql import SparkSession


# ── SparkSession ───────────────────────────────────────────────────────────────

spark = SparkSession.builder \
    .appName("PR28-SQL-Modular") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ── Seed Data (in-memory — same as reading JSON/Parquet from a lake) ───────────

spark.createDataFrame([
    (1, "Pizza Palace", "Italian",  "Sao Paulo", 4.5, 120, 1200.0),
    (2, "Burger Boss",  "American", "Sao Paulo", 3.8, 200,  980.0),
    (3, "Sushi Star",   "Japanese", "Rio",       4.9,  80, 1800.0),
    (4, "Taco Town",    "Mexican",  "Rio",       4.1, 150,  760.0),
    (5, "Pasta Place",  "Italian",  "Curitiba",  4.7,  90, 1400.0),
    (6, "BBQ Barn",     "American", "Curitiba",  3.5, 300,  540.0),
    (7, "Ramen Road",   "Japanese", "Sao Paulo", 4.3, 110, 1050.0),
    (8, "Curry Club",   "Indian",   "Rio",       4.6,  70, 1600.0),
], ["restaurant_id", "name", "cuisine_type", "city",
    "average_rating", "monthly_orders", "monthly_revenue"]).createOrReplaceTempView("raw_restaurants")

spark.createDataFrame([
    (1,  1, 45.90, "2024-01-15"), (2,  1, 89.50, "2024-01-20"),
    (3,  2, 32.00, "2024-02-05"), (4,  2, 67.80, "2024-02-10"),
    (5,  3, 55.00, "2024-01-08"), (6,  3, 92.30, "2024-01-25"),
    (7,  4, 41.20, "2024-03-01"), (8,  5, 78.60, "2024-03-15"),
    (9,  6, 33.40, "2024-02-20"), (10, 7, 95.10, "2024-01-30"),
    (11, 8, 62.70, "2024-02-28"), (12, 1, 88.00, "2024-03-10"),
    (13, 3, 72.50, "2024-03-20"), (14, 5, 55.00, "2024-02-14"),
    (15, 8, 91.20, "2024-01-05"),
], ["order_id", "restaurant_id", "amount", "order_date"]).createOrReplaceTempView("raw_orders")


# ══════════════════════════════════════════════════════════════════════════════
# SQL MODULE 1: create_views.sql
# Responsibility: clean and normalize raw data → temp views for downstream use
# In prod: spark.sql(open("sql/create_views.sql").read())
# ══════════════════════════════════════════════════════════════════════════════

def create_views():
    """mirrors: sql/create_views.sql"""

    # Clean restaurants: uppercase cuisine, add rating label
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW restaurants AS
        SELECT
            restaurant_id,
            name,
            UPPER(TRIM(cuisine_type))   AS cuisine_type,
            city,
            average_rating,
            monthly_orders,
            monthly_revenue,
            CASE
                WHEN average_rating >= 4.5 THEN 'Excellent'
                WHEN average_rating >= 4.0 THEN 'Good'
                ELSE 'Average'
            END AS rating_label
        FROM raw_restaurants
    """)

    # Parse order_date → year/month/day_of_week
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW orders AS
        SELECT
            order_id,
            restaurant_id,
            amount,
            TO_DATE(order_date)            AS order_date,
            YEAR(TO_DATE(order_date))      AS year,
            MONTH(TO_DATE(order_date))     AS month,
            DAYOFWEEK(TO_DATE(order_date)) AS day_of_week
        FROM raw_orders
    """)

    print("✓ Views created: restaurants, orders")


# ══════════════════════════════════════════════════════════════════════════════
# SQL MODULE 2: restaurants.sql
# Responsibility: ranking + top performers per cuisine (CTE chaining)
# ══════════════════════════════════════════════════════════════════════════════

def analyze_restaurants():
    """mirrors: sql/restaurants.sql"""
    print("\n=== MODULE 2: Restaurant Analysis ===")

    # CTE 1: rank every restaurant within its cuisine by rating
    # CTE 2: filter to top 2 per cuisine
    # CTE 3: add cuisine-level averages as context
    # Final: join all three → one clean result
    spark.sql("""
        WITH ranked AS (
            SELECT
                restaurant_id, name, cuisine_type, city, average_rating,
                monthly_orders, rating_label,
                RANK() OVER (
                    PARTITION BY cuisine_type
                    ORDER BY average_rating DESC
                ) AS rank_in_cuisine,
                DENSE_RANK() OVER (
                    PARTITION BY cuisine_type
                    ORDER BY average_rating DESC
                ) AS dense_rank_in_cuisine
            FROM restaurants
        ),
        top_performers AS (
            SELECT * FROM ranked WHERE rank_in_cuisine <= 2
        ),
        cuisine_stats AS (
            SELECT
                cuisine_type,
                ROUND(AVG(average_rating), 2) AS cuisine_avg_rating,
                SUM(monthly_orders)            AS cuisine_total_orders,
                COUNT(*)                       AS restaurant_count
            FROM restaurants
            GROUP BY cuisine_type
        )
        SELECT
            tp.name,
            tp.cuisine_type,
            tp.city,
            tp.average_rating,
            tp.rating_label,
            tp.rank_in_cuisine,
            cs.cuisine_avg_rating,
            ROUND(tp.average_rating - cs.cuisine_avg_rating, 2) AS above_cuisine_avg,
            cs.cuisine_total_orders
        FROM top_performers tp
        JOIN cuisine_stats cs ON tp.cuisine_type = cs.cuisine_type
        ORDER BY tp.cuisine_type, tp.rank_in_cuisine
    """).show(truncate=False)

    # Rating distribution with % — scalar subquery in SELECT
    print("--- Rating Distribution ---")
    spark.sql("""
        WITH rating_counts AS (
            SELECT rating_label, COUNT(*) AS total
            FROM restaurants
            GROUP BY rating_label
        )
        SELECT
            rating_label,
            total,
            ROUND(100.0 * total / (SELECT COUNT(*) FROM restaurants), 1) AS pct
        FROM rating_counts
        ORDER BY total DESC
    """).show()


# ══════════════════════════════════════════════════════════════════════════════
# SQL MODULE 3: orders.sql
# Responsibility: temporal analysis with running totals and monthly rollups
# ══════════════════════════════════════════════════════════════════════════════

def analyze_orders():
    """mirrors: sql/orders.sql"""
    print("\n=== MODULE 3: Order Analysis ===")

    # CTE chain: monthly_summary → running_total → top_month
    spark.sql("""
        WITH monthly_summary AS (
            SELECT
                year, month,
                COUNT(*)                    AS order_count,
                ROUND(SUM(amount), 2)       AS total_revenue,
                ROUND(AVG(amount), 2)       AS avg_order_value
            FROM orders
            GROUP BY year, month
        ),
        running_total AS (
            SELECT
                year, month, order_count, total_revenue, avg_order_value,
                SUM(total_revenue) OVER (
                    ORDER BY year, month
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_revenue,
                LAG(total_revenue, 1) OVER (ORDER BY year, month) AS prev_month_revenue
            FROM monthly_summary
        )
        SELECT
            year, month, order_count,
            total_revenue,
            avg_order_value,
            cumulative_revenue,
            ROUND(total_revenue - COALESCE(prev_month_revenue, total_revenue), 2) AS mom_change
        FROM running_total
        ORDER BY year, month
    """).show(truncate=False)

    # Day-of-week pattern (1=Sun, 7=Sat)
    print("--- Orders by Day of Week ---")
    spark.sql("""
        SELECT
            day_of_week,
            CASE day_of_week
                WHEN 1 THEN 'Sun' WHEN 2 THEN 'Mon' WHEN 3 THEN 'Tue'
                WHEN 4 THEN 'Wed' WHEN 5 THEN 'Thu' WHEN 6 THEN 'Fri'
                ELSE 'Sat'
            END AS day_name,
            COUNT(*)              AS orders,
            ROUND(SUM(amount), 2) AS revenue
        FROM orders
        GROUP BY day_of_week
        ORDER BY day_of_week
    """).show()


# ══════════════════════════════════════════════════════════════════════════════
# SQL MODULE 4: final_report.sql
# Responsibility: executive summary — chains ALL CTEs, uses ROLLUP for subtotals
# This is the hardest query and shows why modular SQL matters:
#   if restaurants.sql and orders.sql were already trusted, this final layer
#   can focus only on business logic without re-cleaning data.
# ══════════════════════════════════════════════════════════════════════════════

def final_report():
    """mirrors: sql/final_report.sql"""
    print("\n=== MODULE 4: Executive Summary (ROLLUP + Chained CTEs) ===")

    spark.sql("""
        WITH restaurant_revenue AS (
            -- Join orders → restaurants to get cuisine/city context per order
            SELECT
                r.cuisine_type,
                r.city,
                r.name                          AS restaurant_name,
                r.average_rating,
                r.rating_label,
                COUNT(o.order_id)               AS order_count,
                ROUND(SUM(o.amount), 2)         AS total_revenue,
                ROUND(AVG(o.amount), 2)         AS avg_ticket
            FROM restaurants r
            LEFT JOIN orders o ON r.restaurant_id = o.restaurant_id
            GROUP BY r.cuisine_type, r.city, r.name,
                     r.average_rating, r.rating_label
        ),
        cuisine_city_rollup AS (
            -- ROLLUP: cuisine+city → cuisine subtotal → grand total
            SELECT
                COALESCE(cuisine_type, '** TOTAL **') AS cuisine_type,
                COALESCE(city,         '** ALL **')   AS city,
                COUNT(*)                              AS restaurant_count,
                SUM(order_count)                      AS total_orders,
                ROUND(SUM(total_revenue), 2)          AS total_revenue,
                ROUND(AVG(avg_ticket), 2)             AS avg_ticket,
                GROUPING(cuisine_type)                AS is_cuisine_subtotal,
                GROUPING(city)                        AS is_city_subtotal
            FROM restaurant_revenue
            GROUP BY ROLLUP(cuisine_type, city)
        ),
        ranked_cuisines AS (
            -- Rank cuisines by revenue (detail level only, no subtotal rows)
            SELECT
                cuisine_type, city, restaurant_count,
                total_orders, total_revenue, avg_ticket,
                is_cuisine_subtotal, is_city_subtotal,
                RANK() OVER (
                    ORDER BY total_revenue DESC
                ) AS revenue_rank
            FROM cuisine_city_rollup
            WHERE is_cuisine_subtotal = 0 AND is_city_subtotal = 0
        )
        SELECT
            cuisine_type, city,
            restaurant_count,
            total_orders,
            total_revenue,
            avg_ticket,
            revenue_rank
        FROM ranked_cuisines
        ORDER BY revenue_rank
    """).show(truncate=False)

    # Summary by cuisine only (the cuisine subtotal rows from ROLLUP)
    print("--- Revenue by Cuisine (ROLLUP subtotals) ---")
    spark.sql("""
        SELECT
            COALESCE(cuisine_type, '** GRAND TOTAL **') AS cuisine_type,
            ROUND(SUM(o.amount), 2)                     AS total_revenue,
            COUNT(o.order_id)                           AS total_orders,
            ROUND(AVG(r.average_rating), 2)             AS avg_rating
        FROM restaurants r
        LEFT JOIN orders o ON r.restaurant_id = o.restaurant_id
        GROUP BY ROLLUP(cuisine_type)
        ORDER BY cuisine_type NULLS LAST
    """).show()


# ══════════════════════════════════════════════════════════════════════════════
# Pipeline orchestrator — mirrors SparkSQLPipeline.run_pipeline()
# ══════════════════════════════════════════════════════════════════════════════

class SparkSQLPipeline:
    """
    In production, _execute_sql_file() would do:
        sql_content = open(file_path).read()
        statements = [s.strip() for s in sql_content.split(';') if s.strip()]
        for stmt in statements:
            self.spark.sql(stmt)

    Here we call functions that contain the equivalent SQL inline.
    """
    def run(self):
        print("\n=== PR-28 SQL Modular Pipeline ===")
        create_views()
        analyze_restaurants()
        analyze_orders()
        final_report()
        print("\n=== PR-28 Complete! ===")


if __name__ == "__main__":
    try:
        SparkSQLPipeline().run()
    finally:
        spark.stop()
