# Dia 21 - Spark SQL como Ferramenta Principal (PR-25)

## Abordagens Híbridas SQL + Python

### Approach 1: SQL → Python API
```python
# Começa com SQL
top = spark.sql("SELECT * FROM restaurants WHERE average_rating > 4.0")

# Continua com Python API
result = top.groupBy("cuisine_type") \
    .agg({"average_rating": "avg"}) \
    .orderBy(col("total_reviews").desc())
```

### Approach 2: Python API → SQL
```python
# Começa com Python
filtered = restaurants_df.filter(col("average_rating") > 3.5)
filtered.createOrReplaceTempView("filtered_restaurants")

# Continua com SQL
result = spark.sql("""
    SELECT city, COUNT(*) as total, AVG(average_rating) as avg
    FROM filtered_restaurants
    GROUP BY city
""")
```

### Approach 3: SQL expressions dentro do Python
```python
result = restaurants_df.groupBy("cuisine_type").agg(
    expr("ROUND(AVG(average_rating), 2) AS avg_rating"),
    expr("SUM(num_reviews) AS total_reviews")
)
```

## Pipeline SQL Documentado

```python
def load_data(spark):
    spark.read.json(path).createOrReplaceTempView("restaurants_raw")

def clean_data(spark):
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW restaurants AS
        SELECT restaurant_id, name,
            COALESCE(cuisine_type, 'Unknown') AS cuisine_type,
            COALESCE(average_rating, 0) AS average_rating,
            REGEXP_REPLACE(phone_number, '[()\\s-]', '') AS phone
        FROM restaurants_raw
        WHERE restaurant_id IS NOT NULL
    """)

def analyze(spark):
    return spark.sql("""
        SELECT cuisine_type, COUNT(*) as total,
            ROUND(AVG(average_rating), 2) as avg_rating
        FROM restaurants
        GROUP BY cuisine_type
        ORDER BY total DESC
    """)

def run_pipeline():
    spark = create_spark_session()
    load_data(spark)
    clean_data(spark)
    result = analyze(spark)
    result.write.mode("overwrite").parquet("output/")
```

## Best Practices SQL em Spark

1. **Views estratégicas** - temp views para intermediários, permanent para produção
2. **Documentar queries** - comentários SQL explicando lógica
3. **Filtrar cedo** - WHERE antes de JOIN reduz volume
4. **CTEs > subqueries** - mais legível e manutenível
5. **Window functions > self-joins** - melhor performance
6. **expr()** - usar expressões SQL dentro do Python API
7. **Adaptive Query Execution** - habilitar para auto-otimização
