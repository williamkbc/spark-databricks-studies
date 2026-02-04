# Dia 10 - Spark SQL Foundations: Data Ingestion (PR-14)

## Objetivo
Usar Spark SQL para ingestão de dados: ler arquivos, criar views/tabelas, e executar queries SQL.

---

## 1. Setup

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark SQL").master("local[*]").getOrCreate()

base_path = "/opt/spark/work/data"
restaurants_df = spark.read.json(f"{base_path}/mysql/restaurants/")
orders_df = spark.read.json(f"{base_path}/kafka/orders/")
drivers_df = spark.read.json(f"{base_path}/postgres/drivers/")
```

---

## 2. Criar Views Temporárias

```python
restaurants_df.createOrReplaceTempView("restaurants")
orders_df.createOrReplaceTempView("orders")
drivers_df.createOrReplaceTempView("drivers")
```

---

## 3. Queries SQL

```python
# Filtrar e ordenar
spark.sql("""
    SELECT name, cuisine_type, average_rating
    FROM restaurants
    WHERE average_rating > 4.0
    ORDER BY average_rating DESC
""").show()

# Agregação
spark.sql("""
    SELECT cuisine_type, COUNT(*) as total, ROUND(AVG(average_rating), 2) as avg_rating
    FROM restaurants
    GROUP BY cuisine_type
    ORDER BY avg_rating DESC
""").show()

# JOIN
spark.sql("""
    SELECT r.name, r.cuisine_type, COUNT(o.order_id) as orders, SUM(o.total_amount) as revenue
    FROM orders o
    JOIN restaurants r ON o.restaurant_key = r.cnpj
    GROUP BY r.name, r.cuisine_type
""").show()
```

---

## 4. Criar Views com Cálculos

```python
spark.sql("""
    CREATE OR REPLACE TEMP VIEW popular_restaurants AS
    SELECT name, cuisine_type, average_rating, num_reviews,
           ROUND(average_rating * SQRT(num_reviews / 100), 2) as popularity_score
    FROM restaurants
    ORDER BY popularity_score DESC
""")
```

---

## Resumo

| Operação | Comando |
|----------|---------|
| Criar view | `df.createOrReplaceTempView("nome")` |
| Query SQL | `spark.sql("SELECT ...")` |
| Criar DB | `CREATE DATABASE IF NOT EXISTS db` |

**Conclusão:** Spark SQL permite usar sintaxe SQL familiar para processar dados distribuídos.
