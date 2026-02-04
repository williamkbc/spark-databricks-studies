# Dia 08 - Técnicas Avançadas do Pandas API on Spark (PR-12)

## Objetivo
Explorar técnicas avançadas: integração com pandas nativo, Pandas UDFs, otimizações de performance, e workarounds para limitações do Pandas API on Spark.

---

## 1. Setup e Imports

```python
import pyspark.pandas as ps
import pandas as pd
import numpy as np
import time

from pyspark.sql.functions import pandas_udf, month, sum as spark_sum, count
from pyspark.sql.types import DoubleType

ps.set_option('compute.ops_on_diff_frames', True)

base_path = "/opt/spark/work/data"
restaurants_ps = ps.read_json(f"{base_path}/mysql/restaurants/")
orders_ps = ps.read_json(f"{base_path}/kafka/orders/")
drivers_ps = ps.read_json(f"{base_path}/postgres/drivers/")
```

---

## 2. Integração com Pandas Nativo

```python
# Converter para pandas nativo
top_restaurants = restaurants_ps.nlargest(10, 'average_rating')
top_restaurants_pd = top_restaurants.to_pandas()

# Operação exclusiva do pandas - rolling mean
rolling_mean = top_restaurants_pd.sort_values('average_rating')['average_rating'].rolling(window=3).mean()
print(rolling_mean)
```

**Conceito:** `to_pandas()` traz dados para o driver. Use apenas para datasets pequenos!

---

## 3. Pandas UDFs (Funções Otimizadas com Arrow)

```python
restaurants_spark = restaurants_ps.to_spark()

@pandas_udf(DoubleType())
def calculate_score(ratings: pd.Series, reviews: pd.Series) -> pd.Series:
    """Score: rating² * log(reviews) / 10"""
    return (ratings ** 2) * np.log1p(reviews) / 10

restaurants_spark = restaurants_spark.withColumn(
    "custom_score", 
    calculate_score("average_rating", "num_reviews")
)

restaurants_spark.select('name', 'average_rating', 'num_reviews', 'custom_score') \
    .orderBy('custom_score', ascending=False).show(10)
```

**Resultado:**
| name | rating | reviews | score |
|------|--------|---------|-------|
| Teles S.A. | 5.0 | 9901 | 23.00 |
| Marins e Associados | 5.0 | 5974 | 21.74 |

---

## 4. Otimizações

### Tipo de Índice
```python
ps.set_option('compute.default_index_type', 'distributed')  # melhor para datasets grandes
```

### Reparticionamento
```python
spark_df = restaurants_ps.to_spark()
spark_df_repartitioned = spark_df.repartition(4)  # 1 -> 4 partições
restaurants_repartitioned = ps.DataFrame(spark_df_repartitioned)
```

---

## 5. Lazy vs Eager Execution

```python
# LAZY (~0.08s) - apenas define
filtered_data = restaurants_ps[restaurants_ps.average_rating > 4.0]

# EAGER (~0.17s) - executa
result_count = len(filtered_data)  # 14 restaurantes
```

---

## 6. Limitações e Workarounds

### Workaround para .loc
```python
# Alternativa que funciona:
result = restaurants_ps[restaurants_ps.average_rating > 4.0][['name', 'cuisine_type']]
```

### Time Series via PySpark (workaround)
```python
from pyspark.sql.functions import month, sum as spark_sum, count

orders_spark = orders_ps.to_spark()

monthly_analysis = orders_spark.withColumn('order_month', month('order_date')) \
    .groupBy('order_month') \
    .agg(
        count('order_id').alias('total_pedidos'),
        spark_sum('total_amount').alias('receita_total')
    ) \
    .orderBy('order_month')

monthly_analysis.show()
```

**Resultado:**
| Mês | Pedidos | Receita |
|-----|---------|---------|
| 1 | 14 | 827.79 |
| 11 | 12 | 648.68 |
| 8 | 7 | 569.65 |

---

## Resumo

| Situação | Abordagem |
|----------|-----------|
| Dataset pequeno | Converter para pandas nativo |
| Função customizada | Pandas UDF |
| Dataset grande | Reparticionar via Spark |
| Operação não suportada | Workaround com PySpark |

---

**Conclusão:** Pandas API on Spark é poderoso, mas conhecer limitações e workarounds é essencial.
