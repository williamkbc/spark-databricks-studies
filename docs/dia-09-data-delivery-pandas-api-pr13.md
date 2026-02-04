# Dia 09 - Data Delivery com Pandas API on Spark (PR-13)

## Objetivo
Aprender a entregar dados processados: exportar para diferentes formatos, particionamento para datasets grandes, e criação de relatórios agregados.

---

## 1. Setup e Preparação dos Dados

```python
import pyspark.pandas as ps
import pandas as pd
import numpy as np
import os

ps.set_option('compute.ops_on_diff_frames', True)

base_path = "/opt/spark/work/data"
restaurants_ps = ps.read_json(f"{base_path}/mysql/restaurants/")
orders_ps = ps.read_json(f"{base_path}/kafka/orders/")

# Criar dataset analítico com popularity_score
restaurant_analytics = restaurants_ps[["name", "cuisine_type", "city", "average_rating", "num_reviews", "cnpj"]]
restaurant_analytics["popularity_score"] = restaurant_analytics["average_rating"] * np.sqrt(restaurant_analytics["num_reviews"] / 100)
```

---

## 2. Exportar para Diferentes Formatos

```python
output_dir = "/opt/spark/work/data/output"
spark_df = restaurant_analytics.to_spark()

# CSV
spark_df.write.mode("overwrite").option("header", "true").csv(f"{output_dir}/csv")

# Parquet
spark_df.write.mode("overwrite").parquet(f"{output_dir}/parquet")

# JSON
spark_df.write.mode("overwrite").json(f"{output_dir}/json")
```

| Formato | Uso | Vantagem |
|---------|-----|----------|
| CSV | Compatibilidade | Legível por humanos |
| Parquet | Analytics | Colunar, comprimido |
| JSON | APIs | Estruturado |

---

## 3. Particionamento por Coluna

```python
# Particionar por cuisine_type
spark_df.write.mode("overwrite").partitionBy("cuisine_type").parquet(f"{output_dir}/parquet_partitioned")
```

**Estrutura criada:**
```
parquet_partitioned/
├── cuisine_type=American/
├── cuisine_type=Chinese/
├── cuisine_type=French/
├── cuisine_type=Indian/
├── cuisine_type=Italian/
├── cuisine_type=Japanese/
└── cuisine_type=Mexican/
```

**Benefício:** Consultas filtradas por `cuisine_type` leem apenas a partição necessária.

---

## 4. Criar Resumo Agregado para Relatórios

```python
cuisine_summary = restaurant_analytics.groupby("cuisine_type").agg({
    "average_rating": "mean",
    "num_reviews": "sum",
    "popularity_score": "mean",
    "name": "count"
}).reset_index()

cuisine_summary.columns = ["cuisine_type", "avg_rating", "total_reviews", "avg_popularity", "restaurant_count"]
cuisine_summary = cuisine_summary.sort_values("avg_popularity", ascending=False)

# Exportar resumo
cuisine_summary.to_spark().write.mode("overwrite").option("header", "true").csv(f"{output_dir}/cuisine_summary")
```

**Resultado:**

| cuisine_type | avg_rating | total_reviews | avg_popularity | count |
|--------------|------------|---------------|----------------|-------|
| Mexican | 3.46 | 48,896 | 28.99 | 7 |
| Indian | 3.16 | 64,886 | 20.97 | 13 |
| French | 2.53 | 72,786 | 18.22 | 12 |
| American | 2.67 | 85,489 | 16.50 | 16 |

---

## 5. Estratégias para Datasets Grandes

### Particionamento por Múltiplas Colunas
```python
spark_df.write.mode("overwrite").partitionBy("country", "cuisine_type").parquet(f"{output_dir}/multi_partitioned")
```

### Exportação Incremental por Data
```python
orders_spark.write.mode("overwrite").partitionBy("year", "month").parquet(f"{output_dir}/orders_by_date")
```

### Compressão
```python
spark_df.write.mode("overwrite").option("compression", "gzip").parquet(f"{output_dir}/compressed")
```

---

## 6. Resumo: Escolha do Formato

| Consumidor | Formato | Motivo |
|------------|---------|--------|
| Data Scientists | Parquet | Eficiente para análise |
| Analistas | CSV | Fácil de abrir no Excel |
| APIs | JSON | Estrutura flexível |
| Data Lake | Parquet particionado | Performance em queries |

---

## Best Practices

1. **Parquet para analytics** - colunar, comprimido, eficiente
2. **Particionar por colunas de filtro** - acelera queries
3. **Agregar antes de exportar** - reduz volume
4. **Múltiplos formatos** - atende diferentes consumidores
5. **Compressão** - reduz armazenamento

---

**Conclusão:** A entrega de dados é a etapa final do pipeline. Escolher o formato e particionamento corretos impacta diretamente a performance das análises downstream.
