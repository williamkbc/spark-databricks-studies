# Dia 25 - Deep Dive & Best Practices PySpark (PR-29)

## Os 5 S's da Otimização Spark

| S | Problema | Solução |
|---|----------|---------|
| **Shuffle** | Dados movendo entre partições | Reduzir shuffle.partitions, broadcast joins |
| **Skew** | Distribuição desigual | Salting, AQE skew join |
| **Spill** | Memória insuficiente | Ajustar memory.fraction |
| **Storage** | Formato ineficiente | Parquet + Snappy |
| **Serialization** | Serialização lenta | Kryo serializer |

## Broadcast Join
```python
from pyspark.sql.functions import broadcast

# Tabela pequena (dimensão) → broadcast
result = orders_df.join(
    broadcast(restaurants_df),  # < 10MB
    on="restaurant_key"
)
```

## Salting para Data Skew
```python
# 1. Identificar chaves com skew
# 2. Adicionar salt ao lado skewed
salted_orders = orders.withColumn("salt", (rand() * 10).cast("int"))
# 3. Explodir lado menor
salted_dim = dim.withColumn("salt", explode(array([lit(i) for i in range(10)])))
# 4. Join com salt + chave original
result = salted_orders.join(salted_dim, ["key", "salt"])
```

## Storage Levels
```python
from pyspark.storagelevel import StorageLevel

df.persist(StorageLevel.MEMORY_ONLY)        # Só memória
df.persist(StorageLevel.MEMORY_AND_DISK)     # Spill para disco
df.persist(StorageLevel.MEMORY_ONLY_SER)     # Serializado (menos RAM)
df.unpersist()                                # Liberar
```

## Repartition vs Coalesce
```python
# Coalesce - REDUZIR partições (sem shuffle)
df = df.coalesce(2)

# Repartition - AUMENTAR partições (com shuffle)
df = df.repartition(20, "join_key")
```

## Pitfalls Comuns

| Pitfall | Ruim | Bom |
|---------|------|-----|
| Coletar dados | `df.collect()` | `df.limit(10).collect()` |
| UDFs lentas | `@udf` Python | `when().otherwise()` built-in |
| Shuffle extra | Múltiplos `groupBy` | Um `groupBy` com múltiplos `agg` |
| Sem cache | Recomputa tudo | `df.cache()` quando reutilizar |

## Debugging
```python
# Ver plano de execução
df.explain(mode="extended")

# Tracker de transformações
def track(df, step):
    print(f"{step}: {df.count()} rows")
    return df

result = df.transform(lambda d: track(d, "step1")) \
    .filter(...) \
    .transform(lambda d: track(d, "step2"))
```

## Configs Essenciais
```python
spark.conf.set("spark.sql.shuffle.partitions", "10")     # Menos partições para dados pequenos
spark.conf.set("spark.sql.adaptive.enabled", "true")      # AQE
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.memory.fraction", "0.8")
```
