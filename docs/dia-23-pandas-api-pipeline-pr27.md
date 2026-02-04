# Dia 23 - Pipeline Pandas API on Spark com Otimizações (PR-27)

## Padrão: Conversão Pandas → Spark → Pandas API

```python
# Operação otimizada: converter para Spark, processar, voltar
spark_df = ps_df.to_spark()
spark_df = spark_df.withColumn("col", regexp_replace(col("col"), pattern, ""))
ps_df = spark_df.pandas_api()
```

## Técnicas de Otimização

### 1. String Operations via Spark Nativo
```python
# Melhor performance que ps_df['col'].str.replace()
spark_df = ps_df.to_spark()
spark_df = spark_df.withColumn("phone",
    regexp_replace(col("phone"), r"[()-\s]", ""))
ps_df = spark_df.pandas_api()
```

### 2. Joins Otimizados
```python
# Em vez de ps.merge(), usar join nativo
left_spark = left_ps.to_spark()
right_spark = right_ps.to_spark()
joined = left_spark.join(right_spark, on="city", how="inner")
result = joined.pandas_api()
```

### 3. Cache
```python
df = ps_df.to_spark().cache().pandas_api()
```

### 4. Reparticionamento
```python
ps_df = ps_df.to_spark().repartition(10).pandas_api()
```

### 5. Pipeline Híbrido
```python
# Tabelas pequenas (dimensão) → Pandas nativo
dim_df = pd.read_json("small.jsonl", lines=True)

# Tabelas grandes (fato) → Pandas API on Spark
fact_df = ps.read_json("large.jsonl", lines=True)

# Converter dimensão para join
dim_ps = spark.createDataFrame(dim_df).pandas_api()
```

## Utilitários de Conversão

```python
def pandas_to_spark_pandas(pandas_df, spark):
    return spark.createDataFrame(pandas_df).pandas_api()

def spark_pandas_to_pandas(ps_df, limit=None):
    return ps_df.head(limit).to_pandas() if limit else ps_df.to_pandas()
```

## Configs Recomendadas

```python
PipelineConfig.SPARK_CONF = {
    "spark.sql.execution.arrow.pyspark.enabled": "true",
    "spark.sql.execution.arrow.maxRecordsPerBatch": "20000",
    "spark.sql.shuffle.partitions": "10",
    "spark.sql.adaptive.enabled": "true"
}

ps.set_option('compute.default_index_type', 'distributed')
ps.set_option('compute.ops_on_diff_frames', True)
```

## Resumo: Quando Usar Cada Abordagem

| Operação | Melhor API |
|----------|-----------|
| String replace | Spark nativo (`regexp_replace`) |
| GroupBy simples | Pandas API (`groupby().agg()`) |
| JOIN grande | Spark nativo (`join()`) |
| Datetime | Spark nativo (`to_timestamp`, `year`) |
| Resultado final | Pandas nativo (`.to_pandas()`) |
| Leitura grande | Pandas API (`ps.read_json()`) |
| Leitura pequena | Pandas nativo (`pd.read_json()`) |
