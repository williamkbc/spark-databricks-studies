# Dia 26 - Deep Dive Pandas API on Spark (PR-30)

## Arquitetura: Como Funciona

```
ps.DataFrame → Spark DataFrame → Catalyst Optimizer → Execução
     ↑                                                      |
     └──────── pandas_api() ←───────────────────────────────┘
```

## Otimizações Chave

### 1. Switch para Spark em Operações Complexas
```python
# Strings → Spark nativo (3-5x mais rápido)
spark_df = ps_df.to_spark()
spark_df = spark_df.withColumn("col", regexp_replace(col("col"), pattern, ""))
ps_df = spark_df.pandas_api()

# Joins → Broadcast join
spark_df1 = ps_df1.to_spark()
spark_df2 = ps_df2.to_spark()
result = spark_df1.join(broadcast(spark_df2), on="key")
```

### 2. Cache Estratégico
```python
# Sem cache: recomputa a cada ação
ps_df.spark.cache()    # Cache
# ... múltiplas operações ...
ps_df.spark.unpersist() # Liberar
```

### 3. Particionamento Adequado
```python
# Muitas partições em dados pequenos = overhead
ps_df.to_spark().coalesce(5).pandas_api()  # Reduzir sem shuffle
ps_df.to_spark().repartition(10, "key").pandas_api()  # Por chave de join
```

## Pitfalls e Soluções

| Pitfall | Problema | Solução |
|---------|----------|---------|
| `.to_pandas()` grande | OOM | `df.head(N).to_pandas()` |
| `.set_index()` | Shuffle global | Usar `.filter()` em vez de `.loc[]` |
| `.iterrows()` | Row-by-row lento | Operações vetorizadas |
| `.apply()` | Python UDF lento | `when().otherwise()` via Spark |
| Múltiplas ações | Recomputa tudo | `.spark.cache()` |

## Quando Usar vs Não Usar

### Usar Pandas API:
- Migrar código pandas existente
- Data Scientists em transição
- EDA em datasets grandes
- ETL com sintaxe familiar

### Não Usar Pandas API:
- Operações row-by-row intensivas
- Index operations complexas
- Datasets muito pequenos (pandas é mais rápido)
- Comportamento pandas exato necessário

## Integração com Ecossistema

```python
# NumPy
ratings = ps_df['rating'].to_numpy()

# Matplotlib (converter resultado pequeno)
result = ps_df.groupby('col').size().to_pandas()

# scikit-learn (dados pequenos)
pd_df = ps_df[['feat1', 'feat2']].to_pandas()
scaler.fit_transform(pd_df)

# Spark ML (dados grandes)
spark_df = ps_df.to_spark()
assembler = VectorAssembler(inputCols=cols, outputCol="features")
```
