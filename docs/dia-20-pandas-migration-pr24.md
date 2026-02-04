# Dia 20 - Migração Pandas → Pandas API on Spark (PR-24)

## Conceito Principal

Pandas API on Spark permite usar **mesma sintaxe pandas** em ambiente distribuído. A migração é quase 1:1.

## Comparação de Código

### Pandas (single machine)
```python
import pandas as pd
df = pd.read_json("data.jsonl", lines=True)
result = df.groupby('cuisine_type')['average_rating'].mean()
```

### Pandas API on Spark (distribuído)
```python
import pyspark.pandas as ps
df = ps.read_json("data.jsonl", lines=True)
result = df.groupby('cuisine_type')['average_rating'].mean()
# Mesma sintaxe!
```

## Técnicas de Otimização

```python
# 1. Cache DataFrames reutilizados
df = df.spark.cache()

# 2. Selecionar colunas cedo (menos memória)
df_slim = df[['id', 'cuisine_type', 'rating']]

# 3. Index distribuído para datasets grandes
ps.set_option('compute.default_index_type', 'distributed')

# 4. Converter para Spark DF quando necessário
spark_df = df.to_spark()
# ... operações Spark nativas ...
result = spark_df.pandas_api()  # volta para Pandas API

# 5. Reparticionamento inteligente
df = df.spark.repartition(4)
```

## Debugging

```python
# 1. Sample pequeno para inspecionar
sample = df.head(5).to_pandas()

# 2. Checar partições
n = df.to_spark().rdd.getNumPartitions()

# 3. Ver plano de execução
df.to_spark().explain()

# 4. Validar incrementalmente (operação por operação)
# 5. Logging estruturado em cada etapa
```

## Diferenças Críticas

| Feature | Pandas | Pandas API on Spark |
|---------|--------|---------------------|
| Execução | Eager (imediata) | Lazy (sob demanda) |
| Memória | Single machine | Distribuída |
| Tamanho | GBs | TBs+ |
| Index | Sempre disponível | Custo de performance |
| UDFs | apply() fácil | Usar Spark UDFs |

## Pitfalls Comuns

1. `.to_pandas()` em datasets grandes = **OOM** (Out of Memory)
2. Operações row-by-row = lento (usar vetorizado)
3. Não cachear = recomputa tudo a cada ação
4. Shuffles excessivos = lentidão
5. Muitas partições pequenas = overhead

## Fluxo de Migração

```
1. Trocar import: pd → ps
2. Testar com dataset pequeno
3. Ajustar operações não suportadas
4. Otimizar (cache, partições, colunas)
5. Monitorar via Spark UI
```
