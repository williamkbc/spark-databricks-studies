# Dia 2 - PySpark: Ingestao e Transformacoes

**Data:** 2026-01-24
**Aluno:** Will
**Modulo:** mod-2 (PR-4 a PR-6)

---

## Objetivos Alcancados

- [x] Carregar dados de diferentes fontes (JSON, Parquet)
- [x] Aplicar transformacoes basicas (select, filter, withColumn, drop, orderBy)
- [x] Aplicar transformacoes complexas (groupBy, aggregations, distinct, JOIN)
- [x] Entender schema inference vs schema explicito
- [x] Gravar dados em formato Parquet

---

## PR-4: Ingestao de Dados

### Conceitos Aprendidos

| Conceito | Descricao |
|----------|-----------|
| **SparkSession** | Ponto de entrada para PySpark, gerencia configuracoes e conexoes |
| **DataFrame** | Estrutura de dados distribuida com schema definido |
| **Schema Inference** | Spark infere tipos automaticamente ao ler dados |
| **Lazy Evaluation** | Transformacoes nao sao executadas ate uma action ser chamada |

### Comandos Executados

```python
# Iniciar PySpark Shell
docker exec -it spark-master pyspark --master local[2]

# Carregar JSON com schema inferido
df = spark.read.json("/opt/spark/work/storage/mysql/restaurants/")
df.printSchema()
df.show(5)
df.count()  # 81 restaurantes

# Gravar em Parquet
df.write.mode("overwrite").parquet("/opt/spark/work/storage/output/restaurants_parquet")

# Ler Parquet
df_parquet = spark.read.parquet("/opt/spark/work/storage/output/restaurants_parquet")
df_parquet.count()  # 81 restaurantes
```

### Resultado

```
root
 |-- city: string
 |-- country: string
 |-- cuisine_type: string
 |-- id: long
 |-- name: string
 |-- owner_name: string
 |-- phone: string
 |-- rating: double
 |-- review_count: long

Total: 81 restaurantes carregados
```

---

## PR-5: Transformacoes Basicas

### Operacoes Aprendidas

| Operacao | Sintaxe | Descricao |
|----------|---------|-----------|
| **select** | `df.select("col1", "col2")` | Seleciona colunas especificas |
| **filter** | `df.filter(df.col > valor)` | Filtra linhas por condicao |
| **withColumnRenamed** | `df.withColumnRenamed("old", "new")` | Renomeia coluna |
| **withColumn** | `df.withColumn("nova", expr)` | Adiciona/modifica coluna |
| **drop** | `df.drop("col")` | Remove coluna |
| **orderBy** | `df.orderBy(df.col.desc())` | Ordena dados |

### Comandos Executados

```python
from pyspark.sql.functions import col, upper, when

# SELECT - escolher colunas
df.select("name", "cuisine_type", "rating").show(5)

# FILTER - filtrar por condicao
df.filter(df.rating >= 4.5).show()
df.filter((df.rating >= 4.0) & (df.cuisine_type == "Italian")).show()

# RENAME - renomear coluna
df.withColumnRenamed("cuisine_type", "tipo_culinaria").printSchema()

# ADD COLUMN - adicionar coluna calculada
df.withColumn("rating_category",
    when(df.rating >= 4.5, "Excelente")
    .when(df.rating >= 4.0, "Bom")
    .otherwise("Regular")
).select("name", "rating", "rating_category").show()

# TRANSFORM - transformar valores
df.withColumn("name_upper", upper(df.name)).select("name", "name_upper").show(5)

# DROP - remover coluna
df.drop("phone", "owner_name").printSchema()

# ORDER BY - ordenar
df.orderBy(df.rating.desc()).select("name", "rating").show(10)
df.orderBy(col("review_count").desc(), col("rating").desc()).show(5)
```

### Resultado

```
Top 5 por Rating:
+--------------------+------+
|                name|rating|
+--------------------+------+
|The Flavorful Fork|   4.9|
|   The Rustic Table|   4.9|
|   The Cozy Kitchen|   4.8|
...
```

---

## PR-6: Transformacoes Complexas

### Operacoes Aprendidas

| Operacao | Sintaxe | Descricao |
|----------|---------|-----------|
| **groupBy** | `df.groupBy("col").agg(...)` | Agrupa dados por coluna |
| **count()** | `agg(count("*"))` | Conta registros por grupo |
| **avg()** | `agg(avg("col"))` | Media por grupo |
| **sum()** | `agg(sum("col"))` | Soma por grupo |
| **distinct** | `df.select("col").distinct()` | Valores unicos |
| **join** | `df1.join(df2, "col", "inner")` | Combina DataFrames |

### Comandos Executados

```python
from pyspark.sql.functions import count, avg, sum, col

# GROUP BY com agregacoes
df.groupBy("cuisine_type").agg(
    count("*").alias("total_restaurants"),
    avg("rating").alias("avg_rating"),
    sum("review_count").alias("total_reviews")
).orderBy(col("total_restaurants").desc()).show()

# GROUP BY multiplas colunas
df.groupBy("cuisine_type", "country").agg(
    count("*").alias("total")
).show()

# DISTINCT - valores unicos
df.select("city").distinct().count()  # 81 cidades unicas

# JOIN - criar dataset auxiliar
top_cuisines = df.groupBy("cuisine_type").agg(
    count("*").alias("total")
).filter(col("total") >= 15)

# INNER JOIN
df.join(top_cuisines, "cuisine_type", "inner").show()

# LEFT JOIN
df.join(top_cuisines, "cuisine_type", "left").show()
```

### Resultado

```
Restaurantes por Tipo de Culinaria:
+------------+-----------------+------------------+-------------+
|cuisine_type|total_restaurants|        avg_rating|total_reviews|
+------------+-----------------+------------------+-------------+
|     Italian|               14|4.2642857142857145|         3890|
|    Japanese|               13|4.2538461538461535|         3234|
|     Mexican|               12| 4.308333333333333|         3012|
|     Chinese|               11|4.2636363636363635|         2512|
|      Indian|               11| 4.236363636363637|         2987|
|      French|               10|              4.31|         2789|
|        Thai|               10|4.2599999999999998|         2456|
+------------+-----------------+------------------+-------------+

Cidades unicas: 81
Pais: todos BR (Brazil)
```

---

## Comparacao: Transformacoes Basicas vs Complexas

| Aspecto | Basicas | Complexas |
|---------|---------|-----------|
| **Escopo** | Linha a linha | Grupos de linhas |
| **Operacoes** | select, filter, withColumn | groupBy, join, distinct |
| **Shuffle** | Minimo | Intensivo (redistribui dados) |
| **Performance** | Rapido | Pode ser lento com dados grandes |
| **Uso** | Limpeza, filtros | Agregacoes, relatorios |

---

## Erros Comuns e Solucoes

| Erro | Causa | Solucao |
|------|-------|---------|
| `AnalysisException: cannot resolve column` | Nome de coluna errado | Verificar com `df.printSchema()` |
| `TypeError: Column is not iterable` | Usar `==` ao inves de `=` | `df.filter(df.col == valor)` |
| `Py4JJavaError: OutOfMemory` | Dados muito grandes | Usar `repartition()` ou aumentar memoria |

---

## Boas Praticas

1. **Sempre verificar schema antes de transformar**
   ```python
   df.printSchema()
   df.show(5)
   ```

2. **Usar alias para clareza**
   ```python
   .agg(count("*").alias("total"))  # Melhor que count(1)
   ```

3. **Encadear transformacoes**
   ```python
   df.filter(...).select(...).orderBy(...)  # Legivel
   ```

4. **Preferir filter antes de groupBy**
   ```python
   df.filter(df.rating >= 4.0).groupBy("cuisine_type")  # Mais eficiente
   ```

---

## Proximos Passos (PR-7+)

1. **Window Functions** - LAG, LEAD, ROW_NUMBER
2. **Pivot/Unpivot** - Transformar linhas em colunas
3. **UDFs** - Funcoes customizadas
4. **Joins Avancados** - broadcast, anti-join

---

## Metricas do Dia

| Metrica | Valor |
|---------|-------|
| Praticas concluidas | 3 (PR-4, PR-5, PR-6) |
| Registros processados | 81 restaurantes |
| Transformacoes aprendidas | 12+ |
| Agregacoes executadas | 5 tipos |
| Arquivos Parquet criados | 1 |

---

*Documento gerado como parte do curso de formacao Spark e Databricks - Engenharia de Dados Academy*
