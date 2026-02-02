# Dia 4 - Data Delivery: Gravacao e Particionamento (PR-8)

**Data:** 2026-01-30
**Aluno:** Will
**Modulo:** mod-2 (PR-8)

---

## Objetivos Alcancados

- [x] Gravar dados em diferentes formatos (Parquet, CSV, JSON)
- [x] Entender modos de escrita (overwrite, append)
- [x] Aplicar particionamento por coluna
- [x] Compreender partition pruning

---

## Conceitos Aprendidos

### 1. Formatos de Saida

| Formato | Caracteristicas | Quando Usar |
|---------|-----------------|-------------|
| **Parquet** | Colunar, comprimido, eficiente | Data Lakes, analytics, ML |
| **CSV** | Texto plano, universal | Excel, sistemas legados |
| **JSON** | Semi-estruturado | APIs, dados flexiveis |
| **ORC** | Similar ao Parquet | Ecosistema Hive |

**Parquet e o padrao de mercado** para Data Lakes porque:
- Formato colunar (le so as colunas necessarias)
- Compressao eficiente (menor storage)
- Schema embutido (autodocumentado)

### 2. Modos de Escrita

| Modo | Comportamento |
|------|---------------|
| `overwrite` | Substitui dados existentes |
| `append` | Adiciona aos dados existentes |
| `ignore` | Nao faz nada se ja existir |
| `errorifexists` | Erro se ja existir (padrao) |

### 3. Particionamento

**Conceito:** Organizar dados em pastas baseado no valor de uma coluna.

```
/output/restaurants/
├── cuisine_type=American/
│   └── part-00000.parquet
├── cuisine_type=Chinese/
│   └── part-00000.parquet
├── cuisine_type=Italian/
│   └── part-00000.parquet
```

**Vantagem - Partition Pruning:**
Quando voce filtra `WHERE cuisine_type = 'Italian'`, o Spark le **so a pasta Italian**, ignorando as outras. Em datasets grandes, isso faz MUITA diferenca!

---

## Comandos Executados

### Configuracao Inicial

```python
from pyspark.sql.functions import col, round, sqrt
import os

base_path = "/opt/spark/work/data"
output_path = "/opt/spark/work/data/output"

restaurants_df = spark.read.json(f"{base_path}/mysql/restaurants/")

# Criar view com coluna calculada
restaurant_analytics = restaurants_df.select(
    "name", "cuisine_type", "city", "country", "average_rating", "num_reviews"
).withColumn(
    "popularity_score",
    round(col("average_rating") * sqrt(col("num_reviews") / 1000), 2)
)

restaurant_analytics.show(5)
```

### Gravar em Parquet

```python
restaurant_analytics.write.mode("overwrite").parquet(f"{output_path}/restaurants_parquet")
print("Parquet gravado!")
```

### Gravar em CSV

```python
restaurant_analytics.write.mode("overwrite").option("header", "true").csv(f"{output_path}/restaurants_csv")
print("CSV gravado!")
```

### Gravar com Particionamento

```python
restaurant_analytics.write.mode("overwrite").partitionBy("cuisine_type").parquet(f"{output_path}/partitioned_cuisine")
print("Particionado por cuisine_type!")
```

### Verificar Particoes Criadas

```python
print(os.listdir(f"{output_path}/partitioned_cuisine/"))
```

**Resultado:**
```
['_SUCCESS',
 'cuisine_type=Italian',
 'cuisine_type=Japanese',
 'cuisine_type=Chinese',
 'cuisine_type=Indian',
 'cuisine_type=American',
 'cuisine_type=French',
 'cuisine_type=Mexican']
```

### Ler Dados Particionados com Filtro

```python
# Spark le APENAS a pasta cuisine_type=Italian (partition pruning)
italian = spark.read.parquet(f"{output_path}/partitioned_cuisine/").filter(col("cuisine_type") == "Italian")
italian.show()
```

---

## Formula do Popularity Score

```python
popularity_score = average_rating * sqrt(num_reviews / 1000)
```

**Logica:**
- Rating alto + muitas reviews = score alto
- Rating baixo = score baixo (mesmo com muitas reviews)
- Raiz quadrada suaviza o impacto do numero de reviews

**Exemplos:**
| Restaurante | Rating | Reviews | Score |
|-------------|--------|---------|-------|
| Fontes LTDA | 4.3 | 5018 | 9.63 |
| Paes S.A. | 4.2 | 4020 | 8.42 |
| Barreto EIRELI | 0.2 | 6030 | 0.49 |

---

## Boas Praticas

### Quando Particionar

| Situacao | Particionar? |
|----------|--------------|
| Queries sempre filtram por data | Sim, por ano/mes/dia |
| Queries filtram por regiao | Sim, por pais/cidade |
| Coluna com alta cardinalidade (ex: user_id) | Nao - muitas particoes pequenas |
| Dataset pequeno (<1GB) | Geralmente nao |

### Tamanho Ideal de Particao

- **Objetivo:** Arquivos entre 128MB e 1GB
- **Muitos arquivos pequenos** = overhead de metadados
- **Poucos arquivos grandes** = menos paralelismo

### Compressao

```python
# Snappy (padrao) - bom equilibrio velocidade/tamanho
df.write.option("compression", "snappy").parquet(path)

# GZIP - maior compressao, mais lento
df.write.option("compression", "gzip").parquet(path)

# ZSTD - melhor compressao moderna
df.write.option("compression", "zstd").parquet(path)
```

---

## Erros Encontrados

| Erro | Causa | Solucao |
|------|-------|---------|
| `Mkdirs failed to create file` | Permissao negada | `chmod -R 777` no diretorio |
| `NameError: name not defined` | Reiniciou PySpark | Recarregar variaveis |
| Java stack traces em writes | Warnings, nao erros | Ignorar se "gravado!" aparecer |

---

## Arquitetura de Data Lake

```
Data Lake (Medallion Architecture)
├── bronze/          # Dados brutos (JSON, CSV)
├── silver/          # Dados limpos e validados
└── gold/            # Dados agregados para consumo
    └── partitioned by date/region
```

O particionamento e tipicamente aplicado nas camadas **silver** e **gold**.

---

## Proximos Passos

- PR-9 a PR-15: Transformacoes avancadas
- Modulo 4: Delta Lake
- Modulo 8: Databricks

---

## Metricas do Dia

| Metrica | Valor |
|---------|-------|
| Pratica concluida | PR-8 |
| Formatos aprendidos | 4 (Parquet, CSV, JSON, ORC) |
| Particoes criadas | 7 (por cuisine_type) |
| Erros encontrados | 2 |
| Erros resolvidos | 2 |

---

*Documento gerado como parte do curso de formacao Spark e Databricks - Engenharia de Dados Academy*
