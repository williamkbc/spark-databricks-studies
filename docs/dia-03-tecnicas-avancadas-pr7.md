# Dia 3 - Tecnicas Avancadas: UDFs e Window Functions (PR-7)

**Data:** 2026-01-30
**Aluno:** Will
**Modulo:** mod-2 (PR-7)

---

## Objetivos Alcancados

- [x] Entender e criar UDFs (User Defined Functions)
- [x] Aplicar Window Functions para ranking
- [x] Usar LAG para comparar com linha anterior
- [x] Compreender particionamento em janelas

---

## Conceitos Aprendidos

### 1. UDFs (User Defined Functions)

**O que sao:** Funcoes customizadas criadas por voce quando as funcoes built-in do Spark nao atendem.

**Quando usar:**

| Situacao | Usar UDF? |
|----------|-----------|
| Soma, media, contagem | Nao - use funcoes built-in |
| Logica de negocio especifica | Sim |
| Categorizacao customizada | Sim |
| Transformacao complexa de texto | Sim |

**Sintaxe:**
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def minha_funcao(valor):
    # logica customizada
    return resultado

# Aplicar
df.withColumn("nova_coluna", minha_funcao(col("coluna_existente")))
```

### 2. Window Functions

**O que sao:** Funcoes que calculam valores "olhando" para outras linhas da tabela, dentro de uma janela definida.

**Diferenca para funcoes normais:**
```
Funcao normal:     [linha1, linha2, linha3] -> 1 resultado
Window Function:   [linha1, linha2, linha3] -> 3 resultados (1 por linha)
```

**Componentes de uma Window:**
- `partitionBy("coluna")` - Divide os dados em grupos (janelas separadas)
- `orderBy("coluna")` - Ordena dentro de cada janela

**Principais funcoes:**

| Funcao | O Que Faz | Uso |
|--------|-----------|-----|
| `rank()` | Ranking com gaps em empates | Top N por categoria |
| `dense_rank()` | Ranking sem gaps | Ranking continuo |
| `row_number()` | Numero sequencial unico | Deduplicacao |
| `lag(col, n)` | Valor da linha n posicoes atras | Comparar com anterior |
| `lead(col, n)` | Valor da linha n posicoes a frente | Comparar com proximo |

---

## Comandos Executados

### Configuracao Inicial

```python
# Entrar no PySpark
docker exec -it spark-master /opt/spark/bin/pyspark --master local[2]

# Imports necessarios
from pyspark.sql.functions import col, count, sum, avg, desc
from pyspark.sql.functions import udf, lag, lead, row_number, rank, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

# Carregar dados
base_path = "/opt/spark/work/data"
restaurants_df = spark.read.json(f"{base_path}/mysql/restaurants/")
print(f"Restaurantes: {restaurants_df.count()}")  # 81
```

### Exemplo 1: Criando uma UDF

**Objetivo:** Categorizar restaurantes pelo rating

```python
@udf(returnType=StringType())
def categorizar_rating(rating):
    if rating is None:
        return "Sem Avaliacao"
    elif rating >= 4.5:
        return "Excelente"
    elif rating >= 4.0:
        return "Muito Bom"
    elif rating >= 3.5:
        return "Bom"
    else:
        return "Regular"

# Aplicar a UDF
restaurants_com_categoria = restaurants_df.withColumn(
    "categoria",
    categorizar_rating(col("average_rating"))
)

restaurants_com_categoria.select("name", "average_rating", "categoria").show(10)
```

**Resultado:**
```
+--------------------+--------------+---------+
|                name|average_rating|categoria|
+--------------------+--------------+---------+
|da Bandeira-Ramei...|           3.1|  Regular|
|Paes S.A. Restaur...|           4.2|Muito Bom|
|Fontes LTDA Resta...|           4.3|Muito Bom|
|Barreto EIRELI Re...|           0.2|  Regular|
|Ornelas, Barreto ...|           4.7|Excelente|
+--------------------+--------------+---------+
```

### Exemplo 2: Window Function - Ranking

**Objetivo:** Rankear restaurantes dentro de cada tipo de culinaria

```python
# Definir a janela
window_cuisine = Window.partitionBy("cuisine_type").orderBy(desc("average_rating"))

# Aplicar o ranking
restaurants_ranked = restaurants_df.withColumn(
    "rank_na_culinaria",
    rank().over(window_cuisine)
)

restaurants_ranked.select(
    "cuisine_type", "name", "average_rating", "rank_na_culinaria"
).orderBy("cuisine_type", "rank_na_culinaria").show(20)
```

**Resultado:**
```
+------------+--------------------+--------------+-----------------+
|cuisine_type|                name|average_rating|rank_na_culinaria|
+------------+--------------------+--------------+-----------------+
|    American|da Fonseca-Taveir...|           4.9|                1|
|    American|Campos EIRELI Res...|           4.9|                1|
|    American|Ornelas, Barreto ...|           4.7|                3|
|    American|Fernandes LTDA Re...|           4.5|                4|
|    American|Galvao, Correa e ...|           4.5|                4|
|     Chinese|da Rosa, da Mota ...|           3.9|                1|
|     Chinese|Pereira LTDA Rest...|           3.5|                2|
+------------+--------------------+--------------+-----------------+
```

**Observacao:** Empates recebem o mesmo rank (dois com 4.9 -> ambos rank 1)

### Exemplo 3: LAG - Comparar com Anterior

**Objetivo:** Ver a diferenca de rating entre um restaurante e o anterior no ranking

```python
restaurants_comparacao = restaurants_ranked.withColumn(
    "rating_anterior",
    lag("average_rating", 1).over(window_cuisine)
).withColumn(
    "diferenca_para_anterior",
    col("average_rating") - col("rating_anterior")
)

restaurants_comparacao.select(
    "cuisine_type", "name", "average_rating",
    "rank_na_culinaria", "rating_anterior", "diferenca_para_anterior"
).filter(col("cuisine_type") == "American").show(10)
```

**Resultado:**
```
+------------+--------------------+--------------+-----------------+---------------+-----------------------+
|cuisine_type|                name|average_rating|rank_na_culinaria|rating_anterior|diferenca_para_anterior|
+------------+--------------------+--------------+-----------------+---------------+-----------------------+
|    American|da Fonseca-Taveir...|           4.9|                1|           NULL|                   NULL|
|    American|Campos EIRELI Res...|           4.9|                1|            4.9|                    0.0|
|    American|Ornelas, Barreto ...|           4.7|                3|            4.9|                   -0.2|
|    American|Costa LTDA Restau...|           3.4|                6|            4.5|                   -1.1|
+------------+--------------------+--------------+-----------------+---------------+-----------------------+
```

**Insights:**
- Primeiro da lista: `rating_anterior = NULL` (nao tem anterior)
- Costa LTDA esta **1.1 pontos** atras do anterior - gap grande!

---

## Padroes Importantes

### Padrao UDF
```python
@udf(returnType=TipoRetorno())
def nome_funcao(parametro):
    # logica
    return resultado

df.withColumn("nova_col", nome_funcao(col("col_existente")))
```

### Padrao Window Function
```python
window = Window.partitionBy("coluna_grupo").orderBy("coluna_ordem")
df.withColumn("nova_col", funcao_window().over(window))
```

---

## Casos de Uso Reais

### UDFs
- Validacao de CPF/CNPJ
- Parsing de enderecos
- Calculo de scores customizados
- Limpeza de dados com regras de negocio

### Window Functions
- **CDC (Change Data Capture):** Identificar versao mais recente de um registro
- **Deduplicacao:** `row_number()` para manter apenas 1 registro por chave
- **Analise temporal:** Comparar vendas mes atual vs anterior com `lag()`
- **Rankings:** Top 10 produtos por categoria

---

## Erros Comuns

| Erro | Causa | Solucao |
|------|-------|---------|
| `AnalysisException: cannot resolve 'rating'` | Nome da coluna errado | Usar `df.printSchema()` para ver nomes corretos |
| UDF retorna None para tudo | Tipo de retorno errado | Verificar `returnType` no decorator |
| Window function sem resultado | Faltou `.over(window)` | Sempre usar `.over(window_spec)` |
| Primeiro registro com NULL em LAG | Comportamento normal | Primeiro nao tem anterior |

---

## Proximos Passos

- PR-8: Partition Operations (repartition, coalesce)
- PR-9: Caching e Persistence
- PR-10: Joins avancados

---

## Metricas do Dia

| Metrica | Valor |
|---------|-------|
| Pratica concluida | PR-7 |
| Conceitos aprendidos | UDF, Window Functions, LAG |
| Comandos executados | ~15 |
| Erros encontrados | 2 (nome coluna, digitacao) |
| Erros resolvidos | 2 |

---

*Documento gerado como parte do curso de formacao Spark e Databricks - Engenharia de Dados Academy*
