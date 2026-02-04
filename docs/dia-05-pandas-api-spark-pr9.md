# Dia 5 - Pandas API on Spark (PR-9)

**Data:** 2026-02-04
**Aluno:** Will
**Modulo:** mod-2 (PR-9)

---

## Objetivos Alcancados

- [x] Entender o que e Pandas API on Spark
- [x] Importar e usar `pyspark.pandas`
- [x] Carregar dados com sintaxe Pandas
- [x] Executar operacoes Pandas no Spark
- [x] Converter entre Pandas API e PySpark DataFrame

---

## O Que e Pandas API on Spark?

**Problema:** Pandas e otimo, mas nao escala para Big Data (limite = RAM da maquina)

**Solucao:** Pandas API on Spark = Sintaxe do Pandas + Poder do Spark

| Pandas Nativo | Pandas API on Spark |
|---------------|---------------------|
| Roda em 1 maquina | Roda distribuido |
| Limite: RAM | Escala para TB |
| `import pandas as pd` | `import pyspark.pandas as ps` |

---

## Conceitos Aprendidos

### 1. Import e Configuracao

```python
# Pandas API on Spark
import pyspark.pandas as ps

# Pandas nativo (para comparacao)
import pandas as pd
```

**Convencao:**
- `ps` = Pandas on Spark
- `pd` = Pandas nativo

### 2. Comparacao de Sintaxe

| Operacao | Pandas/Pandas API | PySpark |
|----------|-------------------|---------|
| Contar linhas | `len(df)` | `df.count()` |
| Primeiras linhas | `df.head()` | `df.show()` |
| Estatisticas | `df.describe()` | `df.describe().show()` |
| Filtrar | `df[df['col'] > x]` | `df.filter(df.col > x)` |
| Agrupar | `df.groupby('col').mean()` | `df.groupBy('col').agg(avg())` |
| Shape | `df.shape` | `(df.count(), len(df.columns))` |
| Tipos | `df.dtypes` | `df.printSchema()` |

### 3. Conversao Entre Formatos

```python
# Pandas API -> PySpark DataFrame
spark_df = ps_df.to_spark()

# PySpark DataFrame -> Pandas API
ps_df = ps.DataFrame(spark_df)

# Pandas API -> Pandas Nativo (CUIDADO: so para dados pequenos!)
pd_df = ps_df.to_pandas()

# Pandas Nativo -> Pandas API
ps_df = ps.from_pandas(pd_df)
```

---

## Comandos Executados

### Instalacao do Pandas no Container

```bash
docker exec -u root spark-master pip install pandas pyarrow
```

### Carregar Dados com Sintaxe Pandas

```python
import pyspark.pandas as ps

base_path = "/opt/spark/work/data"

# Sintaxe IGUAL ao Pandas!
restaurants_ps = ps.read_json(f"{base_path}/mysql/restaurants/")

# len() em vez de count()
print(f"Restaurantes: {len(restaurants_ps)}")  # 81

# head() em vez de show()
restaurants_ps.head()
```

### Explorar Dados

```python
# Shape, colunas, tipos - igual Pandas!
print(f"Shape: {restaurants_ps.shape}")
print(f"Colunas: {restaurants_ps.columns.tolist()}")
print(f"Tipos: \n{restaurants_ps.dtypes}")
```

### Converter para PySpark

```python
# Pandas API -> PySpark
restaurants_spark = restaurants_ps.to_spark()

print(f"Count: {restaurants_spark.count()}")  # 81
restaurants_spark.printSchema()
```

### Filtro com Sintaxe Pandas

```python
bons_restaurantes = restaurants_ps[restaurants_ps['average_rating'] >= 4.0]
print(f"Restaurantes com rating >= 4.0: {len(bons_restaurantes)}")  # 16
```

### Estatisticas Descritivas

```python
restaurants_ps['average_rating'].describe()
```

**Resultado:**
```
count    81.000000
mean      2.543210
std       1.409516
min       0.000000
25%       1.300000
50%       2.700000
75%       3.600000
max       5.000000
```

### GroupBy com Sintaxe Pandas

```python
por_culinaria = restaurants_ps.groupby('cuisine_type')['average_rating'].mean()
print(por_culinaria.sort_values(ascending=False))
```

**Resultado - Ranking por Culinaria:**
```
cuisine_type
Mexican     3.457143
Indian      3.161538
American    2.668750
French      2.525000
Japanese    2.457143
Chinese     2.138462
Italian     1.746154
```

---

## Tipos de Dados: Pandas vs Spark

| Pandas | Spark | Descricao |
|--------|-------|-----------|
| `float64` | `double` | Numero decimal |
| `int64` | `long` | Numero inteiro |
| `object` | `string` | Texto |
| `bool` | `boolean` | Verdadeiro/Falso |
| `datetime64` | `timestamp` | Data e hora |

---

## Quando Usar Cada Abordagem?

| Situacao | Recomendacao |
|----------|--------------|
| Ja sabe Pandas + dados grandes | **Pandas API on Spark** |
| Precisa de mais controle/performance | PySpark nativo |
| Dados pequenos (<1GB) | Pandas nativo |
| Time de Data Science | Pandas API on Spark |
| Time de Data Engineering | PySpark nativo |
| Prototipacao rapida | Pandas API on Spark |
| Pipeline de producao | PySpark nativo |

---

## Vantagens e Desvantagens

### Vantagens
- Curva de aprendizado zero para quem sabe Pandas
- Mesma sintaxe funciona para dados pequenos e grandes
- Facil migracao de codigo Pandas existente
- Integracao com ecossistema Pandas (visualizacoes, etc)

### Desvantagens
- Performance ligeiramente inferior ao PySpark nativo
- Nem todas as funcoes do Pandas estao disponiveis
- Alguns comportamentos diferem (ex: indice distribuido)
- Warnings podem ser confusos para iniciantes

---

## Erros Encontrados

| Erro | Causa | Solucao |
|------|-------|---------|
| `ModuleNotFoundError: No module named 'pandas'` | Pandas nao instalado | `pip install pandas pyarrow` |
| `PYARROW_IGNORE_TIMEZONE warning` | Configuracao de timezone | Ignorar (nao afeta funcionamento) |
| Comando Python no bash | Confundiu terminal | Entrar no PySpark primeiro |

---

## Proximos Passos

- PR-10 a PR-15: Mais transformacoes com Pandas API
- Modulo 4: Delta Lake
- Modulo 8: Databricks

---

## Metricas do Dia

| Metrica | Valor |
|---------|-------|
| Pratica concluida | PR-9 |
| Pacotes instalados | pandas, pyarrow |
| Operacoes Pandas executadas | 6+ |
| Conversoes entre formatos | 2 |
| Restaurantes analisados | 81 |

---

*Documento gerado como parte do curso de formacao Spark e Databricks - Engenharia de Dados Academy*
