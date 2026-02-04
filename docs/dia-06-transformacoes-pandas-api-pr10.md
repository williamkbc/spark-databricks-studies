# Dia 6 - Transformacoes Basicas com Pandas API (PR-10)

**Data:** 2026-02-04
**Aluno:** Will
**Modulo:** mod-2 (PR-10)

---

## Objetivos Alcancados

- [x] Selecionar colunas especificas
- [x] Filtrar linhas por condicao
- [x] Criar colunas calculadas
- [x] Usar assign() para multiplas colunas
- [x] Aplicar operacoes de string
- [x] Criar categorias com apply()

---

## Conceitos Aprendidos

### 1. Selecao de Colunas

```python
# Sintaxe Pandas - funciona igual no Spark!
detalhes = restaurants_ps[['name', 'cuisine_type', 'city', 'average_rating']]
```

### 2. Filtro de Linhas

```python
# Filtrar por condicao
bons = restaurants_ps[restaurants_ps['average_rating'] > 4.0]
print(f"Restaurantes com rating > 4: {len(bons)}")  # 14
```

### 3. Criar Colunas Calculadas

```python
# Coluna com formula
restaurants_ps['popularity_score'] = np.sqrt(restaurants_ps['num_reviews']) * restaurants_ps['average_rating'] / 5
```

### 4. Multiplas Colunas com assign()

```python
# Criar varias colunas de uma vez
enhanced = restaurants_ps.assign(
    rating_normalizado = restaurants_ps['average_rating'] / 5,
    is_top = restaurants_ps['average_rating'] >= 4.0,
    localizacao = restaurants_ps['city'] + ', ' + restaurants_ps['country']
)
```

### 5. Operacoes com Strings

```python
# Transformar texto
restaurants_ps['name_upper'] = restaurants_ps['name'].str.upper()
restaurants_ps['name_len'] = restaurants_ps['name'].str.len()
```

### 6. Categorizar com apply()

```python
def categorizar(rating):
    if rating >= 4.5:
        return "Excelente"
    elif rating >= 4.0:
        return "Muito Bom"
    elif rating >= 3.0:
        return "Bom"
    else:
        return "Regular"

restaurants_ps['categoria'] = restaurants_ps['average_rating'].apply(categorizar)
```

---

## Comandos Executados

### Carregar Dados

```python
import pyspark.pandas as ps
import numpy as np

base_path = "/opt/spark/work/data"
restaurants_ps = ps.read_json(f"{base_path}/mysql/restaurants/")
orders_ps = ps.read_json(f"{base_path}/kafka/orders/")

print(f"Restaurantes: {len(restaurants_ps)}")  # 81
print(f"Pedidos: {len(orders_ps)}")  # 80
```

### Selecao e Filtro

```python
# Selecionar colunas
detalhes = restaurants_ps[['name', 'cuisine_type', 'city', 'average_rating']]

# Filtrar
bons = restaurants_ps[restaurants_ps['average_rating'] > 4.0]
# Resultado: 14 restaurantes
```

### Criar Popularity Score

```python
restaurants_ps['popularity_score'] = np.sqrt(restaurants_ps['num_reviews']) * restaurants_ps['average_rating'] / 5

# Top 3 por popularidade:
# 1. Teles S.A. - rating 5.0, 9901 reviews, score 99.50
# 2. Goncalves-Leiria - rating 4.3, 8707 reviews, score 80.25
# 3. Ornelas, Barreto - rating 4.7, 7164 reviews, score 79.56
```

---

## Resultados

### Top 10 Restaurantes por Popularidade

| Rank | Restaurante | Rating | Reviews | Score |
|------|-------------|--------|---------|-------|
| 1 | Teles S.A. | 5.0 | 9901 | 99.50 |
| 2 | Goncalves-Leiria | 4.3 | 8707 | 80.25 |
| 3 | Ornelas, Barreto | 4.7 | 7164 | 79.56 |
| 4 | Marins e Associados | 5.0 | 5974 | 77.29 |
| 5 | Muniz-da Silveira | 3.8 | 9894 | 75.60 |

### Distribuicao por Categoria

| Categoria | Criterio |
|-----------|----------|
| Excelente | rating >= 4.5 |
| Muito Bom | rating >= 4.0 |
| Bom | rating >= 3.0 |
| Regular | rating < 3.0 |

---

## Tabela de Referencia: Operacoes Pandas API

| Operacao | Sintaxe | Descricao |
|----------|---------|-----------|
| Selecionar colunas | `df[['col1', 'col2']]` | Escolhe colunas especificas |
| Filtrar linhas | `df[df['col'] > x]` | Filtra por condicao |
| Criar coluna | `df['nova'] = expr` | Adiciona coluna calculada |
| Multiplas colunas | `df.assign(a=x, b=y)` | Cria varias colunas |
| String upper | `df['col'].str.upper()` | Converte para maiusculas |
| String lower | `df['col'].str.lower()` | Converte para minusculas |
| String length | `df['col'].str.len()` | Tamanho do texto |
| String contains | `df['col'].str.contains('x')` | Verifica se contem |
| Apply funcao | `df['col'].apply(func)` | Aplica funcao customizada |
| Sort values | `df.sort_values('col')` | Ordena por coluna |

---

## Comparacao: Pandas API vs PySpark

| Operacao | Pandas API | PySpark |
|----------|------------|---------|
| Selecionar | `df[['col1', 'col2']]` | `df.select('col1', 'col2')` |
| Filtrar | `df[df['col'] > x]` | `df.filter(df.col > x)` |
| Criar coluna | `df['nova'] = x` | `df.withColumn('nova', x)` |
| Ordenar | `df.sort_values('col')` | `df.orderBy('col')` |
| Upper | `df['col'].str.upper()` | `upper(df.col)` |

**Vantagem do Pandas API:** Sintaxe mais familiar para quem vem do Pandas!

---

## Boas Praticas

1. **Use assign() para multiplas colunas** - mais legivel
2. **Prefira operacoes vetorizadas** - mais rapido que apply()
3. **Cuidado com apply()** - pode ser lento em datasets grandes
4. **Sempre verifique tipos** - `df.dtypes` antes de operar

---

## Proximos Passos

- PR-11 a PR-15: Transformacoes avancadas com Pandas API
- Modulo 4: Delta Lake
- Modulo 8: Databricks

---

## Metricas do Dia

| Metrica | Valor |
|---------|-------|
| Pratica concluida | PR-10 |
| Operacoes aprendidas | 10+ |
| Colunas criadas | 6 |
| Restaurantes analisados | 81 |
| Top score encontrado | 99.50 (Teles S.A.) |

---

*Documento gerado como parte do curso de formacao Spark e Databricks - Engenharia de Dados Academy*
