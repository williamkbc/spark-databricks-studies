# Dia 13 - Spark SQL: Técnicas Avançadas (PR-17)

## Conceitos Aprendidos

### 1. UDFs (User Defined Functions)
Funções customizadas registradas no Spark SQL:

```python
from pyspark.sql.types import StringType

spark.udf.register("rating_category", 
    lambda r: "Excelente" if r >= 4.5 else
              "Muito Bom" if r >= 4.0 else
              "Bom" if r >= 3.5 else
              "Regular",
    StringType())

# Uso em SQL
spark.sql("""
    SELECT name, average_rating, rating_category(average_rating) as categoria
    FROM restaurants
    ORDER BY average_rating DESC
    LIMIT 10
""").show()
```

### 2. Funções Analíticas

#### PERCENTILE e STDDEV
```python
spark.sql("""
    SELECT 
        cuisine_type,
        COUNT(*) as total,
        ROUND(AVG(average_rating), 2) as media,
        ROUND(STDDEV(average_rating), 2) as desvio_padrao,
        PERCENTILE(average_rating, 0.5) as mediana,
        PERCENTILE(average_rating, 0.25) as q1,
        PERCENTILE(average_rating, 0.75) as q3
    FROM restaurants
    GROUP BY cuisine_type
    ORDER BY total DESC
""").show()
```

**Funções disponíveis:**
- `STDDEV()` - Desvio padrão
- `VARIANCE()` - Variância
- `PERCENTILE(col, p)` - Percentil (p entre 0 e 1)
- `PERCENTILE_APPROX()` - Percentil aproximado (mais eficiente)

### 3. PIVOT - Transformação de Linhas em Colunas

```python
spark.sql("""
    SELECT * FROM (
        SELECT city, cuisine_type, average_rating
        FROM restaurants
    )
    PIVOT (
        AVG(average_rating)
        FOR cuisine_type IN ('Italian', 'French', 'Japanese', 'Chinese')
    )
    ORDER BY Italian DESC NULLS LAST
    LIMIT 10
""").show()
```

**Estrutura PIVOT:**
```sql
SELECT * FROM tabela
PIVOT (
    funcao_agregacao(coluna_valor)
    FOR coluna_pivot IN (valor1, valor2, ...)
)
```

## Resumo

| Técnica | Uso | Exemplo |
|---------|-----|---------|
| UDF | Lógica customizada | Categorização de ratings |
| PERCENTILE | Análise estatística | Mediana, quartis |
| STDDEV | Dispersão dos dados | Variabilidade de ratings |
| PIVOT | Transpor dados | Cidades vs tipos de cozinha |

## Comandos Executados
1. Registro de UDF para categorização
2. Análise com PERCENTILE e STDDEV por tipo de cozinha
3. PIVOT de ratings por cidade e tipo de cozinha
