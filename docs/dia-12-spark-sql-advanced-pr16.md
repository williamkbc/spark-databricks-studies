# Dia 12 - Spark SQL: Advanced Transformations (PR-16)

## Objetivo
Transformações avançadas: JOINs complexos, Window Functions, CTEs.

---

## 1. Multi-table JOIN

```sql
SELECT 
    o.order_id,
    r.name AS restaurant,
    r.cuisine_type,
    d.first_name || ' ' || d.last_name AS driver,
    o.total_amount
FROM orders o
JOIN restaurants r ON o.restaurant_key = r.cnpj
JOIN drivers d ON o.driver_key = d.license_number
```

---

## 2. Window Functions - ROW_NUMBER

```sql
SELECT *
FROM (
    SELECT 
        name,
        cuisine_type,
        average_rating,
        ROW_NUMBER() OVER (PARTITION BY cuisine_type ORDER BY average_rating DESC) AS rank
    FROM restaurants
)
WHERE rank <= 3
ORDER BY cuisine_type, rank
```

**Resultado:** Top 3 de cada culinária
| name | cuisine | rating | rank |
|------|---------|--------|------|
| Teles S.A. | Indian | 5.0 | 1 |
| Marins e Associad | Mexican | 5.0 | 1 |
| da Fonseca-Taveir | American | 4.9 | 1 |

---

## 3. CTE (Common Table Expression)

```sql
WITH RestaurantMetrics AS (
    SELECT 
        cuisine_type, name, average_rating, num_reviews,
        ROUND(average_rating * SQRT(num_reviews / 100), 2) AS popularity_score
    FROM restaurants
),
CuisineAvg AS (
    SELECT cuisine_type, ROUND(AVG(popularity_score), 2) AS avg_score
    FROM RestaurantMetrics
    GROUP BY cuisine_type
)
SELECT 
    rm.name,
    rm.cuisine_type,
    rm.popularity_score,
    ca.avg_score AS cuisine_avg,
    ROUND(rm.popularity_score - ca.avg_score, 2) AS diff_from_avg
FROM RestaurantMetrics rm
JOIN CuisineAvg ca ON rm.cuisine_type = ca.cuisine_type
ORDER BY diff_from_avg DESC
```

**Resultado:** Restaurantes que mais superam a média
| name | cuisine | score | avg | diff |
|------|---------|-------|-----|------|
| Teles S.A. | Indian | 49.75 | 20.97 | +28.78 |
| Ornelas, Barreto | American | 39.78 | 16.5 | +23.28 |

---

## Resumo Window Functions

| Função | Uso |
|--------|-----|
| ROW_NUMBER() | Numeração sequencial |
| RANK() | Ranking com gaps |
| DENSE_RANK() | Ranking sem gaps |
| LAG() | Valor da linha anterior |
| LEAD() | Valor da próxima linha |
| SUM() OVER | Soma acumulada |

---

## Sintaxe CTE

```sql
WITH 
NomeCTE1 AS (SELECT ...),
NomeCTE2 AS (SELECT ... FROM NomeCTE1)
SELECT * FROM NomeCTE2
```

**Vantagem:** Queries mais legíveis e reutilizáveis.

---

**Conclusão:** Window Functions e CTEs permitem análises complexas com SQL limpo e eficiente.
