# Dia 11 - Spark SQL: Basic Transformations (PR-15)

## Objetivo
Transformações básicas com Spark SQL: SELECT, WHERE, ORDER BY, GROUP BY, HAVING.

---

## 1. SELECT com Colunas Computadas e CASE

```sql
SELECT 
    name,
    average_rating,
    num_reviews,
    ROUND(average_rating * SQRT(num_reviews / 100), 2) AS popularity_score,
    CASE
        WHEN average_rating >= 4.5 THEN 'Excelente'
        WHEN average_rating >= 4.0 THEN 'Muito Bom'
        WHEN average_rating >= 3.0 THEN 'Bom'
        ELSE 'Regular'
    END AS categoria
FROM restaurants
ORDER BY popularity_score DESC
```

---

## 2. WHERE com IN e BETWEEN

```sql
SELECT name, cuisine_type, average_rating
FROM restaurants
WHERE cuisine_type IN ('Mexican', 'Japanese', 'Indian')
  AND average_rating BETWEEN 4.0 AND 5.0
ORDER BY average_rating DESC
```

---

## 3. GROUP BY com Múltiplas Agregações

```sql
SELECT 
    cuisine_type,
    COUNT(*) as total,
    ROUND(AVG(average_rating), 2) as avg_rating,
    SUM(num_reviews) as total_reviews
FROM restaurants
GROUP BY cuisine_type
ORDER BY avg_rating DESC
```

---

## 4. HAVING para Filtrar Grupos

```sql
SELECT cuisine_type, COUNT(*) as total
FROM restaurants
GROUP BY cuisine_type
HAVING COUNT(*) >= 5  -- Apenas culinárias com 5+ restaurantes
```

**Diferença WHERE vs HAVING:**
- WHERE: filtra linhas ANTES do GROUP BY
- HAVING: filtra grupos DEPOIS do GROUP BY

---

## 5. JOIN com Agregação

```sql
SELECT 
    r.cuisine_type,
    COUNT(o.order_id) as total_orders,
    ROUND(AVG(o.total_amount), 2) as avg_ticket,
    ROUND(SUM(o.total_amount), 2) as revenue
FROM orders o
JOIN restaurants r ON o.restaurant_key = r.cnpj
GROUP BY r.cuisine_type
ORDER BY revenue DESC
```

**Resultado:**
| cuisine | orders | avg_ticket | revenue |
|---------|--------|------------|---------|
| Japanese | 2 | 75.94 | 151.88 |
| Mexican | 2 | 68.34 | 136.68 |
| French | 3 | 39.83 | 119.49 |

---

## Resumo de Operadores

| Operador | Uso |
|----------|-----|
| IN | Lista de valores |
| BETWEEN | Intervalo |
| LIKE | Padrão de texto |
| IS NULL | Valores nulos |
| CASE WHEN | Condicional |

---

**Conclusão:** Spark SQL permite análises complexas com sintaxe SQL familiar.
