# Dia 27 - Deep Dive Spark SQL (PR-31)

## Plano de Execução

```
SQL Query → Parsed → Analyzed → Optimized (Catalyst) → Physical Plan → Code Gen → Execution
```

```python
# Ver plano
spark.sql("SELECT ...").explain(mode="formatted")  # Resumido
spark.sql("SELECT ...").explain(mode="extended")    # Detalhado
spark.sql("SELECT ...").explain(mode="codegen")     # Código gerado
```

## Join Strategies

| Strategy | Quando | Config |
|----------|--------|--------|
| **Broadcast** | Tabela < 10MB | `autoBroadcastJoinThreshold = 10m` |
| **Shuffle Hash** | Tabelas médias, alta cardinalidade | `preferSortMergeJoin = false` |
| **Sort Merge** | Tabelas grandes | `preferSortMergeJoin = true` (default) |
| **Adaptive** | Deixa Spark decidir | `adaptive.enabled = true` |

```sql
-- Forçar broadcast hint
SELECT /*+ BROADCAST(dim) */ *
FROM fact f JOIN dim d ON f.key = d.key
```

## Técnicas Avançadas

### CUBE e ROLLUP
```sql
-- CUBE: todas combinações possíveis de agrupamento
SELECT cuisine_type, city, COUNT(*)
FROM restaurants
GROUP BY CUBE(cuisine_type, city);

-- ROLLUP: hierárquico (cuisine → city)
SELECT cuisine_type, city, COUNT(*)
FROM restaurants
GROUP BY ROLLUP(cuisine_type, city);
```

### CACHE TABLE
```sql
CACHE TABLE restaurants;          -- Cache em memória
-- ... queries múltiplas ...
UNCACHE TABLE restaurants;        -- Liberar
```

### Window Functions vs Subquery
```sql
-- RUIM: subquery correlacionada
SELECT *, (SELECT COUNT(*) FROM restaurants r2 
           WHERE r2.cuisine = r.cuisine) as total
FROM restaurants r;

-- BOM: window function
SELECT *, COUNT(*) OVER (PARTITION BY cuisine) as total
FROM restaurants;
```

## Otimizações de Produção

### 1. Column Pruning
```sql
-- RUIM: SELECT *
SELECT * FROM restaurants;

-- BOM: só colunas necessárias
SELECT name, cuisine_type, average_rating FROM restaurants;
```

### 2. Predicate Pushdown
```sql
-- Filtrar ANTES do JOIN
SELECT * FROM restaurants r
JOIN orders o ON r.cnpj = o.restaurant_key
WHERE r.average_rating > 4.0;  -- Pushdown automático
```

### 3. Join Ordering
```sql
-- Juntar tabelas menores primeiro
WITH small_result AS (
    SELECT cuisine_type, AVG(rating) as avg
    FROM restaurants GROUP BY cuisine_type  -- Resultado pequeno
)
SELECT * FROM orders o
JOIN small_result sr ON o.cuisine = sr.cuisine_type;
```

## SQL vs DataFrame API

| Critério | SQL | DataFrame API |
|----------|-----|---------------|
| Legibilidade | Melhor para queries | Melhor para ETL complexo |
| Testabilidade | Difícil | Fácil (unit tests) |
| Otimização | Catalyst otimiza bem | Mesmo Catalyst |
| Equipe | Analistas SQL | Devs Python |
| Manutenção | Fácil de alterar | Modular |

## Configs Essenciais para Produção
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "auto")  # AQE ajusta
spark.conf.set("spark.sql.parquet.filterPushdown", "true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning", "true")
```

## Conclusão - Módulo 2 Completo!

### APIs Aprendidas:
1. **PySpark DataFrame** - ETL e processamento geral
2. **Pandas API on Spark** - Data Science distribuído
3. **Spark SQL** - Queries analíticas

### Todas compilam para o MESMO plano via Catalyst Optimizer.

### Próximo: Módulo 3 - Databricks!
