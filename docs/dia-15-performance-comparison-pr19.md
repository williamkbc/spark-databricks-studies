# Dia 15 - Comparação de Performance: PySpark vs Pandas API vs Spark SQL (PR-19)

## Conceito Principal

**Todas as 3 APIs compilam para o MESMO plano de execução** via Catalyst Optimizer. A escolha da API deve ser por legibilidade e familiaridade, não por performance.

## Benchmark Executado

### Operação: Filter + GroupBy + Aggregation

| API | Tempo Médio |
|-----|------------|
| PySpark DataFrame | 0.48s |
| Spark SQL | 0.29s |

Tempos na mesma ordem de grandeza - diferença é overhead de API, não de execução.

## Prova: Planos de Execução Idênticos

### PySpark DataFrame API
```python
restaurants_df.filter("average_rating > 3.0") \
    .groupBy("cuisine_type") \
    .agg(round(avg("average_rating"), 2)) \
    .explain()
```

### Spark SQL
```python
spark.sql("""
    SELECT cuisine_type, ROUND(AVG(average_rating),2)
    FROM restaurants WHERE average_rating > 3.0
    GROUP BY cuisine_type
""").explain()
```

### Resultado: Mesmo Physical Plan
```
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[cuisine_type], functions=[avg(average_rating)])
   +- Exchange hashpartitioning(cuisine_type, 200)
      +- HashAggregate(keys=[cuisine_type], functions=[partial_avg(average_rating)])
         +- Filter (isnotnull(average_rating) AND (average_rating > 3.0))
            +- FileScan json [average_rating, cuisine_type]
```

## Catalyst Optimizer - Fluxo

```
API (PySpark/SQL/Pandas) 
    → Logical Plan (unresolved)
    → Analyzed Logical Plan
    → Optimized Logical Plan (Catalyst)
    → Physical Plan
    → Execution (mesmo para todas as APIs!)
```

## Quando Usar Cada API

| API | Melhor Para |
|-----|------------|
| **PySpark DataFrame** | Pipelines ETL, processamento geral |
| **Pandas API on Spark** | Data Science, quem já sabe pandas |
| **Spark SQL** | Queries analíticas, integração com BI |

## Conclusão

- Performance é praticamente idêntica entre as APIs
- O Catalyst Optimizer otimiza tudo da mesma forma
- Escolha a API pela **legibilidade** e **expertise do time**
- Pode misturar APIs no mesmo projeto sem preocupação
