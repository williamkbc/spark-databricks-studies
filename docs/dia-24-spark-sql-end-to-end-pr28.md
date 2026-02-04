# Dia 24 - Pipeline End-to-End com Spark SQL (PR-28)

## Arquitetura: SQL Modular

```
sql/
├── create_views.sql    # Views de limpeza
├── restaurants.sql     # Análise restaurantes
├── users.sql           # Análise usuários
├── orders.sql          # Análise pedidos
└── delivery.sql        # Análise entregas
```

## Execução de SQL Files

```python
class SparkSQLPipeline:
    def _execute_sql_script(self, file_path):
        sql_content = open(file_path).read()
        statements = [s.strip() for s in sql_content.split(';') if s.strip()]
        for stmt in statements:
            self.spark.sql(stmt)

    def run_pipeline(self):
        self.create_views()       # Limpa dados
        self.analyze_restaurants() # CTE + Window
        self.analyze_orders()      # Temporal
        self.analyze_delivery()    # JOINs complexos
```

## Queries Avançadas

### Ranking por Cuisine (CTE + Window)
```sql
WITH ranked AS (
    SELECT *, RANK() OVER (
        PARTITION BY cuisine_type 
        ORDER BY average_rating DESC
    ) as rank
    FROM restaurants
)
SELECT * FROM ranked WHERE rank <= 3;
```

### Análise Temporal
```sql
CREATE VIEW orders_with_time AS
SELECT *, YEAR(order_date) as year,
    MONTH(order_date) as month,
    DAYOFWEEK(order_date) as day_of_week
FROM orders;
```

### Distribuição de Ratings
```sql
SELECT rating_category,
    COUNT(*) as total,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM restaurants), 2) as pct
FROM (
    SELECT CASE
        WHEN average_rating >= 4.5 THEN 'Excellent'
        WHEN average_rating >= 4.0 THEN 'Very Good'
        ELSE 'Other'
    END as rating_category
    FROM restaurants
)
GROUP BY rating_category;
```

## Vantagens Pipeline SQL

1. **Legibilidade** - SQL é universal
2. **Otimização** - Catalyst otimiza automaticamente
3. **Manutenção** - Fácil alterar queries
4. **Acessibilidade** - Analistas podem contribuir
