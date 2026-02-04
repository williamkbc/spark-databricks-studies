# Dia 14 - Spark SQL: Data Delivery (PR-18)

## Conceitos Aprendidos

### 1. Tabelas Permanentes
Tabelas que persistem entre sessões do Spark:

```python
# Criar database
spark.sql("CREATE DATABASE IF NOT EXISTS ubereats_analytics")
spark.sql("USE ubereats_analytics")

# Criar tabela permanente em Parquet
spark.sql("""
    CREATE TABLE IF NOT EXISTS restaurants
    USING PARQUET
    AS SELECT * FROM restaurants
""")

# isTemporary=false = tabela permanente
spark.sql("SHOW TABLES IN ubereats_analytics").show()
```

### 2. Particionamento
Divide dados em diretórios separados para otimizar leitura:

```python
spark.sql("""
    CREATE TABLE IF NOT EXISTS restaurants_partitioned
    USING PARQUET
    PARTITIONED BY (cuisine_type)
    AS SELECT restaurant_id, name, city, average_rating, num_reviews, cuisine_type
    FROM restaurants
""")

# Ver partições criadas
spark.sql("SHOW PARTITIONS restaurants_partitioned").show()

# Partition pruning - Spark lê APENAS a partição filtrada
spark.sql("""
    SELECT name, city, average_rating
    FROM restaurants_partitioned
    WHERE cuisine_type = 'Italian'
""").show()
```

**Vantagem:** Ao filtrar por `cuisine_type = 'Italian'`, Spark ignora todas as outras pastas (Chinese, French, etc.), reduzindo I/O.

### 3. Arquitetura Bronze/Silver/Gold (Medallion)

```
Bronze (dados brutos) → Silver (dados limpos) → Gold (KPIs prontos)
```

```python
# Bronze - dados como recebidos
spark.sql("CREATE DATABASE IF NOT EXISTS ubereats_bronze")
spark.sql("""
    CREATE TABLE ubereats_bronze.restaurants
    USING PARQUET AS SELECT * FROM restaurants
""")

# Silver - dados validados
spark.sql("CREATE DATABASE IF NOT EXISTS ubereats_silver")
spark.sql("""
    CREATE TABLE ubereats_silver.restaurants
    USING PARQUET AS 
    SELECT *, 
        CASE WHEN average_rating > 5 THEN 5.0 
             WHEN average_rating < 0 THEN 0.0 
             ELSE average_rating END AS validated_rating
    FROM ubereats_bronze.restaurants
""")

# Gold - KPIs de negócio
spark.sql("CREATE DATABASE IF NOT EXISTS ubereats_gold")
spark.sql("""
    CREATE TABLE ubereats_gold.restaurant_kpis
    USING PARQUET AS
    SELECT name, cuisine_type, city, validated_rating, num_reviews,
        validated_rating * SQRT(num_reviews / 1000) AS popularity_score,
        CASE 
            WHEN validated_rating >= 4.5 THEN 'Excelente'
            WHEN validated_rating >= 4.0 THEN 'Muito Bom'
            WHEN validated_rating >= 3.0 THEN 'Bom'
            ELSE 'Regular'
        END AS categoria
    FROM ubereats_silver.restaurants
""")
```

## Resumo

| Conceito | Descrição | Quando Usar |
|----------|-----------|-------------|
| Tabela Permanente | Persiste entre sessões | Dados reutilizáveis |
| Particionamento | Divide por coluna em diretórios | Filtros frequentes em coluna específica |
| Bronze | Dados brutos | Primeira ingestão |
| Silver | Dados limpos/validados | Pós validação |
| Gold | KPIs e métricas | Consumo por BI/negócio |

## Comandos Executados
1. Criação de database e tabela permanente em Parquet
2. Tabela particionada por cuisine_type + partition pruning
3. Arquitetura Medallion (Bronze → Silver → Gold) com KPIs
