# Dia 16 - Integração Spark com PostgreSQL via JDBC (PR-20)

## Conceito Principal

Spark se conecta a bancos relacionais via **JDBC**, permitindo ler e escrever dados diretamente. O driver JDBC traduz operações Spark em queries SQL no banco.

## Configuração

```python
# JAR do driver PostgreSQL necessário
spark = SparkSession.builder \
    .config("spark.jars", "./lib/postgresql-42.5.0.jar") \
    .getOrCreate()

# Parâmetros de conexão
jdbc_url = "jdbc:postgresql://localhost:5432/ubereats"
props = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}
```

## Leitura de Dados

### Leitura Básica
```python
df = spark.read.jdbc(url=jdbc_url, table="restaurants", properties=props)
```

### Predicate Pushdown (filtro executado NO BANCO)
```python
df = spark.read.jdbc(
    url=jdbc_url, table="restaurants",
    properties=props,
    predicates=["average_rating > 4.0"]
)
```
**Vantagem:** O filtro roda no PostgreSQL, menos dados trafegam pela rede.

### Leitura Paralela com Particionamento
```python
df = spark.read.jdbc(
    url=jdbc_url, table="restaurants",
    properties=props,
    column="restaurant_id",    # Coluna numérica para particionar
    lowerBound=1,              # Valor mínimo
    upperBound=1000,           # Valor máximo
    numPartitions=4            # Número de leituras paralelas
)
```
**Resultado:** Spark faz 4 queries simultâneas, cada uma lendo um range de IDs.

### Query SQL Customizada
```python
query = "(SELECT name, cuisine_type, AVG(rating) as avg_rating FROM restaurants GROUP BY name, cuisine_type) AS subquery"
df = spark.read.jdbc(url=jdbc_url, table=query, properties=props)
```

## Escrita de Dados

### Escrita Básica
```python
df.write.jdbc(url=jdbc_url, table="restaurants_spark", mode="overwrite", properties=props)
```

### Modos de Escrita
| Modo | Comportamento |
|------|--------------|
| `overwrite` | Recria a tabela |
| `append` | Adiciona registros |
| `ignore` | Ignora se tabela existe |
| `error` | Erro se tabela existe |

### Bulk Loading (melhor performance)
```python
bulk_props = props.copy()
bulk_props["batchsize"] = "10000"
bulk_props["reWriteBatchedInserts"] = "true"
df.write.jdbc(url=jdbc_url, table="table", mode="overwrite", properties=bulk_props)
```

## Best Practices

1. **Predicate Pushdown** - sempre filtrar no banco quando possível
2. **Column Selection** - selecionar só colunas necessárias
3. **Partitioned Read** - paralelizar leitura de tabelas grandes
4. **Batch Size** - ajustar batchsize para escrita eficiente
5. **Connection Limits** - respeitar limites do PostgreSQL
6. **Credenciais** - nunca hardcoded, usar variáveis de ambiente
