# Dia 17 - Integração Spark com Object Storage MinIO/S3 (PR-21)

## Conceito Principal

Object Storage (S3/MinIO) é a base de Data Lakes modernos. Spark conecta via protocolo **s3a://** usando bibliotecas Hadoop-AWS.

## Configuração S3/MinIO

```python
spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()
```

## Formatos de Armazenamento

| Formato | Tipo | Compressão | Melhor Para |
|---------|------|-----------|-------------|
| **Parquet** | Colunar | Excelente | Analytics (padrão recomendado) |
| **ORC** | Colunar | Muito boa | Ambientes Hive |
| **Avro** | Linhas | Boa | Schema evolution, streaming |
| **JSON** | Texto | Nenhuma | Intercâmbio, debug |

## Escrita em Diferentes Formatos

```python
base = "s3a://ubereats-datalake"

# Parquet (recomendado)
df.write.format("parquet").mode("overwrite").save(f"{base}/parquet/restaurants")

# ORC
df.write.format("orc").mode("overwrite").save(f"{base}/orc/restaurants")

# Com particionamento
df.write.format("parquet") \
    .partitionBy("year", "month") \
    .mode("overwrite") \
    .save(f"{base}/partitioned/orders")
```

## Leitura com Partition Pruning

```python
# Lê APENAS a partição necessária
df = spark.read.parquet(f"{base}/partitioned/orders")
orders_2023 = df.filter(col("year") == 2023)  # Só lê pasta year=2023
```

## Estratégias de Particionamento

| Estratégia | Exemplo | Melhor Para |
|-----------|---------|-------------|
| **Temporal** | year/month/day | Séries temporais |
| **Categórica** | city/cuisine_type | Filtros por categoria |
| **Range** | id ranges | Dados numéricos |
| **Composta** | year/month + city | Queries complexas |

## Data Lake - Arquitetura Medallion no S3

```
s3a://ubereats-datalake/
├── bronze/          ← Dados brutos (Parquet)
│   ├── restaurants/
│   ├── drivers/
│   └── orders/      ← particionado por year/month
├── silver/          ← Dados limpos e transformados
│   ├── restaurants/ ← + rating_category
│   ├── drivers/     ← + full_name
│   └── orders/      ← + day_of_week
└── gold/            ← KPIs prontos para consumo
    ├── restaurant_performance/  ← particionado por cuisine/city
    ├── cuisine_performance/
    ├── driver_performance/
    └── time_analytics/
```

## Performance Tuning

```python
# Configs importantes para S3
.config("spark.hadoop.fs.s3a.connection.maximum", "100")
.config("spark.hadoop.fs.s3a.fast.upload", "true")
.config("spark.hadoop.fs.s3a.multipart.size", "64M")
.config("spark.sql.parquet.filterPushdown", "true")
.config("spark.sql.adaptive.enabled", "true")
```

## Best Practices

1. **Tamanho de arquivo**: 64MB a 1GB por arquivo
2. **Não over-particionar**: evitar muitos arquivos pequenos
3. **Parquet como padrão**: melhor custo-benefício
4. **Partition pruning**: alinhar partições com padrões de query
5. **Bucketing**: para otimizar JOINs frequentes
