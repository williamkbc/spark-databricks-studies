# Dia 19 - Pipeline End-to-End com PySpark (PR-23)

## Estrutura do Projeto

```
src/app/
├── main.py               # Entry point
├── pipeline.py           # Lógica ETL
├── config.py             # Configurações
└── utils/
    ├── transformers.py   # Funções de transformação
    └── validators.py     # Funções de validação
tests/
└── test_pipeline.py      # Testes unitários
```

## Componentes

### Config (config.py)
```python
class AppConfig:
    USERS_PATH = "./storage/mongodb/users/..."
    RESTAURANTS_PATH = "./storage/mysql/restaurants/..."
    ORDERS_PATH = "./storage/kafka/orders/..."
    OUTPUT_DIR = "./output"
    SPARK_EXECUTOR_MEMORY = "2g"
    SPARK_DRIVER_MEMORY = "4g"
```

### Transformers (utils/transformers.py)
```python
def clean_phone_numbers(df, col_name):
    return df.withColumn(col_name,
        F.regexp_replace(F.col(col_name), r'[()+-]', ''))

def convert_timestamps(df, col_name):
    return df.withColumn(col_name,
        F.to_timestamp(F.col(col_name)))
```

### Validators (utils/validators.py)
```python
def validate_no_nulls(df, columns):
    for col in columns:
        if df.filter(F.col(col).isNull()).count() > 0:
            return False
    return True
```

### Pipeline (pipeline.py)
```python
class UberEatsPipeline:
    def __init__(self, spark):
        self.spark = spark
    
    def extract(self):
        users_df = self.spark.read.json(USERS_PATH)
        restaurants_df = self.spark.read.json(RESTAURANTS_PATH)
        orders_df = self.spark.read.json(ORDERS_PATH)
        return users_df, restaurants_df, orders_df
    
    def transform(self, users_df, restaurants_df, orders_df):
        users_df = clean_phone_numbers(users_df, "phone_number")
        orders_df = convert_timestamps(orders_df, "order_date")
        validate_no_nulls(users_df, ["user_id", "email"])
        # aggregations...
        return results
    
    def load(self, *dataframes):
        for df in dataframes:
            df.write.mode("overwrite").parquet(output_path)
    
    def run(self):
        data = self.extract()
        transformed = self.transform(*data)
        self.load(*transformed)
```

### Testes (test_pipeline.py)
```python
@pytest.fixture
def spark():
    return SparkSession.builder \
        .appName("Test") \
        .master("local[1]") \
        .getOrCreate()

def test_clean_phone_numbers(spark):
    data = [(1, "(51) 4463-9821")]
    df = spark.createDataFrame(data, ["id", "phone"])
    result = clean_phone_numbers(df, "phone")
    assert "51 44639821" in [r.phone for r in result.collect()]

def test_validate_no_nulls(spark):
    data = [(1, "test"), (2, None)]
    df = spark.createDataFrame(data, ["id", "name"])
    assert validate_no_nulls(df, ["name"]) == False
```

## Fluxo de Execução

```
main.py → create_spark_session()
       → UberEatsPipeline(spark)
       → pipeline.run()
           → extract() → read JSON files
           → transform() → clean, validate, aggregate
           → load() → write Parquet
       → spark.stop()
```

## Best Practices

1. **Modularidade** - Extract, Transform, Load separados
2. **Validação** - Checar nulls e qualidade antes de processar
3. **Testes** - pytest com SparkSession local
4. **Config externa** - Paths e configs em classe separada
5. **Error handling** - try/except no main com spark.stop() no finally
6. **Logging** - Métricas de qualidade em cada etapa
