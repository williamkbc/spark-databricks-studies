# Dia 22 - Pipeline PySpark Completo com Error Handling (PR-26)

## Arquitetura do Pipeline

```
extractors.py → transformers.py → loaders.py
     ↑                ↑                ↑
     └── validators.py + error_handlers.py + logging_utils.py
```

## Componentes Principais

### 1. Error Handling com Decorators
```python
# Decorator para tratar exceções Spark
def handle_spark_exceptions(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except AnalysisException as e:
            raise ProcessingError(f"Spark Analysis Error: {e}")
    return wrapper

# Decorator para retry com backoff
def retry_operation(max_retries=3, delay_seconds=5):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception:
                    if attempt == max_retries: raise
                    time.sleep(delay_seconds * attempt)
        return wrapper
    return decorator
```

### 2. Custom Exceptions
```python
class PipelineError(Exception): pass
class DataQualityError(PipelineError): pass
class SchemaValidationError(PipelineError): pass
class ProcessingError(PipelineError): pass
```

### 3. Validators
```python
def validate_schema(df, expected_schema, name):
    """Valida que DataFrame tem campos esperados"""

def validate_row_count(df, min_rows, name):
    """Valida contagem mínima de registros"""

def validate_no_nulls(df, columns, name):
    """Valida ausência de nulls em colunas críticas"""

def log_data_profile(df, name):
    """Loga perfil básico do DataFrame"""
```

### 4. Extractor com Validação
```python
class DataExtractor:
    @handle_spark_exceptions
    @retry_operation(max_retries=3)
    def extract_restaurants(self):
        df = self.spark.read.json(path)
        validate_schema(df, RESTAURANT_SCHEMA, "restaurants")
        validate_row_count(df, MIN_ROWS, "restaurants")
        log_data_profile(df, "restaurants")
        return df
```

### 5. Transformer com Limpeza
```python
class DataTransformer:
    @handle_spark_exceptions
    def clean_restaurants(self, df):
        return df \
            .withColumn("phone", regexp_replace(col("phone"), "[()\\s-]", "")) \
            .fillna({"cuisine_type": "Unknown", "average_rating": 0.0}) \
            .withColumn("rating_category",
                when(col("average_rating") >= 4.5, "Excellent")
                .when(col("average_rating") >= 4.0, "Very Good")
                .otherwise("Below Average"))
```

### 6. Loader com Retry
```python
class DataLoader:
    @handle_spark_exceptions
    @retry_operation(max_retries=3)
    def save_analysis(self, df, path):
        df.write.mode("overwrite").parquet(path)
```

### 7. Main com Orquestração
```python
def run_pipeline():
    spark = create_spark_session()
    extractor = DataExtractor(spark)
    transformer = DataTransformer(spark)
    loader = DataLoader(spark)
    
    # Extract
    restaurants = extractor.extract_restaurants()
    orders = extractor.extract_orders()
    
    # Transform
    clean_restaurants = transformer.clean_restaurants(restaurants)
    analysis = transformer.analyze_by_cuisine(clean_restaurants)
    
    # Load
    loader.save_analysis(analysis, output_path)
    
    spark.stop()
```

## Padrões Aplicados

| Padrão | Uso |
|--------|-----|
| **Decorator** | Error handling, retry, logging |
| **Custom Exceptions** | Erros específicos do pipeline |
| **Separation of Concerns** | Extract / Transform / Load separados |
| **Fail-Fast** | Validação logo após extração |
| **Retry com Backoff** | Resiliência em operações I/O |

## Best Practices
1. **Validar cedo** - schema e row count logo após read
2. **Logging detalhado** - início/fim de cada job com métricas
3. **Retry automático** - para falhas transientes (I/O, rede)
4. **Exceptions customizadas** - diferenciar tipo de erro
5. **Data profiling** - logar perfil dos dados para auditoria
