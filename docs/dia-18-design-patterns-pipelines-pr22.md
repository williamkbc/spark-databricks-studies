# Dia 18 - Design Patterns para Data Pipelines (PR-22)

## Estrutura de Projeto Recomendada

```
/src/app/
├── config/config.yaml    # Configurações por ambiente
├── core/
│   ├── session.py        # Singleton Spark Session
│   └── pipeline.py       # Classe base ETL
├── etl/
│   ├── extract.py        # Extractors (Strategy)
│   ├── transform.py      # Transformers (Strategy)
│   ├── load.py           # Loaders (Strategy)
│   └── factory.py        # Factory de componentes
├── utils/
│   ├── config.py         # Loader de config YAML
│   └── logger.py         # Setup de logging
└── main.py               # Entry point
```

## Design Patterns Aplicados

### 1. Singleton - Spark Session Manager
Garante UMA única instância do SparkSession:

```python
from functools import lru_cache

class SparkSessionManager:
    @staticmethod
    @lru_cache(maxsize=1)
    def get_session(config):
        return SparkSession.builder \
            .appName(config["app"]["name"]) \
            .master(config["spark"]["master"]) \
            .getOrCreate()
```

### 2. Template Method - Pipeline Base
Define esqueleto do ETL, subclasses implementam cada fase:

```python
from abc import ABC, abstractmethod

class Pipeline(ABC):
    @abstractmethod
    def extract(self): pass
    
    @abstractmethod
    def transform(self, data): pass
    
    @abstractmethod
    def load(self, data): pass
    
    def run(self):  # Template Method
        data = self.extract()
        transformed = self.transform(data)
        self.load(transformed)
```

### 3. Strategy - Componentes ETL Intercambiáveis

```python
# Trocar fonte de dados sem mudar pipeline
class JsonExtractor(Extractor):
    def extract(self):
        return self.spark.read.json(path)

class PostgresExtractor(Extractor):
    def extract(self):
        return self.spark.read.jdbc(url, table, props)
```

### 4. Factory - Criação de Componentes

```python
class ETLFactory:
    @staticmethod
    def create_extractor(type, spark, config):
        extractors = {"json": JsonExtractor, "postgres": PostgresExtractor}
        return extractors[type](spark, config)
```

## Config YAML por Ambiente

```yaml
dev:
  spark:
    master: "local[*]"
    driver_memory: "1g"
  storage:
    input_path: "./storage"

prod:
  spark:
    master: "yarn"
    deploy_mode: "cluster"
    executor_instances: 2
  storage:
    input_path: "s3a://ubereats-data/input"
```

## Resumo dos Patterns

| Pattern | Uso | Benefício |
|---------|-----|-----------|
| **Singleton** | SparkSession | Evita múltiplas sessões |
| **Template Method** | Pipeline base | Estrutura consistente |
| **Strategy** | Extract/Transform/Load | Componentes intercambiáveis |
| **Factory** | Criação de componentes | Desacoplamento |

## Execução

```bash
python main.py --env dev   # Desenvolvimento
python main.py --env prod  # Produção
```

## Best Practices
1. **Modularidade**: ETL em módulos separados
2. **Config externa**: YAML por ambiente (dev/test/prod)
3. **Logging estruturado**: Em cada fase do pipeline
4. **Single Responsibility**: Cada classe faz uma coisa
5. **Dependency Injection**: Injetar SparkSession e config
