# Progresso - Formação Spark & Databricks (OWSHq MEC)

## Informações
- **Curso**: frm-spark-databricks-mec (owshq-mec)
- **Repo Pessoal**: github.com/williamkbc/spark-databricks-studies
- **Repo Curso**: github.com/owshq-mec/frm-spark-databricks-mec (somente leitura)
- **Ambiente**: Docker com Apache Spark 3.5.1, container spark-master
- **PySpark**: `docker exec -it spark-master /opt/spark/bin/pyspark --master local[2]`
- **Dados**: `/opt/spark/work/data` (dentro do container)

---

## Módulo 1 - Setup do Ambiente ✅
| PR | Tema | Status |
|----|------|--------|
| PR-1 | Instalação via Docker | ✅ Praticado |
| PR-2 | PySpark Shell | ✅ Praticado |
| PR-3 | Spark Submit | ✅ Praticado |
| PR-4 a PR-7 | Docker Setup | ✅ Praticado |

---

## Módulo 2 - PySpark, Pandas API, Spark SQL

### Bloco 1: PySpark DataFrame API
| PR | Tema | Status |
|----|------|--------|
| PR-4 | Ingestão de Dados (JSON, Parquet, schema) | ✅ Praticado |
| PR-5 | Transformações Básicas (select, filter, withColumn) | ✅ Praticado |
| PR-6 | Transformações Complexas (groupBy, JOIN, distinct) | ✅ Praticado |

### Bloco 2: Pandas API on Spark
| PR | Tema | Status |
|----|------|--------|
| PR-7 | Ingestão com Pandas API | ✅ Praticado |
| PR-8 | Transformações Básicas Pandas API | ✅ Praticado |
| PR-9 | Transformações Complexas Pandas API | ✅ Praticado |
| PR-10 | Data Delivery Pandas API | ✅ Praticado |
| PR-11 | Avançado: GroupBy, Merge, Rank, Shift | ✅ Praticado |
| PR-12 | UDFs, Lazy/Eager, Workarounds | ✅ Praticado |
| PR-13 | Data Delivery (CSV, Parquet, particionamento) | ✅ Praticado |

### Bloco 3: Spark SQL
| PR | Tema | Status |
|----|------|--------|
| PR-14 | Ingestion: Views, Tables, Catalog | ✅ Praticado |
| PR-15 | Transformações: SELECT, WHERE, GROUP BY | ✅ Praticado |
| PR-16 | Avançado: Window Functions, CTEs | ✅ Praticado |
| PR-17 | UDFs, PERCENTILE, STDDEV, PIVOT | ✅ Praticado |
| PR-18 | Tabelas Permanentes, Particionamento, Medallion | ✅ Praticado |
| PR-19 | Performance: PySpark vs SQL (Catalyst Optimizer) | ✅ Praticado |

### Bloco 4: Integrações e Arquitetura
| PR | Tema | Status |
|----|------|--------|
| PR-20 | Spark + PostgreSQL (JDBC) | ✅ Praticado |
| PR-21 | Spark + Object Storage (S3/MinIO) | ✅ Praticado |
| PR-22 | Design Patterns (Singleton, Strategy, Factory) | ✅ Praticado |
| PR-23 | Pipeline End-to-End com testes | ✅ Praticado |
| PR-24 | Migração Pandas → Pandas API | ⚠️ Só markdown - PRATICAR |

### Bloco 5: Consolidação e Deep Dive
| PR | Tema | Status |
|----|------|--------|
| PR-25 | Spark SQL como ferramenta principal | ⚠️ Só markdown - PRATICAR |
| PR-26 | Pipeline com Error Handling e Retry | ✅ Praticado |
| PR-27 | Pipeline Pandas API otimizado | ⚠️ Só markdown - PRATICAR |
| PR-28 | SQL End-to-End modular (.sql files) | ✅ Praticado |
| PR-29 | PySpark: 5 S's, Broadcast, Salting, Pitfalls | ✅ Praticado |
| PR-30 | Pandas API Deep Dive | ⚠️ Só markdown - PRATICAR |
| PR-31 | Spark SQL Deep Dive: Joins, CUBE, ROLLUP | ✅ Praticado |

---

## Módulo 3 - Databricks
| PR | Tema | Status |
|----|------|--------|
| - | Pendente | ⏳ Não iniciado |

---

## Legenda
- ✅ Praticado: Executei comandos no PySpark e vi resultados
- 📖 Só markdown: Conceitual, precisa infra adicional
- ⚠️ Só markdown - PRATICAR: Tem conteúdo prático mas não executei ainda
- ⏳ Não iniciado

## Próximos Passos
1. Assistir aulas do curso (PRs 22-31)
2. Praticar PRs marcados com ⚠️ no PySpark
3. Focar em: Broadcast Joins, CUBE/ROLLUP, Cache, Explain plans
4. Iniciar Módulo 3 (Databricks)

## Prioridade de Prática
Os PRs mais importantes pra praticar (conceitos que caem em entrevista):
1. **PR-29**: 5 S's de otimização, broadcast join vs sort merge
2. **PR-31**: CUBE, ROLLUP, Join Strategies, CACHE TABLE
3. **PR-28**: SQL modular com CTEs encadeadas
4. **PR-26**: Error handling com decorators
5. **PR-22**: Design patterns aplicados a pipelines
