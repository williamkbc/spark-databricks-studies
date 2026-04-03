# CLAUDE.md — Spark & Databricks Study Guide

## Who is Will

- Data engineer studying Spark and Databricks via the **frm-spark-databricks-mec** course (OWSHq Academy)
- Practices on a **VPS Hostinger** at `will@31.97.162.147` (Ubuntu, 2 CPU, 8GB RAM)
- Cluster runs via Docker: `apache/spark:3.5.1` containers on `spark-net` network
- Study notes live in `docs/`, practice scripts in `src/`
- GitHub: `github.com/williamkbc/spark-databricks-studies`
- **Databricks**: Community Edition access (free tier, single-node cluster, no streaming)

---

## How to Teach Will

**Be a professor, not a rubber duck.** Will learns best when you:

1. **Explain the WHY before the HOW** — don't just show code, explain what problem the pattern/concept solves
2. **Use analogies** — real-world comparisons make abstract concepts stick (e.g. Singleton = one power grid)
3. **Show pros AND cons** — no pattern is perfect, trade-offs matter in interviews and real work
4. **Walk through each step** as you build — narrate what you're doing and why
5. **Connect to interviews** — flag when a concept commonly comes up in Data Engineer interviews
6. **Fill the gaps** — if Will's doc is incomplete, enrich it with what's missing

### Teaching Format (use when explaining concepts)
```
1. The Problem — what pain does this solve?
2. The Solution — the pattern/concept
3. Code example — minimal, clear
4. Pros & Cons table
5. Real-world analogy
6. Interview angle (when relevant)
```

---

## Learning Path — How Topics Connect

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA PIPELINE LIFECYCLE                   │
│                                                             │
│  INGEST          TRANSFORM         DELIVER       OPTIMIZE   │
│  PR-20 JDBC  →  PR-22 Patterns →  PR-21 MinIO →  PR-29 5S  │
│  PR-21 S3    →  PR-26 Errors   →  PR-28 SQL   →  PR-31 SQL │
│                                                             │
│  FOUNDATIONS (already done): PR-4→19 PySpark, Pandas, SQL  │
│  INTEGRATIONS (current):     PR-20→26 JDBC, S3, Patterns   │
│  OPTIMIZATION (next):        PR-29, PR-31 Performance       │
│  CLOUD (final):              Módulo 3 Databricks            │
└─────────────────────────────────────────────────────────────┘
```

**Build on prior knowledge — don't re-explain from scratch:**
- Will knows PySpark DataFrame API (PR-4 to PR-6)
- Will knows Pandas API on Spark (PR-7 to PR-13)
- Will knows Spark SQL basics (PR-14 to PR-19)
- Will knows JDBC (PR-20), MinIO/S3 (PR-21), Design Patterns (PR-22), Error Handling (PR-26)

---

## Interview Cheat Sheet

### PR-20: JDBC
- **Q: How do you read a large PostgreSQL table efficiently in Spark?**
  A: Parallel read with `column`, `lowerBound`, `upperBound`, `numPartitions` — Spark launches N simultaneous queries each reading an ID range.
- **Q: What is predicate pushdown in JDBC context?**
  A: Using `predicates=["col > value"]` — the filter runs inside the DB, less data crosses the network.
- **Q: Old JDBC driver on PostgreSQL 15?**
  A: Fails with SCRAM-SHA-256 auth error. Need driver 42.x+.

### PR-21: Object Storage
- **Q: How does Spark connect to S3/MinIO?**
  A: `s3a://` protocol via `hadoop-aws` + `aws-java-sdk-bundle` JARs. Credentials via `spark.hadoop.fs.s3a.*` configs.
- **Q: What is partition pruning?**
  A: When you `partitionBy("year","month")` on write, Spark creates folders. On read, a filter on `year/month` skips irrelevant folders entirely — no I/O.
- **Q: Best file format for analytics?**
  A: Parquet — columnar, compressed, supports predicate pushdown and schema evolution.

### PR-22: Design Patterns
- **Q: Why Singleton for SparkSession?**
  A: SparkSession is expensive to create, connects to cluster, allocates driver memory. Two sessions = errors and wasted resources.
- **Q: What is the Strategy pattern used for in pipelines?**
  A: Interchangeable components (extractors, transformers, loaders) behind a common interface — swap source from memory to S3 without changing pipeline code.
- **Q: Difference between Strategy and Template Method?**
  A: Template Method defines the skeleton (run = extract→transform→load), Strategy defines interchangeable implementations of each step.

### PR-26: Error Handling
- **Q: Why custom exceptions instead of catching Exception everywhere?**
  A: `DataQualityError` → alert data team. `ProcessingError` → retry or page on-call. Generic `Exception` loses that context.
- **Q: What is fail-fast validation?**
  A: Validate schema, row counts, nulls immediately after `read()` — before expensive transformations. Saves compute time and gives clear error messages.
- **Q: How does retry with backoff work?**
  A: Linear: wait `N * delay_seconds` per attempt. Exponential: `delay ** attempt`. Prevents hammering a struggling system.

---

## "What to Observe" — Depth Questions

After running each exercise, ask yourself:

**JDBC (PR-20)**
- What SQL did Spark actually send to PostgreSQL? (check `.explain()`)
- What happens if `numPartitions` > number of rows?
- Why is `reWriteBatchedInserts=true` faster?

**MinIO (PR-21)**
- How many files were created in each partition folder? (`mc ls --recursive`)
- What happens if you filter on a non-partitioned column vs a partitioned column? (full scan vs pruning)
- Why is JSON the worst format for analytics?

**Design Patterns (PR-22)**
- What breaks if two pipelines create their own SparkSession?
- How would you add a new `PostgresLoader` without touching existing code?
- What's the difference between Factory and a simple `if/else`?

**Error Handling (PR-26)**
- What happens if you don't use `@wraps(func)`? (hint: function name/docstring is lost)
- When would you use exponential backoff instead of linear?
- What's the difference between catching at the decorator level vs in `main()`?

---

## Databricks Community Edition

- **Access**: community.cloud.databricks.com (free, single-node cluster)
- **Limitations vs production Databricks**:
  - Single node only (no multi-worker clusters)
  - No Delta Live Tables, MLflow, Unity Catalog
  - Cluster auto-terminates after 2h inactivity
  - No JDBC to external DBs (networking restricted)
- **What CAN be practiced**:
  - Delta Lake (ACID transactions, time travel, MERGE)
  - Databricks notebooks + magic commands (`%sql`, `%md`, `%sh`)
  - DBFS (Databricks File System) as storage
  - Spark SQL in notebooks
  - Basic MLlib
- **Key differences from local Spark to know for interviews**:
  - `dbutils.fs` instead of HDFS commands
  - `spark.table()` reads from Hive metastore
  - Delta format is the default (not Parquet)
  - `display()` instead of `.show()` for rich output

---

## VPS Environment

### Start the cluster (after reboot)
```bash
# Spark cluster
docker rm -f spark-master spark-worker-1 2>/dev/null
docker network create spark-net 2>/dev/null

docker run -d --name spark-master --network spark-net \
  -p 8080:8080 -p 7077:7077 \
  apache/spark:3.5.1 /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master

docker run -d --name spark-worker-1 --network spark-net \
  apache/spark:3.5.1 /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077

# PostgreSQL
docker run -d --name postgres --network spark-net \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=ubereats \
  -e POSTGRES_HOST_AUTH_METHOD=md5 \
  postgres:15-alpine

# MinIO
docker run -d --name minio --network spark-net \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

### JARs already in containers (copy after recreation)
```bash
# JDBC (PostgreSQL 15)
docker cp /tmp/postgresql-42.7.3.jar spark-master:/opt/spark/jars/postgresql-42.7.3.jar
docker cp /tmp/postgresql-42.7.3.jar spark-worker-1:/opt/spark/jars/postgresql-42.7.3.jar

# S3/MinIO
docker cp /tmp/hadoop-aws-3.3.4.jar spark-master:/opt/spark/jars/hadoop-aws-3.3.4.jar
docker cp /tmp/aws-java-sdk-bundle-1.12.262.jar spark-master:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar
docker cp /tmp/hadoop-aws-3.3.4.jar spark-worker-1:/opt/spark/jars/hadoop-aws-3.3.4.jar
docker cp /tmp/aws-java-sdk-bundle-1.12.262.jar spark-worker-1:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar
```

### spark-submit templates
```bash
# Basic PySpark
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/script.py

# With PostgreSQL JDBC
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.extraClassPath=/opt/spark/jars/postgresql-42.7.3.jar \
  --jars /opt/spark/jars/postgresql-42.7.3.jar \
  /opt/spark/jobs/script.py

# With MinIO/S3
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.executor.extraClassPath=/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  /opt/spark/jobs/script.py
```

### Copy script to container
```bash
docker cp /tmp/script.py spark-master:/opt/spark/jobs/script.py
```

---

## Lessons Learned (Gotchas)

| Problem | Cause | Fix |
|---------|-------|-----|
| `postgresql-9.4` auth error on PG15 | Old JDBC driver doesn't support SCRAM-SHA-256 | Use `postgresql-42.7.3.jar` |
| S3A `NoAuthWithAWSException` | Credentials not propagated to executor | Pass via `--conf spark.hadoop.fs.s3a.*` in spark-submit |
| `owshq-spark:3.5` exits immediately | Bitnami image, wrong entrypoint for compose | Use `apache/spark:3.5.1` with explicit spark-class command |
| `docker-compose` not found | v2 installed as plugin | Use `docker compose` (no hyphen) |
| Script not found in container | Wrote to host `/tmp`, not copied to container | Always `docker cp` before spark-submit |

---

## Progress

See `docs/PROGRESSO.md` for full status.

### Practiced PRs with scripts in `src/`
| PR | Topic | Script |
|----|-------|--------|
| PR-20 | Spark + PostgreSQL JDBC | `src/pr20_jdbc_practice.py` |
| PR-21 | Spark + MinIO Object Storage | `src/pr21_minio_practice.py` |
| PR-22 | Design Patterns (Singleton, Strategy, Factory, Template Method) | `src/pr22_design_patterns.py` |

### Practiced PRs with scripts in `src/`
| PR | Topic | Script | Key Concepts |
|----|-------|--------|--------------|
| PR-20 | Spark + PostgreSQL JDBC | `src/pr20_jdbc_practice.py` | predicate pushdown, parallel read, write modes |
| PR-21 | Spark + MinIO Object Storage | `src/pr21_minio_practice.py` | s3a://, partition pruning, medallion bronze/silver/gold |
| PR-22 | Design Patterns | `src/pr22_design_patterns.py` | Singleton, Strategy, Factory, Template Method |
| PR-26 | Error Handling & Retry | `src/pr26_error_handling.py` | custom exceptions, decorators, fail-fast, backoff |

### Next priorities
1. **PR-29** — 5 S's of optimization, Broadcast vs Sort-Merge joins *(most interview-critical)*
2. **PR-31** — CUBE, ROLLUP, join strategies, CACHE TABLE
3. **PR-28** — Modular SQL with chained CTEs
4. **PR-23** — Pipeline End-to-End with tests
5. **Módulo 3** — Databricks Community Edition (Delta Lake, notebooks, DBFS)
