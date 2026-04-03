# CLAUDE.md — Spark & Databricks Study Guide

## Who is Will

- Data engineer studying Spark and Databricks via the **frm-spark-databricks-mec** course (OWSHq Academy)
- Practices on a **VPS Hostinger** at `will@31.97.162.147` (Ubuntu, 2 CPU, 8GB RAM)
- Cluster runs via Docker: `apache/spark:3.5.1` containers on `spark-net` network
- Study notes live in `docs/`, practice scripts in `src/`
- GitHub: `github.com/williamkbc/spark-databricks-studies`

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

### Next priorities
1. **PR-26** — Error handling with decorators and retry
2. **PR-29** — 5 S's of optimization, Broadcast vs Sort-Merge joins
3. **PR-31** — CUBE, ROLLUP, join strategies, CACHE TABLE
4. **PR-28** — Modular SQL with chained CTEs
5. **Módulo 3** — Databricks
