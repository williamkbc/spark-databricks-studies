"""
PR-23: End-to-End Pipeline with Tests — Entry Point
Practiced on: 2026-04-03
VPS: will@31.97.162.147

Run:
  docker cp /tmp/pr23/src/pr23_config.py      spark-master:/opt/spark/jobs/pr23_config.py
  docker cp /tmp/pr23/src/pr23_validators.py  spark-master:/opt/spark/jobs/pr23_validators.py
  docker cp /tmp/pr23/src/pr23_transformers.py spark-master:/opt/spark/jobs/pr23_transformers.py
  docker cp /tmp/pr23/src/pr23_pipeline.py    spark-master:/opt/spark/jobs/pr23_pipeline.py
  docker cp /tmp/pr23/src/pr23_main.py        spark-master:/opt/spark/jobs/pr23_main.py

  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/pr23_main.py

Tests (VPS, needs virtualenv):
  cd /tmp/pr23
  python3 -m venv venv && venv/bin/pip install pytest pyspark
  venv/bin/pytest tests/test_pipeline.py -v

Key concepts demonstrated:
  - Modular ETL: extract / validate / transform / load are separate methods
  - Fail-fast validation before expensive transforms
  - Broadcast join for small dimension tables
  - Unit tests with local[1] SparkSession (scope="session" for speed)
  - IEEE 754 float trap: never use == on FloatType (use < or > instead)
"""
from pyspark.sql import SparkSession
from pr23_pipeline import UberEatsPipeline


def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("PR23-UberEats-Pipeline") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


if __name__ == "__main__":
    spark = None
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        pipeline = UberEatsPipeline(spark)
        pipeline.run()
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise
    finally:
        if spark:
            spark.stop()
