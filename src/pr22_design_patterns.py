"""
PR-22: Design Patterns for Data Pipelines
Practiced on: 2026-04-03
VPS: will@31.97.162.147

Patterns covered:
  1. Singleton     — One SparkSession for the whole app
  2. Strategy      — Interchangeable Extractors, Transformers, Loaders
  3. Factory       — Creates the right component by type string
  4. Template Method — Pipeline skeleton (extract→transform→load)

Run:
  docker cp /tmp/pr22/pr22_design_patterns.py spark-master:/opt/spark/jobs/pr22_design_patterns.py
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    --conf spark.executor.extraClassPath=/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    /opt/spark/jobs/pr22_design_patterns.py
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from abc import ABC, abstractmethod
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

# ══════════════════════════════════════════════════════════════════════════════
# PATTERN 1: SINGLETON
# Problem: SparkSession is expensive — creating two by accident causes errors
# Solution: Class-level variable that holds the one instance forever
# ══════════════════════════════════════════════════════════════════════════════
class SparkSessionManager:
    _instance = None

    @classmethod
    def get_session(cls, app_name="PR22-Patterns", master="spark://spark-master:7077"):
        if cls._instance is None:
            cls._instance = SparkSession.builder \
                .appName(app_name) \
                .master(master) \
                .getOrCreate()
            cls._instance.sparkContext.setLogLevel("WARN")
            logging.getLogger("Singleton").info(f"SparkSession created: {app_name}")
        else:
            logging.getLogger("Singleton").info("Reusing existing SparkSession")
        return cls._instance


# ══════════════════════════════════════════════════════════════════════════════
# PATTERN 2: STRATEGY — Extractors
# Problem: Source changes (memory → S3 → PostgreSQL) shouldn't break pipeline
# Solution: Abstract interface + multiple implementations, all look the same
# ══════════════════════════════════════════════════════════════════════════════
class Extractor(ABC):
    def __init__(self, spark): self.spark = spark
    @abstractmethod
    def extract(self): pass

class InMemoryExtractor(Extractor):
    """Dev/test — no infra needed"""
    def extract(self):
        logging.getLogger("Extractor").info("Extracting from in-memory data")
        schema = StructType([
            StructField("restaurant_id", IntegerType()),
            StructField("name", StringType()),
            StructField("cuisine_type", StringType()),
            StructField("city", StringType()),
            StructField("average_rating", FloatType()),
        ])
        data = [
            (1,"Pizza Palace","Italian","Sao Paulo",4.5),
            (2,"Burger Boss","American","Sao Paulo",3.8),
            (3,"Sushi Star","Japanese","Rio",4.9),
            (4,"Taco Town","Mexican","Rio",4.1),
            (5,"Pasta Place","Italian","Curitiba",4.7),
            (6,"BBQ Barn","American","Curitiba",3.5),
            (7,"Ramen Road","Japanese","Sao Paulo",4.3),
            (8,"Curry Club","Indian","Rio",4.6),
        ]
        return self.spark.createDataFrame(data, schema)

class MinIOExtractor(Extractor):
    """Production — reads from object storage"""
    def __init__(self, spark, path):
        super().__init__(spark)
        self.path = path
    def extract(self):
        logging.getLogger("Extractor").info(f"Extracting from MinIO: {self.path}")
        return self.spark.read.parquet(self.path)


# ══════════════════════════════════════════════════════════════════════════════
# PATTERN 2: STRATEGY — Transformers
# ══════════════════════════════════════════════════════════════════════════════
class Transformer(ABC):
    @abstractmethod
    def transform(self, df): pass

class RatingCategoryTransformer(Transformer):
    """Enriches data with a human-readable rating label"""
    def transform(self, df):
        logging.getLogger("Transformer").info("Adding rating_category column")
        return df.withColumn("rating_category",
            F.when(F.col("average_rating") >= 4.5, "Excellent")
             .when(F.col("average_rating") >= 4.0, "Good")
             .otherwise("Average")
        )

class CuisineKPITransformer(Transformer):
    """Aggregates KPIs per cuisine type — Gold layer ready"""
    def transform(self, df):
        logging.getLogger("Transformer").info("Aggregating KPIs by cuisine")
        return df.groupBy("cuisine_type").agg(
            F.count("*").alias("total"),
            F.round(F.avg("average_rating"), 2).alias("avg_rating"),
            F.max("average_rating").alias("best_rating")
        ).orderBy("avg_rating", ascending=False)


# ══════════════════════════════════════════════════════════════════════════════
# PATTERN 2: STRATEGY — Loaders
# ══════════════════════════════════════════════════════════════════════════════
class Loader(ABC):
    @abstractmethod
    def load(self, df): pass

class ConsoleLoader(Loader):
    """Dev/debug — just print to stdout"""
    def __init__(self, label=""): self.label = label
    def load(self, df):
        logging.getLogger("Loader").info(f"Loading to console: {self.label}")
        df.show()

class ParquetLoader(Loader):
    """Production — persists to object storage"""
    def __init__(self, path): self.path = path
    def load(self, df):
        logging.getLogger("Loader").info(f"Writing parquet to: {self.path}")
        df.write.format("parquet").mode("overwrite").save(self.path)


# ══════════════════════════════════════════════════════════════════════════════
# PATTERN 3: FACTORY
# Problem: Who decides which Extractor/Transformer/Loader to use?
# Solution: One class owns all creation logic — callers just pass a string
# ══════════════════════════════════════════════════════════════════════════════
class ETLFactory:
    @staticmethod
    def create_extractor(source_type, spark, **kwargs):
        extractors = {
            "memory": lambda: InMemoryExtractor(spark),
            "minio":  lambda: MinIOExtractor(spark, kwargs["path"]),
        }
        if source_type not in extractors:
            raise ValueError(f"Unknown extractor: {source_type}")
        return extractors[source_type]()

    @staticmethod
    def create_transformer(transform_type):
        transformers = {
            "rating": RatingCategoryTransformer,
            "kpi":    CuisineKPITransformer,
        }
        if transform_type not in transformers:
            raise ValueError(f"Unknown transformer: {transform_type}")
        return transformers[transform_type]()

    @staticmethod
    def create_loader(loader_type, **kwargs):
        loaders = {
            "console": lambda: ConsoleLoader(kwargs.get("label", "")),
            "parquet": lambda: ParquetLoader(kwargs["path"]),
        }
        if loader_type not in loaders:
            raise ValueError(f"Unknown loader: {loader_type}")
        return loaders[loader_type]()


# ══════════════════════════════════════════════════════════════════════════════
# PATTERN 4: TEMPLATE METHOD
# Problem: Every pipeline is extract→transform→load, but steps differ
# Solution: Base class owns the skeleton (run()), subclasses fill the steps
# ══════════════════════════════════════════════════════════════════════════════
class Pipeline(ABC):
    def __init__(self, name):
        self.name = name
        self.logger = logging.getLogger(f"Pipeline.{name}")

    @abstractmethod
    def extract(self): pass
    @abstractmethod
    def transform(self, df): pass
    @abstractmethod
    def load(self, df): pass

    def run(self):  # Template Method — structure is fixed here
        self.logger.info(f"=== Starting pipeline: {self.name} ===")
        df = self.extract()
        self.logger.info(f"Extracted {df.count()} rows")
        df_t = self.transform(df)
        self.load(df_t)
        self.logger.info(f"=== Pipeline {self.name} complete ===")

class RestaurantRatingPipeline(Pipeline):
    def __init__(self, spark):
        super().__init__("RestaurantRating")
        self.extractor   = ETLFactory.create_extractor("memory", spark)
        self.transformer = ETLFactory.create_transformer("rating")
        self.loader      = ETLFactory.create_loader("console", label="Rating Pipeline Output")

    def extract(self):       return self.extractor.extract()
    def transform(self, df): return self.transformer.transform(df)
    def load(self, df):      return self.loader.load(df)

class CuisineKPIPipeline(Pipeline):
    def __init__(self, spark, output_path):
        super().__init__("CuisineKPI")
        self.extractor   = ETLFactory.create_extractor("memory", spark)
        self.transformer = ETLFactory.create_transformer("kpi")
        self.loader      = ETLFactory.create_loader("parquet", path=output_path)

    def extract(self):       return self.extractor.extract()
    def transform(self, df): return self.transformer.transform(df)
    def load(self, df):      return self.loader.load(df)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("\n=== PR-22: Design Patterns ===")

    # Pattern 1: Singleton — same object returned every time
    print("\n--- Pattern 1: Singleton ---")
    spark1 = SparkSessionManager.get_session()
    spark2 = SparkSessionManager.get_session()
    print(f"Same session? {spark1 is spark2}")  # Must be True

    # Patterns 2+3+4 working together
    print("\n--- Pipeline 1: Restaurant Rating (console output) ---")
    RestaurantRatingPipeline(spark1).run()

    print("\n--- Pipeline 2: Cuisine KPI (parquet to MinIO) ---")
    CuisineKPIPipeline(spark1, output_path="s3a://ubereats-datalake/gold/cuisine_kpi_pr22").run()

    # Verify gold layer
    print("\n--- Verifying Gold layer output ---")
    spark1.read.parquet("s3a://ubereats-datalake/gold/cuisine_kpi_pr22").show()

    print("\n=== PR-22 Complete! ===")
    spark1.stop()
