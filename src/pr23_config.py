"""
PR-23: End-to-End Pipeline with Tests — Config
"""


class AppConfig:
    # Data sources (mock paths for local testing)
    USERS_PATH = "./storage/mongodb/users"
    RESTAURANTS_PATH = "./storage/mysql/restaurants"
    ORDERS_PATH = "./storage/kafka/orders"
    OUTPUT_DIR = "./output"

    # Spark tuning
    SPARK_EXECUTOR_MEMORY = "2g"
    SPARK_DRIVER_MEMORY = "4g"
    SPARK_SHUFFLE_PARTITIONS = "10"

    # Validation thresholds
    MIN_ROW_COUNT = 1
    CRITICAL_COLUMNS_USERS = ["user_id", "email", "phone_number"]
    CRITICAL_COLUMNS_RESTAURANTS = ["restaurant_id", "name", "cuisine_type"]
    CRITICAL_COLUMNS_ORDERS = ["order_id", "restaurant_id", "amount"]
