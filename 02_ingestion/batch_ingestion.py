from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import requests
import json
import os

# --- Config ---
PG_CONFIG = {
    "url": "jdbc:postgresql://host.docker.internal:5433/swiggy",
    "user": "swiggy_user",
    "password": "swiggy_pass",
    "driver": "org.postgresql.Driver"
}

BRONZE_PATH = "data/delta_lake/bronze"

def ingest_batch():
    spark = SparkSession.builder \
        .appName("BatchIngestion") \
        .master("local[2]") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.postgresql:postgresql:42.7.3") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # 1. File Ingestion: CSV
    print("Ingesting restaurants.csv...")
    df_rest = spark.read.csv("data/raw/restaurants.csv", header=True, inferSchema=True)
    df_rest = df_rest.withColumn("ingested_at", current_timestamp())
    df_rest.write.format("delta").mode("overwrite").save(f"{BRONZE_PATH}/restaurants")
    print(f"Loaded {df_rest.count()} restaurant records.")

    # 2. File Ingestion: JSON
    print("Ingesting menu_items.json...")
    df_menu = spark.read.option("multiline", "true").json("data/raw/menu_items.json")
    df_menu = df_menu.withColumn("ingested_at", current_timestamp())
    df_menu.write.format("delta").mode("overwrite").save(f"{BRONZE_PATH}/menu_items")
    print(f"Loaded {df_menu.count()} menu items.")

    # 3. DB Ingestion: JDBC from Postgres
    print("Ingesting historical orders from Postgres...")
    df_orders = spark.read.format("jdbc") \
        .option("url", PG_CONFIG["url"]) \
        .option("dbtable", "orders") \
        .option("user", PG_CONFIG["user"]) \
        .option("password", PG_CONFIG["password"]) \
        .option("driver", PG_CONFIG["driver"]) \
        .load()
    df_orders = df_orders.withColumn("ingested_at", current_timestamp())
    df_orders.write.format("delta").mode("overwrite").save(f"{BRONZE_PATH}/historical_orders")
    print(f"Loaded {df_orders.count()} historical orders.")

    print("\nBatch Ingestion Complete!")
    spark.stop()

if __name__ == "__main__":
    # Ensure bronze dirs exist
    os.makedirs(BRONZE_PATH, exist_ok=True)
    ingest_batch()
