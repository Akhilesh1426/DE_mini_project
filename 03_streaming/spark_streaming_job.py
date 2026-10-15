from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, when, to_json, struct
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, ArrayType, IntegerType
)

import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BRONZE_PATH = os.path.join(BASE_DIR, "data", "delta_lake", "bronze", "orders")
DLQ_PATH = os.path.join(BASE_DIR, "data", "delta_lake", "bronze", "dlq")
CHECKPOINT_DIR = os.path.join(BASE_DIR, "data", "spark_checkpoints")

# --- Spark Session ---
spark = SparkSession.builder \
    .appName("SwiggyStreamingPipeline") \
    .master("local[2]") \
    .config("spark.driver.memory", "512m") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.default.parallelism", "2") \
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.streaming.checkpointLocation", os.path.join(CHECKPOINT_DIR, "default")) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- Schema Definition ---
ITEM_SCHEMA = StructType([
    StructField("item_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType())
])

ORDER_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("timestamp", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("restaurant_id", StringType()),
    StructField("items", ArrayType(ITEM_SCHEMA)),
    StructField("total_amount", DoubleType()),
    StructField("status", StringType()),
    StructField("delivery_address", StringType()),
    StructField("is_corrupted", BooleanType()),
    StructField("_error_reason", StringType())
])

VALID_EVENT_TYPES = ["order_placed", "order_updated", "order_delivered"]

# --- Read from Redpanda ---
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "orders.events") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# --- Parse JSON ---
parsed = raw_stream \
    .select(from_json(col("value").cast("string"), ORDER_SCHEMA).alias("data")) \
    .select("data.*") \
    .withColumn("ingested_at", current_timestamp())

# --- Validation Rules (DLQ Logic) ---
is_valid = (
    col("order_id").isNotNull() &
    (col("total_amount") > 0) &
    col("event_type").isin(VALID_EVENT_TYPES) &
    col("customer_id").isNotNull()
)

clean_stream = parsed.filter(is_valid)
dlq_stream = parsed.filter(~is_valid).withColumn(
    "dlq_reason",
    when(col("order_id").isNull(), "null_order_id")
    .when(col("total_amount") <= 0, "non_positive_amount")
    .when(~col("event_type").isin(VALID_EVENT_TYPES), "invalid_event_type")
    .otherwise("missing_required_field")
)

# --- Write Clean Records to Bronze Delta ---
clean_query = clean_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "bronze_orders")) \
    .partitionBy("status") \
    .trigger(processingTime="10 seconds") \
    .start(BRONZE_PATH)

# --- Write Bad Records to DLQ ---
dlq_query = dlq_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "dlq")) \
    .trigger(processingTime="10 seconds") \
    .start(DLQ_PATH)

# --- Write Bad Records to DLQ (Kafka) ---
dlq_kafka_stream = dlq_stream.select(to_json(struct("*")).alias("value"))

dlq_query_kafka = dlq_kafka_stream.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("topic", "orders.dlq") \
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "dlq_kafka")) \
    .trigger(processingTime="10 seconds") \
    .start()

print(f"[Streaming] Clean -> {BRONZE_PATH}")
print(f"[Streaming] DLQ (Delta) -> {DLQ_PATH}")
print(f"[Streaming] DLQ (Kafka) -> Topic: orders.dlq")
print("[Streaming] Spark UI: http://localhost:4040")
print("[Streaming] Awaiting stream termination... (Ctrl+C to stop)")

spark.streams.awaitAnyTermination()
