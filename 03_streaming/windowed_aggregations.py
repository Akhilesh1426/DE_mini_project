from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, sum as _sum, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, ArrayType, IntegerType
)

spark = SparkSession.builder \
    .appName("WindowedAggregations") \
    .master("local[2]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

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

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders.events") \
    .option("startingOffsets", "latest") \
    .load()

events = raw \
    .select(from_json(col("value").cast("string"), ORDER_SCHEMA).alias("d")) \
    .select("d.*") \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .filter(col("order_id").isNotNull())

# Watermark: tolerate events up to 2 minutes late
windowed = events \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("restaurant_id")
    ) \
    .agg(
        count("order_id").alias("order_count"),
        _sum("total_amount").alias("total_revenue")
    )

query = windowed.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
