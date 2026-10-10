from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Verify") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("\n--- RESULTS ---")
try:
    clean = spark.read.format("delta").load("data/delta_lake/bronze/orders").count()
    print(f"Clean Records: {clean}")
except Exception as e:
    print("Clean Records: None or Error")

try:
    dlq = spark.read.format("delta").load("data/delta_lake/bronze/dlq").count()
    print(f"DLQ Records: {dlq}")
except Exception as e:
    print("DLQ Records: None or Error")

print("--- END ---")
