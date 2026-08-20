from pyspark.sql import SparkSession
from pyspark.sql.functions import col


INPUT_PATH = "hdfs://namenode:9000/maritime/raw/{2024,2025}/*.csv"
OUTPUT_PATH = "hdfs://namenode:9000/maritime/processed/ais"


spark = (
    SparkSession.builder
    .appName("Maritime AIS CSV to Parquet")
    .getOrCreate()
)

print("========================================")
print("Reading AIS CSV data from HDFS...")
print("========================================")

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(INPUT_PATH)
)

print("Input schema:")
df.printSchema()

input_count = df.count()
print(f"Input records: {input_count}")

print("========================================")
print("Cleaning and transforming data...")
print("========================================")

clean_df = (
    df
    .withColumn("mmsi", col("mmsi").cast("long"))
    .withColumn("longitude", col("longitude").cast("double"))
    .withColumn("latitude", col("latitude").cast("double"))
    .withColumn("sog", col("sog").cast("double"))
    .withColumn("cog", col("cog").cast("double"))
    .withColumn("heading", col("heading").cast("double"))

    # Required fields
    .filter(col("mmsi").isNotNull())
    .filter(col("latitude").isNotNull())
    .filter(col("longitude").isNotNull())

    # Valid geographic coordinates
    .filter((col("latitude") >= -90) & (col("latitude") <= 90))
    .filter((col("longitude") >= -180) & (col("longitude") <= 180))

    # Valid speed
    .filter((col("sog") >= 0) | col("sog").isNull())

    # Valid course
    .filter((col("cog") >= 0) & (col("cog") <= 360) | col("cog").isNull())
)

output_count = clean_df.count()

print(f"Clean records: {output_count}")
print(f"Removed records: {input_count - output_count}")

print("========================================")
print("Writing cleaned data as Parquet...")
print("========================================")

(
    clean_df
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print("========================================")
print(f"SUCCESS: Parquet written to:")
print(OUTPUT_PATH)
print("========================================")

spark.stop()