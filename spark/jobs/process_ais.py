from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    to_timestamp,
    hour,
    dayofweek,
    when,
    lit,
    avg,
    stddev,
    count,
    min,
    max,
)


# ============================================================
# CONFIGURATION
# ============================================================

INPUT_PATH = "hdfs://namenode:9000/maritime/raw/2025/ais-2025-01-01.csv"

AIS_OUTPUT_PATH = "hdfs://namenode:9000/maritime/processed/ais"

BASELINE_OUTPUT_PATH = "hdfs://namenode:9000/maritime/processed/baseline"


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Maritime AIS Historical Processing")

    # Shuffle configuration
    .config("spark.sql.shuffle.partitions", "200")

    # Adaptive Query Execution
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")

    # Reduce memory pressure during aggregation/shuffle
    .config("spark.sql.files.maxPartitionBytes", "128m")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# START
# ============================================================

print("=" * 70)
print("MARITIME AIS HISTORICAL PROCESSING")
print("=" * 70)

print(f"Input    : {INPUT_PATH}")
print(f"AIS      : {AIS_OUTPUT_PATH}")
print(f"Baseline : {BASELINE_OUTPUT_PATH}")


# ============================================================
# 1. READ HISTORICAL AIS DATA
# ============================================================

print("\n[1/8] Reading AIS historical data from HDFS...")

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(INPUT_PATH)
)

print("Raw schema:")
df.printSchema()

input_count = df.count()

print(f"Input records: {input_count:,}")


# ============================================================
# 2. STANDARDIZE COLUMNS
# ============================================================

print("\n[2/8] Standardizing columns...")

df = (
    df
    .withColumn(
        "mmsi",
        trim(col("mmsi").cast("string"))
    )
    .withColumn(
        "timestamp",
        to_timestamp(col("base_date_time"))
    )
    .withColumn(
        "longitude",
        col("longitude").cast("double")
    )
    .withColumn(
        "latitude",
        col("latitude").cast("double")
    )
    .withColumn(
        "sog",
        col("sog").cast("double")
    )
    .withColumn(
        "cog",
        col("cog").cast("double")
    )
    .withColumn(
        "heading",
        col("heading").cast("double")
    )
    .withColumn(
        "vessel_type",
        col("vessel_type").cast("integer")
    )
    .withColumn(
        "status",
        col("status").cast("integer")
    )
)


# ============================================================
# 3. CLEAN AIS RECORDS
# ============================================================

print("\n[3/8] Cleaning AIS records...")

clean_df = (
    df

    # --------------------------------------------------------
    # Required fields
    # --------------------------------------------------------

    .filter(col("mmsi").isNotNull())
    .filter(col("timestamp").isNotNull())
    .filter(col("latitude").isNotNull())
    .filter(col("longitude").isNotNull())

    # --------------------------------------------------------
    # Geographic validation
    # --------------------------------------------------------

    .filter(
        (col("latitude") >= -90)
        & (col("latitude") <= 90)
    )
    .filter(
        (col("longitude") >= -180)
        & (col("longitude") <= 180)
    )

    # --------------------------------------------------------
    # Speed validation
    # --------------------------------------------------------

    .filter(
        col("sog").isNull()
        |
        (
            (col("sog") >= 0)
            & (col("sog") <= 100)
        )
    )

    # --------------------------------------------------------
    # Course validation / normalization
    # --------------------------------------------------------

    .withColumn(
        "cog",
        when(
            col("cog").isNotNull(),
            ((col("cog") % 360) + 360) % 360
        )
    )

    # --------------------------------------------------------
    # Heading validation / normalization
    # --------------------------------------------------------

    .withColumn(
        "heading",
        when(
            col("heading").isNotNull(),
            ((col("heading") % 360) + 360) % 360
        )
    )

    # --------------------------------------------------------
    # Vessel name cleanup
    # --------------------------------------------------------

    .withColumn(
        "vessel_name",
        trim(col("vessel_name"))
    )

    # --------------------------------------------------------
    # IMO cleanup
    # --------------------------------------------------------

    .withColumn(
        "imo",
        trim(col("imo"))
    )

    # --------------------------------------------------------
    # Call sign cleanup
    # --------------------------------------------------------

    .withColumn(
        "call_sign",
        trim(col("call_sign"))
    )
)


# ============================================================
# 4. CREATE TIME FEATURES
# ============================================================

print("\n[4/8] Creating time features...")

clean_df = (
    clean_df
    .withColumn(
        "hour",
        hour(col("timestamp"))
    )
    .withColumn(
        "day_of_week",
        dayofweek(col("timestamp"))
    )
)


# ============================================================
# 5. REMOVE DUPLICATES
# ============================================================

print("\n[5/8] Removing duplicate AIS records...")

print("Repartitioning by MMSI before duplicate removal...")

# IMPORTANT:
# Distribute records by MMSI before the large shuffle.
#
# 200 partitions is appropriate for the current dataset size.
# AQE can coalesce them later if necessary.

clean_df = clean_df.repartition(
    200,
    col("mmsi")
)

print("Removing duplicates...")

clean_df = clean_df.dropDuplicates(
    [
        "mmsi",
        "timestamp",
        "latitude",
        "longitude"
    ]
)

# Materialize the result once
clean_count = clean_df.count()

print(f"Clean records   : {clean_count:,}")
print(f"Removed records : {input_count - clean_count:,}")


# ============================================================
# 6. WRITE CLEAN AIS DATA AS PARQUET
# ============================================================

print("\n[6/8] Writing cleaned AIS data as Parquet...")

(
    clean_df
    .write
    .mode("overwrite")
    .partitionBy("day_of_week")
    .parquet(AIS_OUTPUT_PATH)
)

print(
    f"AIS Parquet written to: {AIS_OUTPUT_PATH}"
)


# ============================================================
# 7. BUILD HISTORICAL VESSEL BEHAVIOR BASELINE
# ============================================================

print("\n[7/8] Building historical vessel behavioral baseline...")

baseline_df = (
    clean_df
    .groupBy("mmsi")
    .agg(
        count("*").alias("record_count"),

        avg("sog").alias("avg_speed"),
        stddev("sog").alias("speed_stddev"),

        avg("cog").alias("avg_course"),
        stddev("cog").alias("course_stddev"),

        min("sog").alias("min_speed"),
        max("sog").alias("max_speed"),

        min("latitude").alias("min_latitude"),
        max("latitude").alias("max_latitude"),

        min("longitude").alias("min_longitude"),
        max("longitude").alias("max_longitude"),

        min("timestamp").alias("first_seen"),
        max("timestamp").alias("last_seen"),
    )
)


# ============================================================
# HANDLE NULL STANDARD DEVIATIONS
# ============================================================

baseline_df = (
    baseline_df

    .withColumn(
        "speed_stddev",
        when(
            col("speed_stddev").isNull(),
            lit(0.0)
        ).otherwise(col("speed_stddev"))
    )

    .withColumn(
        "course_stddev",
        when(
            col("course_stddev").isNull(),
            lit(0.0)
        ).otherwise(col("course_stddev"))
    )
)


print("Baseline schema:")
baseline_df.printSchema()


# ============================================================
# 8. WRITE BASELINE
# ============================================================

print("\n[8/8] Writing vessel behavioral baseline...")

(
    baseline_df
    .write
    .mode("overwrite")
    .parquet(BASELINE_OUTPUT_PATH)
)

print(
    f"Baseline written to: {BASELINE_OUTPUT_PATH}"
)


# ============================================================
# FINAL SUMMARY
# ============================================================

print("\n" + "=" * 70)
print("MARITIME AIS PROCESSING COMPLETED SUCCESSFULLY")
print("=" * 70)

print(f"Input records       : {input_count:,}")
print(f"Clean records       : {clean_count:,}")
print(f"Removed records     : {input_count - clean_count:,}")
print(f"AIS Parquet         : {AIS_OUTPUT_PATH}")
print(f"Behavioral Baseline : {BASELINE_OUTPUT_PATH}")

print("=" * 70)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()