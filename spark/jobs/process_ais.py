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
    abs as spark_abs,
    round as spark_round,
)


# ============================================================
# CONFIGURATION
# ============================================================

INPUT_PATHS = [
    "hdfs://namenode:9000/maritime/raw/2024/ais-2024-01-01.csv",
    "hdfs://namenode:9000/maritime/raw/2024/ais-2024-01-02.csv",

    "hdfs://namenode:9000/maritime/raw/2025/ais-2025-01-01.csv",
    "hdfs://namenode:9000/maritime/raw/2025/ais-2025-01-02.csv",
]

AIS_OUTPUT_PATH = (
    "hdfs://namenode:9000/maritime/processed/ais"
)

BASELINE_OUTPUT_PATH = (
    "hdfs://namenode:9000/maritime/processed/baseline"
)

BATCH_ANOMALY_OUTPUT_PATH = (
    "hdfs://namenode:9000/maritime/processed/anomalies"
)


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Maritime AIS Historical Processing")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
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


# ============================================================
# 1. READ HISTORICAL DATA
# ============================================================

print("\n[1/10] Reading AIS historical data...")

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(INPUT_PATHS)
)

input_count = df.count()

print(f"Input records: {input_count:,}")


# ============================================================
# 2. STANDARDIZE
# ============================================================

print("\n[2/10] Standardizing columns...")

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
# 3. CLEAN
# ============================================================

print("\n[3/10] Cleaning AIS records...")

clean_df = (
    df
    .filter(col("mmsi").isNotNull())
    .filter(col("timestamp").isNotNull())
    .filter(col("latitude").isNotNull())
    .filter(col("longitude").isNotNull())

    .filter(
        (col("latitude") >= -90)
        & (col("latitude") <= 90)
    )

    .filter(
        (col("longitude") >= -180)
        & (col("longitude") <= 180)
    )

    .filter(
        col("sog").isNull()
        |
        (
            (col("sog") >= 0)
            &
            (col("sog") <= 100)
        )
    )

    .withColumn(
        "cog",
        when(
            col("cog").isNotNull(),
            ((col("cog") % 360) + 360) % 360
        )
    )

    .withColumn(
        "heading",
        when(
            col("heading").isNotNull(),
            ((col("heading") % 360) + 360) % 360
        )
    )

    .withColumn(
        "vessel_name",
        trim(col("vessel_name"))
    )

    .withColumn(
        "imo",
        trim(col("imo"))
    )

    .withColumn(
        "call_sign",
        trim(col("call_sign"))
    )
)


# ============================================================
# 4. TIME FEATURES
# ============================================================

print("\n[4/10] Creating time features...")

clean_df = (
    clean_df
    .withColumn("hour", hour(col("timestamp")))
    .withColumn("day_of_week", dayofweek(col("timestamp")))
)


# ============================================================
# 5. REMOVE DUPLICATES
# ============================================================

print("\n[5/10] Removing duplicates...")

clean_df = clean_df.repartition(
    200,
    col("mmsi")
)

clean_df = clean_df.dropDuplicates(
    [
        "mmsi",
        "timestamp",
        "latitude",
        "longitude"
    ]
)

clean_count = clean_df.count()

print(f"Clean records: {clean_count:,}")
print(f"Removed: {input_count - clean_count:,}")


# ============================================================
# 6. WRITE CLEAN HISTORICAL DATA
# ============================================================

print("\n[6/10] Writing historical Parquet...")

(
    clean_df
    .write
    .mode("overwrite")
    .partitionBy("day_of_week")
    .parquet(AIS_OUTPUT_PATH)
)


# ============================================================
# 7. BUILD BASELINE
# ============================================================

print("\n[7/10] Building vessel behavioral baseline...")

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

baseline_df.cache()

baseline_count = baseline_df.count()

print(f"Baseline vessels: {baseline_count:,}")


# ============================================================
# 8. WRITE BASELINE
# ============================================================

print("\n[8/10] Writing behavioral baseline...")

(
    baseline_df
    .write
    .mode("overwrite")
    .parquet(BASELINE_OUTPUT_PATH)
)


# ============================================================
# 9. BATCH ANOMALY DETECTION
# ============================================================

print("\n[9/10] Detecting historical anomalies...")

batch_df = (
    clean_df
    .join(
        baseline_df,
        on="mmsi",
        how="left"
    )
)

# ------------------------------------------------------------
# Speed anomaly
#
# Normal:
# average +/- 3 standard deviations
#
# If stddev = 0, fallback to min/max.
# ------------------------------------------------------------

batch_df = (
    batch_df

    .withColumn(
        "speed_lower_bound",
        when(
            col("speed_stddev") > 0,
            col("avg_speed") - (3 * col("speed_stddev"))
        ).otherwise(col("min_speed"))
    )

    .withColumn(
        "speed_upper_bound",
        when(
            col("speed_stddev") > 0,
            col("avg_speed") + (3 * col("speed_stddev"))
        ).otherwise(col("max_speed"))
    )

    .withColumn(
        "speed_anomaly",
        when(
            col("sog").isNull(),
            False
        ).otherwise(
            (col("sog") < col("speed_lower_bound"))
            |
            (col("sog") > col("speed_upper_bound"))
        )
    )

    .withColumn(
        "course_lower_bound",
        col("avg_course") - (3 * col("course_stddev"))
    )

    .withColumn(
        "course_upper_bound",
        col("avg_course") + (3 * col("course_stddev"))
    )

    .withColumn(
        "course_anomaly",
        when(
            col("cog").isNull(),
            False
        ).otherwise(
            (col("cog") < col("course_lower_bound"))
            |
            (col("cog") > col("course_upper_bound"))
        )
    )

    .withColumn(
        "anomaly",
        col("speed_anomaly") | col("course_anomaly")
    )

    .withColumn(
        "anomaly_type",
        when(
            col("speed_anomaly") & col("course_anomaly"),
            lit("SPEED_AND_COURSE")
        )
        .when(
            col("speed_anomaly"),
            lit("SPEED")
        )
        .when(
            col("course_anomaly"),
            lit("COURSE")
        )
        .otherwise(
            lit("NORMAL")
        )
    )
)

batch_anomalies = (
    batch_df
    .filter(col("anomaly") == True)
    .select(
        "mmsi",
        "timestamp",
        "latitude",
        "longitude",
        "sog",
        "cog",
        "avg_speed",
        "speed_stddev",
        "anomaly_type"
    )
)

anomaly_count = batch_anomalies.count()

print(f"Batch anomalies: {anomaly_count:,}")


# ============================================================
# 10. WRITE BATCH ANOMALIES
# ============================================================

print("\n[10/10] Writing batch anomalies...")

(
    batch_anomalies
    .write
    .mode("overwrite")
    .parquet(BATCH_ANOMALY_OUTPUT_PATH)
)


# ============================================================
# FINAL
# ============================================================

print("\n" + "=" * 70)
print("MARITIME BATCH PROCESSING COMPLETED")
print("=" * 70)

print(f"Input records       : {input_count:,}")
print(f"Clean records       : {clean_count:,}")
print(f"Historical vessels  : {baseline_count:,}")
print(f"Batch anomalies     : {anomaly_count:,}")

print(f"AIS output          : {AIS_OUTPUT_PATH}")
print(f"Baseline output     : {BASELINE_OUTPUT_PATH}")
print(f"Anomaly output      : {BATCH_ANOMALY_OUTPUT_PATH}")

print("=" * 70)

baseline_df.unpersist()

spark.stop()