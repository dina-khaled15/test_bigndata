from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    to_timestamp,
    when,
    lit
)

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType
)


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "ais-events"

BASELINE_PATH = (
    "hdfs://namenode:9000/maritime/processed/baseline"
)

STREAMING_OUTPUT_PATH = (
    "hdfs://namenode:9000/maritime/streaming"
)

STREAMING_ANOMALY_PATH = (
    "hdfs://namenode:9000/maritime/streaming/anomalies"
)

REJECTED_PATH = (
    "hdfs://namenode:9000/maritime/rejected"
)

CHECKPOINT_PATH = (
    "hdfs://namenode:9000/maritime/checkpoints/ais-streaming"
)

ANOMALY_CHECKPOINT_PATH = (
    "hdfs://namenode:9000/maritime/checkpoints/ais-anomalies"
)

REJECTED_CHECKPOINT_PATH = (
    "hdfs://namenode:9000/maritime/checkpoints/ais-rejected"
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("MaritimeAISStreaming")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# SCHEMA
# ============================================================

schema = StructType([

    StructField(
        "mmsi",
        LongType(),
        True
    ),

    StructField(
        "base_date_time",
        StringType(),
        True
    ),

    StructField(
        "longitude",
        DoubleType(),
        True
    ),

    StructField(
        "latitude",
        DoubleType(),
        True
    ),

    StructField(
        "sog",
        DoubleType(),
        True
    ),

    StructField(
        "cog",
        DoubleType(),
        True
    )
])


# ============================================================
# READ KAFKA
# ============================================================

print("=" * 70)
print("MARITIME AIS STREAMING")
print("=" * 70)

print("Reading from Kafka...")
print(f"Bootstrap: {KAFKA_BOOTSTRAP}")
print(f"Topic: {KAFKA_TOPIC}")


raw = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP
    )
    .option(
        "subscribe",
        KAFKA_TOPIC
    )
    .option(
        "startingOffsets",
        "latest"
    )
    .load()
)


# ============================================================
# PARSE JSON
# ============================================================

parsed = raw.select(

    col("value")
    .cast("string")
    .alias("raw_value"),

    col("topic"),

    col("partition"),

    col("offset"),

    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
)


# ============================================================
# VALID RECORDS
# ============================================================

valid = (
    parsed

    .filter(
        col("data").isNotNull()
    )

    .select(
        "data.*",
        "topic",
        "partition",
        "offset"
    )

    .withColumn(
        "base_date_time",
        to_timestamp(
            col("base_date_time"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )

    .withColumn(
        "processed_at",
        current_timestamp()
    )
)


# ============================================================
# REJECTED RECORDS
# ============================================================

rejected = (
    parsed

    .filter(
        col("data").isNull()
    )

    .select(
        "raw_value",
        "topic",
        "partition",
        "offset"
    )

    .withColumn(
        "rejected_at",
        current_timestamp()
    )
)


# ============================================================
# LOAD HISTORICAL BASELINE
# ============================================================

print("Loading historical vessel baseline...")

baseline = spark.read.parquet(
    BASELINE_PATH
)

baseline = baseline.select(
    "mmsi",
    "avg_speed",
    "speed_stddev",
    "avg_course",
    "course_stddev",
    "min_speed",
    "max_speed"
)

baseline.cache()

print("Historical baseline loaded.")


# ============================================================
# STREAMING ANOMALY DETECTION
# ============================================================

def detect_anomalies(batch_df, batch_id):

    print("=" * 60)
    print(f"Processing micro-batch: {batch_id}")

    if batch_df.rdd.isEmpty():
        print("Empty micro-batch.")
        return

    # --------------------------------------------------------
    # Join streaming records with historical baseline
    # --------------------------------------------------------

    enriched = (
        batch_df
        .join(
            baseline,
            on="mmsi",
            how="left"
        )
    )

    # --------------------------------------------------------
    # Speed bounds
    # --------------------------------------------------------

    enriched = (
        enriched

        .withColumn(
            "speed_lower_bound",
            when(
                col("speed_stddev") > 0,
                col("avg_speed")
                - (3 * col("speed_stddev"))
            )
            .otherwise(
                col("min_speed")
            )
        )

        .withColumn(
            "speed_upper_bound",
            when(
                col("speed_stddev") > 0,
                col("avg_speed")
                + (3 * col("speed_stddev"))
            )
            .otherwise(
                col("max_speed")
            )
        )
    )

    # --------------------------------------------------------
    # Speed anomaly
    # --------------------------------------------------------

    enriched = (
        enriched

        .withColumn(
            "speed_anomaly",

            when(
                col("avg_speed").isNull(),
                lit(False)
            )

            .otherwise(
                (
                    col("sog")
                    < col("speed_lower_bound")
                )
                |
                (
                    col("sog")
                    > col("speed_upper_bound")
                )
            )
        )
    )

    # --------------------------------------------------------
    # Course anomaly
    # --------------------------------------------------------

    enriched = (
        enriched

        .withColumn(
            "course_lower_bound",
            col("avg_course")
            - (3 * col("course_stddev"))
        )

        .withColumn(
            "course_upper_bound",
            col("avg_course")
            + (3 * col("course_stddev"))
        )

        .withColumn(
            "course_anomaly",

            when(
                col("avg_course").isNull(),
                lit(False)
            )

            .otherwise(
                (
                    col("cog")
                    < col("course_lower_bound")
                )
                |
                (
                    col("cog")
                    > col("course_upper_bound")
                )
            )
        )
    )

    # --------------------------------------------------------
    # Final anomaly
    # --------------------------------------------------------

    enriched = (
        enriched

        .withColumn(
            "anomaly",
            col("speed_anomaly")
            |
            col("course_anomaly")
        )

        .withColumn(
            "anomaly_type",

            when(
                col("speed_anomaly")
                & col("course_anomaly"),

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

        .withColumn(
            "streaming_batch_id",
            lit(batch_id)
        )
    )

    # --------------------------------------------------------
    # Only anomalies
    # --------------------------------------------------------

    anomalies = (
        enriched

        .filter(
            col("anomaly") == True
        )

        .select(
            "mmsi",
            "base_date_time",
            "longitude",
            "latitude",
            "sog",
            "cog",
            "avg_speed",
            "speed_stddev",
            "anomaly_type",
            "processed_at",
            "streaming_batch_id"
        )
    )

    anomaly_count = anomalies.count()

    print(
        f"Streaming anomalies in batch "
        f"{batch_id}: {anomaly_count}"
    )

    if anomaly_count > 0:

        (
            anomalies
            .write
            .mode("append")
            .parquet(
                STREAMING_ANOMALY_PATH
            )
        )


# ============================================================
# WRITE NORMAL STREAM
# ============================================================

valid_query = (
    valid.writeStream

    .format("parquet")

    .outputMode("append")

    .option(
        "path",
        STREAMING_OUTPUT_PATH
    )

    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )

    .start()
)


# ============================================================
# WRITE STREAMING ANOMALIES
# ============================================================

anomaly_query = (
    valid.writeStream

    .foreachBatch(
        detect_anomalies
    )

    .outputMode("update")

    .option(
        "checkpointLocation",
        ANOMALY_CHECKPOINT_PATH
    )

    .start()
)


# ============================================================
# WRITE REJECTED
# ============================================================

rejected_query = (
    rejected.writeStream

    .format("parquet")

    .outputMode("append")

    .option(
        "path",
        REJECTED_PATH
    )

    .option(
        "checkpointLocation",
        REJECTED_CHECKPOINT_PATH
    )

    .start()
)


# ============================================================
# WAIT
# ============================================================

try:

    print("Streaming queries started.")

    spark.streams.awaitAnyTermination()

except KeyboardInterrupt:

    print(
        "Stopping streaming queries..."
    )

finally:

    valid_query.stop()

    anomaly_query.stop()

    rejected_query.stop()

    baseline.unpersist()

    spark.stop()

    print(
        "Spark session stopped."
    )