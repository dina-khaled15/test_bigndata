import json
import os
import time

import pandas as pd
import requests
from kafka import KafkaProducer


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "ais-events"

HDFS_FILES = [
    "/maritime/raw/2024/ais-2024-01-01.csv",
    "/maritime/raw/2025/ais-2025-01-01.csv",
]

LOCAL_DIR = "/tmp/ais_data"

# Number of records sent per second
ROWS_PER_SECOND = 200

# Number of rows read from each CSV
ROWS_PER_FILE = 10000

# Print progress every N records
PRINT_EVERY = 200


# ============================================================
# Kafka Producer
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(
        value,
        allow_nan=False
    ).encode("utf-8"),
    linger_ms=5,
    batch_size=65536,
    acks="all",
    retries=5,
)


# ============================================================
# HDFS Download
# ============================================================

def download_from_hdfs(hdfs_file):

    os.makedirs(LOCAL_DIR, exist_ok=True)

    filename = os.path.basename(hdfs_file)
    local_file = os.path.join(LOCAL_DIR, filename)

    print()
    print("=" * 70)
    print("DOWNLOADING FROM HDFS")
    print("=" * 70)
    print(f"HDFS : {hdfs_file}")
    print(f"LOCAL: {local_file}")
    print("=" * 70)

    url = (
        "http://namenode:9870/webhdfs/v1"
        f"{hdfs_file}?op=OPEN"
    )

    try:

        with requests.get(
            url,
            allow_redirects=True,
            stream=True,
            timeout=300,
        ) as response:

            response.raise_for_status()

            with open(local_file, "wb") as file:

                for chunk in response.iter_content(
                    chunk_size=1024 * 1024
                ):

                    if chunk:
                        file.write(chunk)

    except Exception as error:

        print()
        print("HDFS DOWNLOAD ERROR")
        print(error)
        raise

    file_size_mb = os.path.getsize(local_file) / (
        1024 * 1024
    )

    print(
        f"Downloaded successfully: "
        f"{file_size_mb:.2f} MB"
    )

    return local_file


# ============================================================
# Clean Values
# ============================================================

def clean_value(value):

    if value is None:
        return None

    try:

        if pd.isna(value):
            return None

    except (TypeError, ValueError):

        pass

    # Convert NumPy scalar types to native Python types
    if hasattr(value, "item"):

        try:
            value = value.item()

        except Exception:
            pass

    # Convert timestamps to string
    if hasattr(value, "isoformat"):

        try:
            return value.isoformat()

        except Exception:
            pass

    return value


# ============================================================
# Convert CSV Row to Kafka Event
# ============================================================

def row_to_event(row):

    return {
        "mmsi": clean_value(
            row.get("mmsi")
        ),

        "base_date_time": clean_value(
            row.get("base_date_time")
        ),

        "longitude": clean_value(
            row.get("longitude")
        ),

        "latitude": clean_value(
            row.get("latitude")
        ),

        "sog": clean_value(
            row.get("sog")
        ),

        "cog": clean_value(
            row.get("cog")
        ),
    }


# ============================================================
# Validate Event
# ============================================================

def is_valid_event(event):

    if event["mmsi"] is None:
        return False

    if event["latitude"] is None:
        return False

    if event["longitude"] is None:
        return False

    return True


# ============================================================
# Stream One CSV File
# ============================================================

def stream_file(hdfs_file):

    local_file = download_from_hdfs(hdfs_file)

    print()
    print("=" * 70)
    print("READING CSV")
    print("=" * 70)
    print(f"File: {local_file}")
    print("=" * 70)

    try:

        df = pd.read_csv(
            local_file,
            nrows=ROWS_PER_FILE,
        )

    except Exception as error:

        print()
        print("CSV READ ERROR")
        print(error)
        raise

    print(f"Rows loaded: {len(df)}")
    print()
    print("Columns:")
    print(df.columns.tolist())

    required_columns = {
        "mmsi",
        "base_date_time",
        "longitude",
        "latitude",
        "sog",
        "cog",
    }

    missing_columns = required_columns - set(
        df.columns
    )

    if missing_columns:

        raise RuntimeError(
            f"Missing required columns: "
            f"{sorted(missing_columns)}"
        )

    print()
    print("=" * 70)
    print("STARTING KAFKA STREAM")
    print("=" * 70)
    print(f"Topic          : {KAFKA_TOPIC}")
    print(f"Target rate    : {ROWS_PER_SECOND} rows/sec")
    print(f"Maximum rows   : {ROWS_PER_FILE}")
    print("=" * 70)

    sent = 0
    skipped = 0

    start_time = time.time()

    for index, row in df.iterrows():

        event = row_to_event(row)

        # ----------------------------------------------------
        # Validate
        # ----------------------------------------------------

        if not is_valid_event(event):

            skipped += 1

            print(
                f"Skipping invalid row: {index}"
            )

            continue

        # ----------------------------------------------------
        # Send to Kafka
        # ----------------------------------------------------

        try:

            future = producer.send(
                KAFKA_TOPIC,
                value=event,
            )

            # Wait for Kafka confirmation
            # every PRINT_EVERY messages
            # instead of blocking every message.

            if sent % PRINT_EVERY == 0:

                future.get(timeout=10)

        except Exception as error:

            print()
            print("=" * 70)
            print("KAFKA SEND ERROR")
            print("=" * 70)
            print(f"Row: {index}")
            print(error)
            print("=" * 70)

            raise

        sent += 1

        # ----------------------------------------------------
        # Rate limiting
        # ----------------------------------------------------

        target_time = (
            start_time
            + (sent / ROWS_PER_SECOND)
        )

        sleep_time = (
            target_time
            - time.time()
        )

        if sleep_time > 0:

            time.sleep(sleep_time)

        # ----------------------------------------------------
        # Progress
        # ----------------------------------------------------

        if sent % PRINT_EVERY == 0:

            elapsed = (
                time.time()
                - start_time
            )

            rate = (
                sent / elapsed
                if elapsed > 0
                else 0
            )

            print(
                f"[PRODUCER] "
                f"Sent={sent} | "
                f"Skipped={skipped} | "
                f"Rate={rate:.1f} rows/sec | "
                f"MMSI={event['mmsi']} | "
                f"LAT={event['latitude']} | "
                f"LON={event['longitude']}"
            )

    # --------------------------------------------------------
    # Flush remaining Kafka messages
    # --------------------------------------------------------

    print()
    print("Flushing Kafka producer...")

    producer.flush()

    elapsed = (
        time.time()
        - start_time
    )

    final_rate = (
        sent / elapsed
        if elapsed > 0
        else 0
    )

    print()
    print("=" * 70)
    print("FILE STREAM COMPLETED")
    print("=" * 70)
    print(f"Source file : {hdfs_file}")
    print(f"Sent        : {sent}")
    print(f"Skipped     : {skipped}")
    print(f"Elapsed     : {elapsed:.2f} sec")
    print(f"Average rate: {final_rate:.1f} rows/sec")
    print("=" * 70)


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    print()
    print("=" * 70)
    print("MARITIME AIS KAFKA PRODUCER")
    print("=" * 70)
    print(f"Kafka : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic : {KAFKA_TOPIC}")
    print(
        f"Rate  : {ROWS_PER_SECOND} rows/sec"
    )
    print(
        f"Rows/file: {ROWS_PER_FILE}"
    )
    print("=" * 70)

    try:

        for hdfs_file in HDFS_FILES:

            stream_file(hdfs_file)

    except KeyboardInterrupt:

        print()
        print("Producer stopped by user.")

    except Exception as error:

        print()
        print("=" * 70)
        print("PRODUCER ERROR")
        print("=" * 70)
        print(error)
        print("=" * 70)

        raise

    finally:

        print()
        print("Closing Kafka producer...")

        try:
            producer.flush()
        except Exception:
            pass

        try:
            producer.close()
        except Exception:
            pass

        print("Producer closed.")