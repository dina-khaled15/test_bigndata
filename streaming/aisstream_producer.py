# import asyncio
# import json
# import os
# from datetime import datetime, timezone

# import websockets
# from kafka import KafkaProducer


# AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"

# KAFKA_BOOTSTRAP_SERVERS = os.getenv(
#     "KAFKA_BOOTSTRAP_SERVERS",
#     "kafka:9092",
# )

# KAFKA_TOPIC = "ais-events"

# API_KEY = os.getenv("AISSTREAM_API_KEY")


# if not API_KEY:
#     raise RuntimeError("AISSTREAM_API_KEY is not set")


# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#     value_serializer=lambda value: json.dumps(value).encode("utf-8"),
# )


# async def stream_ais():

#     async with websockets.connect(AISSTREAM_URL) as websocket:

#         subscription = {
#             "APIKey": API_KEY,
#             "BoundingBoxes": [
#                 [
#                     [-10.0, -30.0],
#                     [60.0, 60.0],
#                 ]
#             ],
#             "FilterMessageTypes": [
#                 "PositionReport",
#             ],
#         }

#         await websocket.send(json.dumps(subscription))

#         print("Connected to AISStream")
#         print("Subscription sent")
#         print("Streaming AIS PositionReports to Kafka...")

#         async for message in websocket:

#             print("RAW AISSTREAM MESSAGE:")
#             print(message)

#             data = json.loads(message)

#             if "error" in data:
#                 print(f"AISStream ERROR: {data['error']}")
#                 continue

#             if data.get("MessageType") != "PositionReport":
#                 print(f"Skipping message type: {data.get('MessageType')}")
#                 continue

#             position = data["Message"]["PositionReport"]

#             event = {
#                 "mmsi": position.get("UserID"),
#                 "base_date_time": datetime.now(timezone.utc).strftime(
#                     "%Y-%m-%d %H:%M:%S"
#                 ),
#                 "longitude": position.get("Longitude"),
#                 "latitude": position.get("Latitude"),
#                 "sog": position.get("Sog"),
#                 "cog": position.get("Cog"),
#             }

#             producer.send(
#                 KAFKA_TOPIC,
#                 value=event,
#             )

#             producer.flush()

#             print(
#                 f"Sent AIS event: "
#                 f"MMSI={event['mmsi']} "
#                 f"lat={event['latitude']} "
#                 f"lon={event['longitude']}"
#             )


# if __name__ == "__main__":
#     asyncio.run(stream_ais())


import json
import time
import os

import requests
import pandas as pd
from kafka import KafkaProducer


# ==============================
# Kafka Configuration
# ==============================

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "ais-events"


# ==============================
# HDFS Configuration
# ==============================

HDFS_FILE = "/maritime/raw/2024/ais-2024-01-01.csv.zst"

LOCAL_FILE = "/tmp/ais-2024-01-01.csv.zst"


# ==============================
# Kafka Producer
# ==============================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)


# ==============================
# Download file from HDFS
# ==============================

def download_from_hdfs():

    print("==============================")
    print("Downloading AIS data from HDFS...")
    print("==============================")

    url = (
        "http://namenode:9870/webhdfs/v1"
        f"{HDFS_FILE}?op=OPEN"
    )

    with requests.get(
        url,
        allow_redirects=True,
        stream=True,
        timeout=300
    ) as response:

        response.raise_for_status()

        with open(LOCAL_FILE, "wb") as file:

            for chunk in response.iter_content(
                chunk_size=1024 * 1024
            ):
                if chunk:
                    file.write(chunk)

    print("Download completed successfully")

    print(
        f"Local file size: "
        f"{os.path.getsize(LOCAL_FILE) / (1024 * 1024):.2f} MB"
    )


# ==============================
# Clean values
# ==============================

def clean_value(value):

    if pd.isna(value):
        return None

    if hasattr(value, "item"):
        return value.item()

    return value


# ==============================
# Stream CSV to Kafka
# ==============================

def stream_csv():

    download_from_hdfs()

    print("==============================")
    print("Reading AIS CSV...")
    print("==============================")

    df = pd.read_csv(
        LOCAL_FILE,
        compression="zstd",
        nrows=10000
    )

    print(f"Rows loaded: {len(df)}")

    print("==============================")
    print("Columns in CSV:")
    print(df.columns.tolist())
    print("==============================")

    print("First 5 rows:")
    print(df.head())

    print("==============================")
    print("Starting Kafka streaming...")
    print("==============================")


    for index, row in df.iterrows():

        # IMPORTANT:
        # The actual columns in our dataset are lowercase.

        event = {

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
            )
        }


        # Skip rows without basic position information

        if (
            event["mmsi"] is None
            and event["latitude"] is None
            and event["longitude"] is None
        ):

            print(
                f"Skipping invalid row {index}"
            )

            continue


        # Send event to Kafka

        future = producer.send(
            KAFKA_TOPIC,
            value=event
        )


        # Wait for Kafka confirmation

        future.get(timeout=10)


        print(
            f"Sent row {index} | "
            f"MMSI={event['mmsi']} | "
            f"LAT={event['latitude']} | "
            f"LON={event['longitude']}"
        )


        # Simulate streaming

        time.sleep(0.2)


    producer.flush()

    print("==============================")
    print("Finished sending AIS data")
    print("==============================")


# ==============================
# Main
# ==============================

if __name__ == "__main__":

    try:

        stream_csv()

    except Exception as error:

        print("==============================")
        print("PRODUCER ERROR")
        print("==============================")

        print(error)

        raise

    finally:

        producer.close()