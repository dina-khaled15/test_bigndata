from datetime import datetime
from pathlib import Path
import subprocess
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator


DOWNLOAD_DIR = Path("/tmp/maritime_noaa")
HDFS_BASE = "/maritime/raw"

FILES = {
    2024: [
        "ais-2024-01-01.csv.zst",
        "ais-2024-01-02.csv.zst",
        "ais-2024-01-03.csv.zst",
        "ais-2024-01-04.csv.zst",
        "ais-2024-01-05.csv.zst",
    ],
    2025: [
        "ais-2025-01-01.csv.zst",
        "ais-2025-01-02.csv.zst",
        "ais-2025-01-03.csv.zst",
        "ais-2025-01-04.csv.zst",
        "ais-2025-01-05.csv.zst",
    ],
}


def download_and_extract():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    for year, filenames in FILES.items():

        for filename in filenames:

            url = (
                f"https://noaaocm.blob.core.windows.net/"
                f"ais/csv2/csv{year}/{filename}"
            )

            compressed_file = DOWNLOAD_DIR / filename

            print("=" * 70)
            print(f"Downloading: {filename}")
            print(f"URL: {url}")

            response = requests.get(
                url,
                stream=True,
                timeout=300
            )

            response.raise_for_status()

            with compressed_file.open("wb") as f:
                for chunk in response.iter_content(
                    chunk_size=1024 * 1024
                ):
                    if chunk:
                        f.write(chunk)

            print(
                f"Downloaded: "
                f"{compressed_file.stat().st_size / (1024**2):.2f} MB"
            )

            # ------------------------------------------------
            # Extract .zst -> .csv
            # ------------------------------------------------

            csv_file = compressed_file.with_suffix("")

            print(f"Extracting: {filename}")

            subprocess.run(
                [
                    "zstd",
                    "-d",
                    "-f",
                    str(compressed_file),
                    "-o",
                    str(csv_file),
                ],
                check=True,
            )

            print(f"Extracted: {csv_file}")

            # Remove compressed file to save space
            compressed_file.unlink()

            print(f"Removed compressed file: {compressed_file}")


def upload_to_hdfs():

    for year, filenames in FILES.items():

        hdfs_directory = f"{HDFS_BASE}/{year}"

        # Create HDFS directory
        subprocess.run(
            [
                "curl",
                "-s",
                "-X",
                "PUT",
                "-L",
                f"http://namenode:9870/webhdfs/v1"
                f"{hdfs_directory}"
                f"?op=MKDIRS",
            ],
            check=True,
        )

        print(f"HDFS directory ready: {hdfs_directory}")

        for filename in filenames:

            csv_filename = filename.replace(".zst", "")

            local_file = DOWNLOAD_DIR / csv_filename

            hdfs_file = (
                f"{hdfs_directory}/{csv_filename}"
            )

            if not local_file.exists():
                raise FileNotFoundError(
                    f"File not found: {local_file}"
                )

            print("=" * 70)
            print(f"Uploading to HDFS:")
            print(f"Local : {local_file}")
            print(f"HDFS  : {hdfs_file}")

            subprocess.run(
                [
                    "curl",
                    "-s",
                    "-X",
                    "PUT",
                    "-L",
                    "-H",
                    "Content-Type: application/octet-stream",
                    f"http://namenode:9870/webhdfs/v1"
                    f"{hdfs_file}"
                    f"?op=CREATE&overwrite=true",
                    "--data-binary",
                    f"@{local_file}",
                ],
                check=True,
            )

            print(f"Uploaded successfully: {hdfs_file}")


with DAG(
    dag_id="noaa_historical_ingestion",

    start_date=datetime(2026, 1, 1),

    schedule=None,

    catchup=False,

    tags=[
        "maritime",
        "noaa",
        "historical",
        "ingestion",
    ],
) as dag:

    download_extract = PythonOperator(
        task_id="download_and_extract_noaa",
        python_callable=download_and_extract,
    )

    upload = PythonOperator(
        task_id="upload_to_hdfs",
        python_callable=upload_to_hdfs,
    )

    download_extract >> upload