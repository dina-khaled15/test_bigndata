from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="maritime_streaming_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["maritime", "spark", "kafka", "streaming"],
) as dag:

    start_streaming = BashOperator(
        task_id="start_ais_streaming",

        bash_command="""
        docker exec -d maritime-spark-master \
          /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          /opt/spark-apps/jobs/ais_streaming.py
        """,
    )

    start_streaming