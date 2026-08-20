from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="maritime_batch_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["maritime", "spark", "ais"],
) as dag:

    process_ais = BashOperator(
        task_id="process_ais_csv_to_parquet",
        bash_command="""
        docker exec maritime-spark-master \
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-apps/jobs/process_ais.py
        """,
    )

    process_ais