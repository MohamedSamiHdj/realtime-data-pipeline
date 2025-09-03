from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "Andy",
    "retries": 0,
    "depends_on_past": False,
}

with DAG(
    "nyc_taxi_batch_etl",
    start_date=datetime(2024,1,1),
    schedule="0 3 * * *",  # daily 03:00
    catchup=False,
    default_args=default_args,
    tags=["batch","pyspark"],
) as dag:

    fetch = BashOperator(
        task_id="fetch_data",
        bash_command="python scripts/fetch_data.py"
    )

    batch = BashOperator(
        task_id="spark_batch_etl",
        bash_command="python spark_jobs/batch_etl.py"
    )

    dq = BashOperator(
        task_id="dq_checks",
        bash_command="python spark_jobs/dq_checks.py"
    )

    fetch >> batch >> dq
