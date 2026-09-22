from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "keerthi",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_elt_pipeline",
    default_args=default_args,
    description="Daily NSE stock ELT pipeline: ingest -> transform -> load -> quality checks",
    schedule="0 18 * * 1-5",  # 6 PM IST, weekdays (after market close)
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["stock", "elt"],
) as dag:

    ingest = BashOperator(
        task_id="ingest",
        bash_command="cd /opt/airflow && python src/ingest.py",
    )

    transform = BashOperator(
        task_id="transform",
        bash_command="cd /opt/airflow && python src/transform.py",
    )

    load = BashOperator(
        task_id="load",
        bash_command="cd /opt/airflow && python src/load.py",
    )

    quality_check = BashOperator(
        task_id="quality_checks",
        bash_command="cd /opt/airflow && python src/quality_checks.py",
    )

    ingest >> transform >> load >> quality_check