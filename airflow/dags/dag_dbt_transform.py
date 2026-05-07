"""
DAG: dag_dbt_transform
Schedule: mỗi 5 phút
Chạy dbt run → dbt test để cập nhật mart tables.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

DBT_DIR = "/dbt"

with DAG(
    dag_id="dag_dbt_transform",
    schedule_interval="*/5 * * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["transform", "dbt"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"/home/airflow/.local/bin/dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        env={
            "PGHOST":     "postgres",
            "PGUSER":     "binance",
            "PGPASSWORD": "binance",
        },
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/home/airflow/.local/bin/dbt test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        env={
            "PGHOST":     "postgres",
            "PGUSER":     "binance",
            "PGPASSWORD": "binance",
        },
    )

    dbt_run >> dbt_test
