"""
DAG: dag_init_dimensions
Schedule: @once
Tạo dim tables và generate 525,600 rows cho dim_datetime.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="dag_init_dimensions",
    schedule_interval="@once",
    start_date=days_ago(1),
    catchup=False,
    tags=["init"],
) as dag:

    def run_generate():
        import subprocess, sys
        result = subprocess.run(
            [sys.executable, "-m", "scripts.generate_dim_datetime"],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        print(result.stdout)

    generate_dim_datetime = PythonOperator(
        task_id="generate_dim_datetime",
        python_callable=run_generate,
    )
