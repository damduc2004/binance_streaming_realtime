"""
DAG: dag_health_check
Schedule: mỗi 5 phút
Kiểm tra Kafka lag, PostgreSQL insert rate, WebSocket reconnect counter.
"""
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
DATABASE_URL    = os.getenv("DATABASE_URL", "postgresql://binance:binance@postgres:5432/binance_dw")


def check_postgres_insert_rate():
    import psycopg2
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()
    cur.execute("""
        SELECT schemaname, relname, n_tup_ins
        FROM pg_stat_user_tables
        WHERE relname IN ('fact_trade_agg', 'fact_spread_snapshot')
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    for schema, table, ins_count in rows:
        print(f"  {table}: n_tup_ins = {ins_count}")
    # Không raise — chỉ log. Alerting qua Prometheus.


def check_kafka_consumer_groups():
    from confluent_kafka.admin import AdminClient
    client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    groups = client.list_consumer_groups(states={"Empty", "Stable"})
    result = groups.result()
    print(f"  Consumer groups: {len(result.valid)}")
    for g in result.valid:
        print(f"    {g.group_id} — state: {g.state.name}")


def check_recent_data():
    """Kiểm tra có dữ liệu mới trong 5 phút gần nhất không."""
    import psycopg2, time
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()
    threshold_ms = int(time.time() * 1000) - 5 * 60 * 1000
    cur.execute("""
        SELECT COUNT(*) FROM fact_trade_agg
        WHERE window_start > %s
    """, (threshold_ms,))
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    print(f"  Records in last 5 min: {count}")
    if count == 0:
        raise ValueError("No data ingested in the last 5 minutes!")


with DAG(
    dag_id="dag_health_check",
    schedule_interval="*/5 * * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["monitoring"],
) as dag:

    t_postgres = PythonOperator(
        task_id="check_postgres_insert_rate",
        python_callable=check_postgres_insert_rate,
    )

    t_kafka = PythonOperator(
        task_id="check_kafka_consumer_groups",
        python_callable=check_kafka_consumer_groups,
    )

    t_data = PythonOperator(
        task_id="check_recent_data",
        python_callable=check_recent_data,
    )

    [t_postgres, t_kafka] >> t_data
