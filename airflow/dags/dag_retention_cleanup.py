"""
DAG: dag_retention_cleanup
Schedule: 02:00 UTC mỗi ngày
Xóa dữ liệu cũ hơn retention policy, sau đó VACUUM ANALYZE.

Retention:
  - fact_trade_agg       90 ngày
  - fact_order_flow      90 ngày
  - fact_kline_closed    90 ngày
  - fact_technical_indicator 90 ngày
  - fact_spread_snapshot 30 ngày
  - fact_price_alert     90 ngày
"""
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime, timezone

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://binance:binance@postgres:5432/binance_dw")

RETENTION = {
    "fact_trade_agg":           ("window_start",   90),
    "fact_order_flow":          ("window_start",   90),
    "fact_kline_closed":        ("open_time",      90),
    "fact_technical_indicator": ("open_time",      90),
    "fact_spread_snapshot":     ("snapshot_time",  30),
    "fact_price_alert":         ("triggered_at",   90),
}


def cleanup_old_data():
    import psycopg2
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    total_deleted = 0

    for table, (ts_col, days) in RETENTION.items():
        cutoff_ms = now_ms - days * 24 * 3600 * 1000
        cur.execute(f"""
            DELETE FROM {table} WHERE {ts_col} < %s
        """, (cutoff_ms,))
        deleted = cur.rowcount
        total_deleted += deleted
        print(f"  {table}: deleted {deleted:,} rows (cutoff={days} days)")

    conn.commit()
    print(f"\nTotal deleted: {total_deleted:,} rows")
    cur.close()
    conn.close()


def vacuum_analyze():
    import psycopg2
    # VACUUM ANALYZE không thể chạy trong transaction block
    conn = psycopg2.connect(DATABASE_URL)
    conn.set_isolation_level(0)   # AUTOCOMMIT
    cur = conn.cursor()
    for table in RETENTION:
        print(f"  VACUUM ANALYZE {table}...")
        cur.execute(f"VACUUM ANALYZE {table}")
    cur.close()
    conn.close()
    print("VACUUM ANALYZE complete.")


with DAG(
    dag_id="dag_retention_cleanup",
    schedule_interval="0 2 * * *",   # 02:00 UTC
    start_date=days_ago(1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["maintenance"],
) as dag:

    t_cleanup = PythonOperator(
        task_id="cleanup_old_data",
        python_callable=cleanup_old_data,
    )

    t_vacuum = PythonOperator(
        task_id="vacuum_analyze",
        python_callable=vacuum_analyze,
    )

    t_cleanup >> t_vacuum
