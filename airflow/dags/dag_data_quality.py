"""
DAG: dag_data_quality
Schedule: mỗi giờ
Kiểm tra null rate, duplicate rate, late data rate, gap detection.
"""
import os
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://binance:binance@postgres:5432/binance_dw")

NULL_RATE_THRESHOLD    = 0.001   # 0.1%
LATE_DATA_THRESHOLD    = 0.01    # 1%
DUP_RATE_THRESHOLD     = 0.0001  # 0.01%


def check_null_rate():
    import psycopg2
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()
    hour_ago_ms = int(time.time() * 1000) - 3600 * 1000

    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE vwap IS NULL OR close IS NULL) AS null_count,
            COUNT(*) AS total
        FROM fact_trade_agg
        WHERE window_start > %s
    """, (hour_ago_ms,))
    null_cnt, total = cur.fetchone()
    cur.close()
    conn.close()

    if total == 0:
        return
    rate = null_cnt / total
    print(f"  Null rate: {rate:.4%} ({null_cnt}/{total})")
    if rate > NULL_RATE_THRESHOLD:
        raise ValueError(f"Null rate {rate:.4%} exceeds threshold {NULL_RATE_THRESHOLD:.4%}")


def check_late_data_rate():
    """Tỷ lệ record có ingested_at - trade_time > 5s."""
    import psycopg2
    # Proxy: kiểm tra window gap — nếu có khoảng trống > 5s thì có thể late data
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()
    hour_ago_ms = int(time.time() * 1000) - 3600 * 1000

    cur.execute("""
        SELECT symbol_key, COUNT(*) AS gaps
        FROM (
            SELECT
                symbol_key,
                window_start,
                LAG(window_start) OVER (
                    PARTITION BY symbol_key ORDER BY window_start
                ) AS prev_window
            FROM fact_trade_agg
            WHERE window_start > %s AND window_key = 1
        ) sub
        WHERE window_start - prev_window > 5000   -- gap > 5 giây
        GROUP BY symbol_key
    """, (hour_ago_ms,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    for sym_key, gaps in rows:
        print(f"  Symbol {sym_key}: {gaps} gaps > 5s in last hour")


def check_duplicate_rate():
    import psycopg2
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()
    hour_ago_ms = int(time.time() * 1000) - 3600 * 1000

    # UNIQUE constraint nghĩa là duplicates = 0 (ON CONFLICT DO NOTHING đã xử lý)
    # Kiểm tra gián tiếp: đếm số record vs số unique (symbol, window_start, window_key)
    cur.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT (symbol_key, window_start, window_key)) AS unique_count
        FROM fact_trade_agg
        WHERE window_start > %s
    """, (hour_ago_ms,))
    total, unique_count = cur.fetchone()
    cur.close()
    conn.close()

    dups = total - unique_count
    rate = dups / total if total else 0
    print(f"  Duplicate rate: {rate:.6%} ({dups} dups in {total} records)")
    if rate > DUP_RATE_THRESHOLD:
        raise ValueError(f"Duplicate rate {rate:.6%} too high")


with DAG(
    dag_id="dag_data_quality",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["quality"],
) as dag:

    t_null = PythonOperator(
        task_id="check_null_rate",
        python_callable=check_null_rate,
    )

    t_late = PythonOperator(
        task_id="check_late_data_rate",
        python_callable=check_late_data_rate,
    )

    t_dup = PythonOperator(
        task_id="check_duplicate_rate",
        python_callable=check_duplicate_rate,
    )

    [t_null, t_late, t_dup]
