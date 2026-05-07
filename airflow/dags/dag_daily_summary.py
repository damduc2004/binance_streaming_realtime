"""
DAG: dag_daily_summary
Schedule: 00:05 UTC mỗi ngày
Tổng hợp OHLCV ngày, ranking, thống kê phiên châu Á/Âu/Mỹ.
Trigger dbt refresh mart_daily_performance.
"""
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, date, timezone, datetime

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://binance:binance@postgres:5432/binance_dw")
DBT_DIR      = "/dbt"


def compute_daily_stats():
    """Log tóm tắt ngày hôm qua."""
    import psycopg2
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()

    yesterday = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()

    cur.execute("""
        SELECT
            s.symbol,
            MIN(k.low)::FLOAT   AS day_low,
            MAX(k.high)::FLOAT  AS day_high,
            SUM(k.volume)::FLOAT AS day_volume,
            ROUND(
                (MAX(k.close) - MIN(k.open)) / MIN(k.open) * 100, 4
            )                   AS day_change_pct
        FROM fact_kline_closed k
        JOIN dim_symbol s ON k.symbol_key = s.symbol_key
        WHERE d.date = %s
        -- join dim_datetime để lọc theo date
        JOIN dim_datetime d ON k.datetime_key = d.datetime_key
        GROUP BY s.symbol
        ORDER BY day_change_pct DESC
    """, (yesterday,))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    print(f"\n=== Daily Summary: {yesterday} ===")
    for symbol, low, high, volume, chg in rows:
        print(f"  {symbol:12s} low={low:12.4f}  high={high:12.4f}  "
              f"vol={volume:16.4f}  change={chg:+.2f}%")


with DAG(
    dag_id="dag_daily_summary",
    schedule_interval="5 0 * * *",   # 00:05 UTC
    start_date=days_ago(1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["summary"],
) as dag:

    t_stats = PythonOperator(
        task_id="compute_daily_stats",
        python_callable=compute_daily_stats,
    )

    t_dbt_daily = BashOperator(
        task_id="dbt_refresh_daily_performance",
        bash_command=f"dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} "
                     f"--select mart_daily_performance",
        env={"PGHOST": "postgres", "PGUSER": "binance", "PGPASSWORD": "binance"},
    )

    t_stats >> t_dbt_daily
