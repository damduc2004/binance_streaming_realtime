"""
Generate 525,600 rows cho dim_datetime (365 ngày × 1440 phút).
Chạy một lần bởi Airflow dag_init_dimensions hoặc thủ công.

Chạy: python -m scripts.generate_dim_datetime
"""
import os
import psycopg2
from datetime import datetime, date, timedelta, timezone

DB_URL = os.getenv("DATABASE_URL", "postgresql://binance:binance@localhost:5432/binance_dw")


def trading_session(hour: int) -> str:
    if 0 <= hour < 8:
        return "Asia"
    elif 8 <= hour < 16:
        return "Europe"
    else:
        return "US"


def generate_rows(start: date, end: date):
    """Yield một row dict cho mỗi phút trong khoảng [start, end)."""
    current = datetime(start.year, start.month, start.day, tzinfo=timezone.utc)
    end_dt  = datetime(end.year, end.month, end.day, tzinfo=timezone.utc)

    while current < end_dt:
        dow = current.weekday()  # 0=Mon, 6=Sun
        yield {
            "datetime_key":    int(current.strftime("%Y%m%d%H%M")),
            "full_timestamp":  current.replace(tzinfo=None),
            "date":            current.date(),
            "year":            current.year,
            "quarter":         (current.month - 1) // 3 + 1,
            "month":           current.month,
            "week_of_year":    int(current.strftime("%W")),
            "day_of_month":    current.day,
            "day_of_week":     dow,
            "is_weekend":      dow >= 5,
            "hour":            current.hour,
            "minute":          current.minute,
            "trading_session": "Off" if dow >= 5 else trading_session(current.hour),
        }
        current += timedelta(minutes=1)


def main():
    conn = psycopg2.connect(DB_URL)
    cur  = conn.cursor()

    # 2026 + 1 năm buffer về trước (2025) để có lịch sử
    start = date(2025, 1, 1)
    end   = date(2027, 1, 1)

    SQL = """
        INSERT INTO dim_datetime (
            datetime_key, full_timestamp, date, year, quarter, month,
            week_of_year, day_of_month, day_of_week, is_weekend,
            hour, minute, trading_session
        ) VALUES (
            %(datetime_key)s, %(full_timestamp)s, %(date)s, %(year)s,
            %(quarter)s, %(month)s, %(week_of_year)s, %(day_of_month)s,
            %(day_of_week)s, %(is_weekend)s, %(hour)s, %(minute)s,
            %(trading_session)s
        )
        ON CONFLICT (datetime_key) DO NOTHING
    """

    batch, BATCH_SIZE = [], 5000
    total = 0

    for row in generate_rows(start, end):
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            cur.executemany(SQL, batch)
            conn.commit()
            total += len(batch)
            print(f"  Inserted {total:,} rows...")
            batch.clear()

    if batch:
        cur.executemany(SQL, batch)
        conn.commit()
        total += len(batch)

    print(f"Done — {total:,} rows inserted into dim_datetime.")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
