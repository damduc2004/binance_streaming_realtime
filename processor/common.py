"""
Shared utilities cho tất cả Spark jobs:
- DB connection helpers
- Schema lookup (symbol_key, window_key, interval_key)
- datetime_key calculator
- Data quality filter
"""
import os
from datetime import datetime, timezone
from functools import lru_cache

import psycopg2

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://binance:binance@postgres:5432/binance_dw",
)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_DIR", "/opt/spark-checkpoints")

# Parse: postgresql://user:pass@host:port/db
_db_url_body = DATABASE_URL.split("://")[1]          # user:pass@host:port/db
_db_userinfo, _db_hostpart = _db_url_body.split("@") # user:pass | host:port/db
_db_user, _db_pass = _db_userinfo.split(":", 1)
_db_host_port, _db_name = _db_hostpart.split("/", 1)

JDBC_URL = f"jdbc:postgresql://{_db_host_port}/{_db_name}"
JDBC_PROPS = {
    "user":     _db_user,
    "password": _db_pass,
    "driver":   "org.postgresql.Driver",
    "batchsize": "1000",
}


def get_conn():
    return psycopg2.connect(DATABASE_URL)


@lru_cache(maxsize=1)
def load_symbol_map() -> dict:
    """{'BTCUSDT': 1, 'ETHUSDT': 2, ...}"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol, symbol_key FROM dim_symbol WHERE is_active = true")
            return {row[0]: row[1] for row in cur.fetchall()}


@lru_cache(maxsize=1)
def load_window_map() -> dict:
    """{'1s': 1, '5s': 2}"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT window_label, window_key FROM dim_window_type")
            return {row[0]: row[1] for row in cur.fetchall()}


@lru_cache(maxsize=1)
def load_interval_map() -> dict:
    """{'1m': 1}"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT interval_label, interval_key FROM dim_kline_interval")
            return {row[0]: row[1] for row in cur.fetchall()}


@lru_cache(maxsize=1)
def load_alert_type_map() -> dict:
    """{'price_spike': 1, ...}"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT alert_code, alert_type_key FROM dim_alert_type")
            return {row[0]: row[1] for row in cur.fetchall()}


def epoch_ms_to_datetime_key(epoch_ms: int) -> int:
    """Chuyển epoch milliseconds sang datetime_key (YYYYMMDDHHmm)."""
    dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
    return int(dt.strftime("%Y%m%d%H%M"))
