"""
Spark Job 4 — AlertDetector
-----------------------------
Query PostgreSQL mỗi 5 giây, phát hiện 4 loại bất thường:
  1. price_spike      — Giá thay đổi > 1% trong 5 giây
  2. volume_surge     — Volume > 3x trung bình 30 phút
  3. spread_widening  — Spread > 2x trung bình 1 giờ
  4. low_liquidity    — Bid+ask qty < 20% trung bình 24 giờ

Ghi cảnh báo vào fact_price_alert.

Không dùng Spark Streaming — chạy như một scheduled Python process
(Airflow hoặc cron mỗi 5 giây). Nhẹ hơn, dễ debug hơn so với
Spark foreachBatch với trigger 5s.

Chạy: python -m processor.job4_alert_detect
"""
import os
import time
import logging
from datetime import datetime, timezone

from processor.common import (
    get_conn,
    load_symbol_map,
    load_alert_type_map,
    epoch_ms_to_datetime_key,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] AlertDetect | %(message)s",
)
log = logging.getLogger(__name__)

INTERVAL_SEC = int(os.getenv("ALERT_INTERVAL_SEC", "5"))

# Ngưỡng phát hiện
PRICE_SPIKE_PCT   = float(os.getenv("PRICE_SPIKE_PCT",   "1.0"))   # %
VOLUME_SURGE_X    = float(os.getenv("VOLUME_SURGE_X",    "3.0"))   # lần
SPREAD_WIDEN_X    = float(os.getenv("SPREAD_WIDEN_X",    "2.0"))   # lần
LOW_LIQ_PCT       = float(os.getenv("LOW_LIQ_PCT",       "20.0"))  # % của baseline


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def detect_price_spikes(cur, symbol_map: dict, alert_type_map: dict) -> list:
    """So sánh giá close 5 giây gần nhất với 5 giây trước đó."""
    rows = []
    for symbol, sym_key in symbol_map.items():
        cur.execute("""
            SELECT close, window_start
            FROM fact_trade_agg
            WHERE symbol_key = %s AND window_key = (
                SELECT window_key FROM dim_window_type WHERE window_label = '5s'
            )
            ORDER BY window_start DESC
            LIMIT 2
        """, (sym_key,))
        results = cur.fetchall()
        if len(results) < 2:
            continue
        close_now, ws_now  = results[0]
        close_prev, _      = results[1]
        if close_prev == 0:
            continue
        pct = abs(float(close_now) - float(close_prev)) / float(close_prev) * 100
        if pct >= PRICE_SPIKE_PCT:
            severity = "HIGH" if pct >= 2.0 else "MEDIUM"
            rows.append((
                sym_key,
                epoch_ms_to_datetime_key(ws_now),
                alert_type_map["price_spike"],
                ws_now,
                float(close_now),
                round(pct, 6),
                severity,
            ))
            log.info("price_spike %s pct=%.2f%%", symbol, pct)
    return rows


def detect_volume_surges(cur, symbol_map: dict, alert_type_map: dict) -> list:
    """Volume window 5s gần nhất > 3x trung bình 30 phút trước."""
    rows = []
    now = now_ms()
    thirty_min_ago = now - 30 * 60 * 1000

    for symbol, sym_key in symbol_map.items():
        cur.execute("""
            SELECT AVG(volume) FROM fact_trade_agg
            WHERE symbol_key = %s
              AND window_key = (SELECT window_key FROM dim_window_type WHERE window_label = '5s')
              AND window_start BETWEEN %s AND %s
        """, (sym_key, thirty_min_ago, now))
        avg_row = cur.fetchone()
        if not avg_row or avg_row[0] is None:
            continue
        avg_vol = float(avg_row[0])

        cur.execute("""
            SELECT volume, window_start FROM fact_trade_agg
            WHERE symbol_key = %s AND window_key = (
                SELECT window_key FROM dim_window_type WHERE window_label = '5s'
            )
            ORDER BY window_start DESC LIMIT 1
        """, (sym_key,))
        latest = cur.fetchone()
        if not latest or avg_vol == 0:
            continue
        curr_vol, ws = latest
        ratio = float(curr_vol) / avg_vol
        if ratio >= VOLUME_SURGE_X:
            severity = "HIGH" if ratio >= 5.0 else "MEDIUM"
            rows.append((
                sym_key,
                epoch_ms_to_datetime_key(ws),
                alert_type_map["volume_surge"],
                ws,
                float(curr_vol),
                round(ratio, 6),
                severity,
            ))
            log.info("volume_surge %s ratio=%.1fx", symbol, ratio)
    return rows


def detect_spread_widening(cur, symbol_map: dict, alert_type_map: dict) -> list:
    """Spread hiện tại > 2x trung bình 1 giờ trước."""
    rows = []
    now = now_ms()
    one_hour_ago = now - 60 * 60 * 1000

    for symbol, sym_key in symbol_map.items():
        cur.execute("""
            SELECT AVG(spread) FROM fact_spread_snapshot
            WHERE symbol_key = %s AND snapshot_time BETWEEN %s AND %s
        """, (sym_key, one_hour_ago, now))
        avg_row = cur.fetchone()
        if not avg_row or avg_row[0] is None:
            continue
        avg_spread = float(avg_row[0])

        cur.execute("""
            SELECT spread, snapshot_time FROM fact_spread_snapshot
            WHERE symbol_key = %s ORDER BY snapshot_time DESC LIMIT 1
        """, (sym_key,))
        latest = cur.fetchone()
        if not latest or avg_spread == 0:
            continue
        curr_spread, snap_t = latest
        ratio = float(curr_spread) / avg_spread
        if ratio >= SPREAD_WIDEN_X:
            rows.append((
                sym_key,
                epoch_ms_to_datetime_key(snap_t),
                alert_type_map["spread_widening"],
                snap_t,
                float(curr_spread),
                round(ratio, 6),
                "MEDIUM",
            ))
            log.info("spread_widening %s ratio=%.1fx", symbol, ratio)
    return rows


def detect_low_liquidity(cur, symbol_map: dict, alert_type_map: dict) -> list:
    """Bid+ask qty hiện tại < 20% trung bình 24 giờ."""
    rows = []
    now = now_ms()
    one_day_ago = now - 24 * 60 * 60 * 1000

    for symbol, sym_key in symbol_map.items():
        cur.execute("""
            SELECT AVG(bid_qty + ask_qty) FROM fact_spread_snapshot
            WHERE symbol_key = %s AND snapshot_time BETWEEN %s AND %s
        """, (sym_key, one_day_ago, now))
        avg_row = cur.fetchone()
        if not avg_row or avg_row[0] is None:
            continue
        avg_total_qty = float(avg_row[0])

        cur.execute("""
            SELECT bid_qty + ask_qty, snapshot_time FROM fact_spread_snapshot
            WHERE symbol_key = %s ORDER BY snapshot_time DESC LIMIT 1
        """, (sym_key,))
        latest = cur.fetchone()
        if not latest or avg_total_qty == 0:
            continue
        curr_qty, snap_t = latest
        pct = float(curr_qty) / avg_total_qty * 100
        if pct < LOW_LIQ_PCT:
            rows.append((
                sym_key,
                epoch_ms_to_datetime_key(snap_t),
                alert_type_map["low_liquidity"],
                snap_t,
                float(curr_qty),
                round(pct, 6),
                "LOW",
            ))
            log.info("low_liquidity %s pct=%.1f%%", symbol, pct)
    return rows


def insert_alerts(cur, alert_rows: list):
    if not alert_rows:
        return
    cur.executemany("""
        INSERT INTO fact_price_alert
            (symbol_key, datetime_key, alert_type_key,
             triggered_at, trigger_value, threshold_pct, severity)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (symbol_key, alert_type_key, triggered_at) DO NOTHING
    """, alert_rows)


def run():
    symbol_map     = load_symbol_map()
    alert_type_map = load_alert_type_map()

    log.info("AlertDetector started — interval=%ds, symbols=%d", INTERVAL_SEC, len(symbol_map))

    while True:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    all_alerts = (
                        detect_price_spikes(cur, symbol_map, alert_type_map) +
                        detect_volume_surges(cur, symbol_map, alert_type_map) +
                        detect_spread_widening(cur, symbol_map, alert_type_map) +
                        detect_low_liquidity(cur, symbol_map, alert_type_map)
                    )
                    insert_alerts(cur, all_alerts)
                conn.commit()

            if all_alerts:
                log.info("Inserted %d alerts", len(all_alerts))

        except Exception as e:
            log.error("Detection error: %s", e)

        time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    run()
