"""
Spark Job 2 — KlineSink
-------------------------
Đọc từ binance.klines (chỉ is_closed=true) → ghi vào:
  - fact_kline_closed
  - fact_technical_indicator  (RSI-14, MACD, Bollinger Bands, ATR-14, OBV)

Trigger: 1 phút (nến đóng mỗi 60 giây)

Technical indicators được tính bằng pandas/numpy sau khi query
26 nến lịch sử từ PostgreSQL (foreachBatch pattern).

Chạy:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
    processor/job2_kline_sink.py
"""
from typing import Optional
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, BooleanType, IntegerType,
)

from processor.common import (
    KAFKA_BOOTSTRAP, CHECKPOINT_BASE,
    load_symbol_map, load_interval_map,
    epoch_ms_to_datetime_key, get_conn,
)

CHECKPOINT_DIR = f"{CHECKPOINT_BASE}/kline_sink"
LOOKBACK = 26   # nến cần cho MACD (lookback dài nhất)

KLINE_SCHEMA = StructType([
    StructField("event_type",  StringType(),  True),
    StructField("symbol",      StringType(),  True),
    StructField("interval",    StringType(),  True),
    StructField("open_time",   LongType(),    True),
    StructField("close_time",  LongType(),    True),
    StructField("open",        DoubleType(),  True),
    StructField("high",        DoubleType(),  True),
    StructField("low",         DoubleType(),  True),
    StructField("close",       DoubleType(),  True),
    StructField("volume",      DoubleType(),  True),
    StructField("trade_count", IntegerType(), True),
    StructField("is_closed",   BooleanType(), True),
    StructField("ingested_at", LongType(),    True),
])

# ---------------------------------------------------------------------------
# Technical Indicator calculations (numpy, stateless functions)
# ---------------------------------------------------------------------------

def calc_rsi(closes: np.ndarray, period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    deltas = np.diff(closes)
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - 100 / (1 + rs), 4)


def calc_ema(values: np.ndarray, period: int) -> np.ndarray:
    ema = np.zeros(len(values))
    k   = 2 / (period + 1)
    ema[period - 1] = np.mean(values[:period])
    for i in range(period, len(values)):
        ema[i] = values[i] * k + ema[i - 1] * (1 - k)
    return ema


def calc_macd(closes: np.ndarray):
    """Returns (macd, signal, hist) or (None, None, None)."""
    if len(closes) < 26:
        return None, None, None
    ema12 = calc_ema(closes, 12)
    ema26 = calc_ema(closes, 26)
    macd_line = ema12 - ema26
    if len(macd_line) < 9:
        return None, None, None
    signal = calc_ema(macd_line[25:], 9)   # start from index 25 (first valid ema26)
    if len(signal) == 0:
        return None, None, None
    m = round(macd_line[-1], 8)
    s = round(signal[-1], 8)
    return m, s, round(m - s, 8)


def calc_bollinger(closes: np.ndarray, period: int = 20):
    """Returns (upper, middle, lower, width) or (None, None, None, None)."""
    if len(closes) < period:
        return None, None, None, None
    window = closes[-period:]
    mid    = np.mean(window)
    std    = np.std(window, ddof=1)
    upper  = mid + 2 * std
    lower  = mid - 2 * std
    width  = round((upper - lower) / mid * 100, 6) if mid else None
    return round(upper, 8), round(mid, 8), round(lower, 8), width


def calc_atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    tr_list = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        )
        tr_list.append(tr)
    if len(tr_list) < period:
        return None
    atr = np.mean(tr_list[:period])
    for i in range(period, len(tr_list)):
        atr = (atr * (period - 1) + tr_list[i]) / period
    return round(atr, 8)


def calc_obv(closes: np.ndarray, volumes: np.ndarray) -> float:
    obv = 0.0
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            obv += volumes[i]
        elif closes[i] < closes[i - 1]:
            obv -= volumes[i]
    return round(obv, 8)


# ---------------------------------------------------------------------------
# Batch writer
# ---------------------------------------------------------------------------

def write_kline_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    symbol_map   = load_symbol_map()
    interval_map = load_interval_map()

    rows = batch_df.collect()

    kline_rows = []
    tech_rows  = []

    with get_conn() as conn:
        for row in rows:
            sym_key  = symbol_map.get(row["symbol"])
            int_key  = interval_map.get(row["interval"])
            if sym_key is None or int_key is None:
                continue

            dt_key   = epoch_ms_to_datetime_key(row["open_time"])
            open_p   = float(row["open"])
            close_p  = float(row["close"])
            p_chg    = close_p - open_p
            p_chg_pct = round(p_chg / open_p * 100, 6) if open_p else 0.0
            amplitude = round((row["high"] - row["low"]) / open_p * 100, 6) if open_p else 0.0

            kline_rows.append((
                sym_key, dt_key, int_key,
                row["open_time"], row["close_time"],
                open_p, row["high"], row["low"], close_p,
                row["volume"], row["trade_count"],
                p_chg, p_chg_pct, amplitude,
                close_p >= open_p,
            ))

            # Query lịch sử để tính indicators
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT open::float, high::float, low::float, close::float, volume::float
                    FROM fact_kline_closed
                    WHERE symbol_key = %s
                    ORDER BY open_time DESC
                    LIMIT %s
                """, (sym_key, LOOKBACK))
                hist = cur.fetchall()

            # Thêm nến hiện tại vào đầu (newest first → reverse)
            all_candles = [(open_p, row["high"], row["low"], close_p, row["volume"])] + list(hist)
            all_candles.reverse()   # oldest first

            opens   = np.array([float(c[0]) for c in all_candles])
            highs   = np.array([float(c[1]) for c in all_candles])
            lows    = np.array([float(c[2]) for c in all_candles])
            closes  = np.array([float(c[3]) for c in all_candles])
            volumes = np.array([float(c[4]) for c in all_candles])

            rsi  = calc_rsi(closes)
            macd, macd_sig, macd_hist = calc_macd(closes)
            bb_u, bb_m, bb_l, bb_w   = calc_bollinger(closes)
            atr  = calc_atr(highs, lows, closes)
            obv  = calc_obv(closes, volumes)

            tech_rows.append((
                sym_key, dt_key, row["open_time"],
                rsi, macd, macd_sig, macd_hist,
                bb_u, bb_m, bb_l, bb_w,
                atr, obv,
            ))

        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO fact_kline_closed
                    (symbol_key, datetime_key, interval_key,
                     open_time, close_time,
                     open, high, low, close,
                     volume, trade_count,
                     price_change, price_change_pct, amplitude, is_bullish)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (symbol_key, open_time) DO NOTHING
            """, kline_rows)

            cur.executemany("""
                INSERT INTO fact_technical_indicator
                    (symbol_key, datetime_key, open_time,
                     rsi_14, macd, macd_signal, macd_hist,
                     bb_upper, bb_middle, bb_lower, bb_width,
                     atr_14, obv)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (symbol_key, open_time) DO NOTHING
            """, tech_rows)

        conn.commit()


def main():
    spark = (
        SparkSession.builder
        .appName("KlineSink")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "binance.klines")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw
        .select(F.from_json(F.col("value").cast("string"), KLINE_SCHEMA).alias("d"))
        .select("d.*")
        .filter(F.col("is_closed") == True)
        .filter(F.col("symbol").isNotNull())
        .filter(F.col("close") > 0)
        .filter(F.col("volume") >= 0)
    )

    query = (
        parsed.writeStream
        .outputMode("append")
        .trigger(processingTime="1 minute")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .foreachBatch(write_kline_batch)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
