"""
Spark Job 1 — TradeAggregator
------------------------------
Đọc từ binance.trades → tính OHLCV + order flow → ghi vào:
  - fact_trade_agg      (window 1s và 5s)
  - fact_order_flow     (window 1s và 5s)

Trigger: 1 giây (micro-batch)
Watermark: 2 giây (chấp nhận late data)

Chạy:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
    processor/job1_trade_agg.py
"""
import os
import json
import threading
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, BooleanType,
)

from processor.common import (
    KAFKA_BOOTSTRAP, JDBC_URL, JDBC_PROPS, CHECKPOINT_BASE,
    load_symbol_map, load_window_map,
    epoch_ms_to_datetime_key,
)

CHECKPOINT_DIR = f"{CHECKPOINT_BASE}/trade_agg"

TRADE_SCHEMA = StructType([
    StructField("event_type",  StringType(),  True),
    StructField("symbol",      StringType(),  True),
    StructField("trade_id",    LongType(),    True),
    StructField("price",       DoubleType(),  True),
    StructField("quantity",    DoubleType(),  True),
    StructField("buyer_maker", BooleanType(), True),
    StructField("trade_time",  LongType(),    True),
    StructField("ingested_at", LongType(),    True),
])


def write_agg_batch(batch_df, batch_id, window_label: str):
    """foreachBatch writer — map dim keys, upsert vào PostgreSQL."""
    try:
        _write_agg_batch_inner(batch_df, batch_id, window_label)
    except Exception as e:
        import traceback
        print(f"[ERROR] batch_id={batch_id} window={window_label}: {e}")
        traceback.print_exc()


def _write_agg_batch_inner(batch_df, batch_id, window_label: str):
    if batch_df.isEmpty():
        return

    symbol_map  = load_symbol_map()
    window_map  = load_window_map()

    rows = batch_df.collect()

    trade_agg_rows  = []
    order_flow_rows = []

    for row in rows:
        sym_key = symbol_map.get(row["symbol"])
        win_key = window_map.get(window_label)
        if sym_key is None or win_key is None:
            continue

        w_start = int(row["window_start"].timestamp() * 1000)
        w_end   = int(row["window_end"].timestamp()   * 1000)
        dt_key  = epoch_ms_to_datetime_key(w_start)

        open_p  = row["open"]
        close_p = row["close"]
        p_chg   = close_p - open_p
        p_chg_pct = (p_chg / open_p * 100) if open_p else 0.0

        trade_agg_rows.append((
            sym_key, dt_key, win_key,
            w_start, w_end,
            open_p, row["high"], row["low"], close_p,
            row["volume"], row["vwap"],
            row["trade_count"],
            p_chg, p_chg_pct,
        ))

        total_vol = row["buy_volume"] + row["sell_volume"]
        buy_pct   = (row["buy_volume"] / total_vol * 100) if total_vol else 50.0
        net_flow  = row["buy_volume"] - row["sell_volume"]

        order_flow_rows.append((
            sym_key, dt_key, win_key,
            w_start,
            row["buy_volume"], row["sell_volume"], total_vol,
            row["buy_count"], row["sell_count"],
            buy_pct, net_flow,
        ))

    import psycopg2
    from processor.common import get_conn

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO fact_trade_agg
                    (symbol_key, datetime_key, window_key,
                     window_start, window_end,
                     open, high, low, close,
                     volume, vwap, trade_count,
                     price_change, price_change_pct)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (symbol_key, window_start, window_key) DO NOTHING
            """, trade_agg_rows)

            cur.executemany("""
                INSERT INTO fact_order_flow
                    (symbol_key, datetime_key, window_key,
                     window_start,
                     buy_volume, sell_volume, total_volume,
                     buy_count, sell_count,
                     buy_pct, net_flow)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (symbol_key, window_start, window_key) DO NOTHING
            """, order_flow_rows)

        conn.commit()


def build_agg_query(parsed_df, window_duration: str):
    """Aggregate OHLCV + order flow cho một window size."""
    windowed = (
        parsed_df
        .withWatermark("event_time", "2 seconds")
        .groupBy(
            F.window("event_time", window_duration),
            "symbol",
        )
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("quantity").alias("volume"),
            (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("vwap"),
            F.count("*").alias("trade_count"),
            # Order flow: buyer_maker=false → lệnh MUA chủ động
            F.sum(
                F.when(F.col("buyer_maker") == False, F.col("quantity")).otherwise(0.0)
            ).alias("buy_volume"),
            F.sum(
                F.when(F.col("buyer_maker") == True, F.col("quantity")).otherwise(0.0)
            ).alias("sell_volume"),
            F.sum(
                F.when(F.col("buyer_maker") == False, F.lit(1)).otherwise(0)
            ).alias("buy_count"),
            F.sum(
                F.when(F.col("buyer_maker") == True, F.lit(1)).otherwise(0)
            ).alias("sell_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol", "open", "high", "low", "close",
            "volume", "vwap", "trade_count",
            "buy_volume", "sell_volume", "buy_count", "sell_count",
        )
    )
    return windowed


def main():
    spark = (
        SparkSession.builder
        .appName("TradeAggregator")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Đọc từ Kafka
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "binance.trades")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON
    parsed = (
        raw
        .select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("d"))
        .select("d.*")
        .filter(
            F.col("price").isNotNull() &
            (F.col("price") > 0) &
            F.col("quantity").isNotNull() &
            (F.col("quantity") > 0) &
            F.col("symbol").isNotNull()
        )
        .withColumn("event_time", (F.col("trade_time") / 1000).cast("timestamp"))
    )

    # Serialize foreachBatch calls to avoid py4j concurrent callback issues
    _lock = threading.Lock()

    def write_1s(df, bid):
        with _lock:
            write_agg_batch(df, bid, "1s")

    def write_5s(df, bid):
        with _lock:
            write_agg_batch(df, bid, "5s")

    # Window 1 giây
    agg_1s = build_agg_query(parsed, "1 second")
    query_1s = (
        agg_1s.writeStream
        .outputMode("append")
        .trigger(processingTime="1 second")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/1s")
        .foreachBatch(write_1s)
        .start()
    )

    # Window 5 giây
    agg_5s = build_agg_query(parsed, "5 seconds")
    query_5s = (
        agg_5s.writeStream
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/5s")
        .foreachBatch(write_5s)
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
