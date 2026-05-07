"""
Spark Job 3 — SpreadAggregator
--------------------------------
Đọc từ binance.bookticker → tính spread + liquidity imbalance → ghi vào:
  - fact_spread_snapshot  (aggregate theo window 1 giây)

Trigger: 1 giây

Chạy:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
    processor/job3_spread_agg.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
)

from processor.common import (
    KAFKA_BOOTSTRAP, CHECKPOINT_BASE,
    load_symbol_map, epoch_ms_to_datetime_key, get_conn,
)

CHECKPOINT_DIR = f"{CHECKPOINT_BASE}/spread_agg"

BOOKTICKER_SCHEMA = StructType([
    StructField("event_type",  StringType(), True),
    StructField("symbol",      StringType(), True),
    StructField("bid_price",   DoubleType(), True),
    StructField("bid_qty",     DoubleType(), True),
    StructField("ask_price",   DoubleType(), True),
    StructField("ask_qty",     DoubleType(), True),
    StructField("ingested_at", LongType(),   True),
])


def write_spread_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    symbol_map = load_symbol_map()
    rows = batch_df.collect()
    spread_rows = []

    for row in rows:
        sym_key = symbol_map.get(row["symbol"])
        if sym_key is None:
            continue

        bid = row["avg_bid_price"]
        ask = row["avg_ask_price"]

        if bid <= 0 or ask <= bid:
            continue

        mid     = (bid + ask) / 2
        spread  = ask - bid
        spr_pct = round(spread / mid * 100, 6) if mid else 0.0
        bid_qty = row["avg_bid_qty"]
        ask_qty = row["avg_ask_qty"]
        total_qty = bid_qty + ask_qty
        imbalance = round((bid_qty - ask_qty) / total_qty, 6) if total_qty else 0.0

        snap_time = int(row["window_start"].timestamp() * 1000)
        dt_key    = epoch_ms_to_datetime_key(snap_time)

        spread_rows.append((
            sym_key, dt_key, snap_time,
            round(bid, 8), round(ask, 8), round(mid, 8),
            round(spread, 8), spr_pct,
            round(bid_qty, 8), round(ask_qty, 8),
            imbalance,
        ))

    if not spread_rows:
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO fact_spread_snapshot
                    (symbol_key, datetime_key, snapshot_time,
                     bid_price, ask_price, mid_price,
                     spread, spread_pct,
                     bid_qty, ask_qty, liquidity_imbalance)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, spread_rows)
        conn.commit()


def main():
    spark = (
        SparkSession.builder
        .appName("SpreadAggregator")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "binance.bookticker")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw
        .select(F.from_json(F.col("value").cast("string"), BOOKTICKER_SCHEMA).alias("d"))
        .select("d.*")
        .filter(
            F.col("symbol").isNotNull() &
            (F.col("bid_price") > 0) &
            (F.col("ask_price") > 0)
        )
        .withColumn("event_time", (F.col("ingested_at") / 1000).cast("timestamp"))
    )

    agg = (
        parsed
        .withWatermark("event_time", "2 seconds")
        .groupBy(
            F.window("event_time", "1 second"),
            "symbol",
        )
        .agg(
            F.avg("bid_price").alias("avg_bid_price"),
            F.avg("ask_price").alias("avg_ask_price"),
            F.avg("bid_qty").alias("avg_bid_qty"),
            F.avg("ask_qty").alias("avg_ask_qty"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            "symbol",
            "avg_bid_price", "avg_ask_price",
            "avg_bid_qty", "avg_ask_qty",
        )
    )

    query = (
        agg.writeStream
        .outputMode("append")
        .trigger(processingTime="1 second")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .foreachBatch(write_spread_batch)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
