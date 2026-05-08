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
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, BooleanType,
)

from processor.common import (
    KAFKA_BOOTSTRAP, JDBC_URL, JDBC_PROPS, CHECKPOINT_BASE,
    load_symbol_map, load_window_map,
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


def build_dim_maps(spark: SparkSession):
    """Load symbol_map và window_map một lần, broadcast cho toàn cluster."""
    symbol_map = load_symbol_map()
    window_map = load_window_map()
    bc_symbol = spark.sparkContext.broadcast(symbol_map)
    bc_window = spark.sparkContext.broadcast(window_map)
    return bc_symbol, bc_window


def write_trade_agg(batch_df: DataFrame, batch_id: int, window_label: str,
                    bc_symbol, bc_window):
    """Bulk-write fact_trade_agg + fact_order_flow dùng JDBC — không collect()."""
    if batch_df.isEmpty():
        return

    win_key = bc_window.value.get(window_label)
    if win_key is None:
        print(f"[WARN] window_label '{window_label}' không có trong dim_window_type")
        return

    # Map symbol → symbol_key bằng UDF broadcast (chạy trên executor)
    symbol_map_val = bc_symbol.value

    @F.udf("int")
    def sym_to_key(sym):
        return symbol_map_val.get(sym)

    @F.udf("long")
    def epoch_to_dt_key(epoch_ms):
        from datetime import datetime, timezone
        dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
        return int(dt.strftime("%Y%m%d%H%M"))

    enriched = (
        batch_df
        .withColumn("symbol_key", sym_to_key(F.col("symbol")))
        .withColumn("window_start_ms", (F.unix_timestamp(F.col("window_start")) * 1000).cast("long"))
        .withColumn("window_end_ms",   (F.unix_timestamp(F.col("window_end"))   * 1000).cast("long"))
        .withColumn("datetime_key", epoch_to_dt_key(F.col("window_start_ms")))
        .withColumn("window_key", F.lit(win_key))
        .filter(F.col("symbol_key").isNotNull())
    )

    # ── fact_trade_agg ──────────────────────────────────────────────────────
    trade_agg_df = (
        enriched
        .withColumn("price_change",     F.col("close") - F.col("open"))
        .withColumn("price_change_pct",
            F.when(F.col("open") != 0,
                (F.col("close") - F.col("open")) / F.col("open") * 100
            ).otherwise(F.lit(0.0))
        )
        .select(
            "symbol_key", "datetime_key", "window_key",
            F.col("window_start_ms").alias("window_start"),
            F.col("window_end_ms").alias("window_end"),
            "open", "high", "low", "close",
            "volume", "vwap", "trade_count",
            "price_change", "price_change_pct",
        )
    )

    (
        trade_agg_df.write
        .mode("append")
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "2000")
        .option("isolationLevel", "READ_COMMITTED")
        .jdbc(JDBC_URL, "fact_trade_agg", properties=JDBC_PROPS)
    )

    # ── fact_order_flow ─────────────────────────────────────────────────────
    order_flow_df = (
        enriched
        .withColumn("total_volume", F.col("buy_volume") + F.col("sell_volume"))
        .withColumn("buy_pct",
            F.when(
                (F.col("buy_volume") + F.col("sell_volume")) > 0,
                F.col("buy_volume") / (F.col("buy_volume") + F.col("sell_volume")) * 100
            ).otherwise(F.lit(50.0))
        )
        .withColumn("net_flow", F.col("buy_volume") - F.col("sell_volume"))
        .select(
            "symbol_key", "datetime_key", "window_key",
            F.col("window_start_ms").alias("window_start"),
            "buy_volume", "sell_volume", "total_volume",
            "buy_count", "sell_count",
            "buy_pct", "net_flow",
        )
    )

    (
        order_flow_df.write
        .mode("append")
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "2000")
        .option("isolationLevel", "READ_COMMITTED")
        .jdbc(JDBC_URL, "fact_order_flow", properties=JDBC_PROPS)
    )


def build_agg_query(parsed_df: DataFrame, window_duration: str) -> DataFrame:
    """Aggregate OHLCV + order flow cho một window size."""
    return (
        parsed_df
        .withWatermark("event_time", "2 seconds")
        .groupBy(F.window("event_time", window_duration), "symbol")
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("quantity").alias("volume"),
            (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("vwap"),
            F.count("*").alias("trade_count"),
            F.sum(F.when(~F.col("buyer_maker"), F.col("quantity")).otherwise(0.0)).alias("buy_volume"),
            F.sum(F.when( F.col("buyer_maker"), F.col("quantity")).otherwise(0.0)).alias("sell_volume"),
            F.sum(F.when(~F.col("buyer_maker"), F.lit(1)).otherwise(0)).alias("buy_count"),
            F.sum(F.when( F.col("buyer_maker"), F.lit(1)).otherwise(0)).alias("sell_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol", "open", "high", "low", "close",
            "volume", "vwap", "trade_count",
            "buy_volume", "sell_volume", "buy_count", "sell_count",
        )
    )


def main():
    spark = (
        SparkSession.builder
        .appName("TradeAggregator")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    bc_symbol, bc_window = build_dim_maps(spark)

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "binance.trades")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "50000")
        .load()
    )

    parsed = (
        raw
        .select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("d"))
        .select("d.*")
        .filter(
            F.col("price").isNotNull() & (F.col("price") > 0) &
            F.col("quantity").isNotNull() & (F.col("quantity") > 0) &
            F.col("symbol").isNotNull()
        )
        .withColumn("event_time", (F.col("trade_time") / 1000).cast("timestamp"))
    )

    agg_1s = build_agg_query(parsed, "1 second")
    agg_5s = build_agg_query(parsed, "5 seconds")

    query_1s = (
        agg_1s.writeStream
        .outputMode("append")
        .trigger(processingTime="1 second")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/1s")
        .foreachBatch(lambda df, bid: write_trade_agg(df, bid, "1s", bc_symbol, bc_window))
        .start()
    )

    query_5s = (
        agg_5s.writeStream
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/5s")
        .foreachBatch(lambda df, bid: write_trade_agg(df, bid, "5s", bc_symbol, bc_window))
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
