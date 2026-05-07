
"""
Spark Job 5 — RawSink
-----------------------
Đọc từ tất cả 3 Kafka topics → ghi Parquet vào Azure Data Lake Gen2.

Partition layout:
  trades/     symbol=BTCUSDT/year=2026/month=05/day=07/hour=14/
  klines/     symbol=BTCUSDT/year=2026/month=05/day=07/
  bookticker/ symbol=BTCUSDT/year=2026/month=05/day=07/hour=14/

Trigger: mỗi 5 phút (micro-batch)

Chạy:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
               org.apache.hadoop:hadoop-azure:3.3.6,\
               com.microsoft.azure:azure-storage:8.6.6 \
    processor/job5_raw_sink.py
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, BooleanType, IntegerType,
)

from processor.common import KAFKA_BOOTSTRAP, CHECKPOINT_BASE

AZURE_ACCOUNT   = os.getenv("AZURE_STORAGE_ACCOUNT", "")
AZURE_KEY       = os.getenv("AZURE_STORAGE_KEY", "")
AZURE_CONTAINER = os.getenv("AZURE_CONTAINER", "crypto-raw")
ADLS_BASE       = f"abfss://{AZURE_CONTAINER}@{AZURE_ACCOUNT}.dfs.core.windows.net"

CHECKPOINT_DIR  = f"{CHECKPOINT_BASE}/raw_sink"

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

TRADE_SCHEMA = StructType([
    StructField("symbol",      StringType(),  True),
    StructField("trade_id",    LongType(),    True),
    StructField("price",       DoubleType(),  True),
    StructField("quantity",    DoubleType(),  True),
    StructField("buyer_maker", BooleanType(), True),
    StructField("trade_time",  LongType(),    True),
    StructField("ingested_at", LongType(),    True),
])

KLINE_SCHEMA = StructType([
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

BOOKTICKER_SCHEMA = StructType([
    StructField("symbol",      StringType(), True),
    StructField("bid_price",   DoubleType(), True),
    StructField("bid_qty",     DoubleType(), True),
    StructField("ask_price",   DoubleType(), True),
    StructField("ask_qty",     DoubleType(), True),
    StructField("ingested_at", LongType(),   True),
])


def add_partition_cols(df, ts_col: str):
    """Thêm year, month, day, hour từ epoch ms."""
    return (
        df
        .withColumn("_ts", (F.col(ts_col) / 1000).cast("timestamp"))
        .withColumn("year",  F.year("_ts").cast("string"))
        .withColumn("month", F.lpad(F.month("_ts").cast("string"), 2, "0"))
        .withColumn("day",   F.lpad(F.dayofmonth("_ts").cast("string"), 2, "0"))
        .withColumn("hour",  F.lpad(F.hour("_ts").cast("string"), 2, "0"))
        .drop("_ts")
    )


def main():
    spark = (
        SparkSession.builder
        .appName("RawSink")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # Azure ADLS Gen2 credentials
        .config(f"fs.azure.account.key.{AZURE_ACCOUNT}.dfs.core.windows.net", AZURE_KEY)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    def make_stream(topic: str):
        return (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

    # Trades stream
    trades_raw = (
        make_stream("binance.trades")
        .select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("d"))
        .select("d.*")
        .filter(F.col("symbol").isNotNull())
    )
    trades = add_partition_cols(trades_raw, "trade_time")

    # Klines stream
    klines_raw = (
        make_stream("binance.klines")
        .select(F.from_json(F.col("value").cast("string"), KLINE_SCHEMA).alias("d"))
        .select("d.*")
        .filter(F.col("symbol").isNotNull())
    )
    klines = add_partition_cols(klines_raw, "open_time")

    # Bookticker stream
    bt_raw = (
        make_stream("binance.bookticker")
        .select(F.from_json(F.col("value").cast("string"), BOOKTICKER_SCHEMA).alias("d"))
        .select("d.*")
        .filter(F.col("symbol").isNotNull())
    )
    bookticker = add_partition_cols(bt_raw, "ingested_at")

    def parquet_sink(df, path: str, partition_cols: list, checkpoint: str):
        return (
            df.writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", f"{ADLS_BASE}/{path}")
            .option("checkpointLocation", f"{CHECKPOINT_DIR}/{checkpoint}")
            .trigger(processingTime="5 minutes")
            .partitionBy(*partition_cols)
            .start()
        )

    q_trades     = parquet_sink(trades,     "trades",     ["symbol", "year", "month", "day", "hour"], "trades")
    q_klines     = parquet_sink(klines,     "klines",     ["symbol", "year", "month", "day"],         "klines")
    q_bookticker = parquet_sink(bookticker, "bookticker", ["symbol", "year", "month", "day", "hour"], "bookticker")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
