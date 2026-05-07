import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")

TOPICS = {
    "trades":     "binance.trades",
    "klines":     "binance.klines",
    "bookticker": "binance.bookticker",
}

TOPIC_CONFIG = {
    "binance.trades":     {"partitions": 10, "retention_ms": 604800000},
    "binance.klines":     {"partitions": 5,  "retention_ms": 604800000},
    "binance.bookticker": {"partitions": 10, "retention_ms": 604800000},
}

SYMBOLS = [
    "btcusdt",
    "ethusdt",
    "bnbusdt",
    "solusdt",
    "xrpusdt",
    "dogeusdt",
    "adausdt",
    "avaxusdt",
]

PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "acks": "all",
    "retries": 5,
    "retry.backoff.ms": 300,
    "compression.type": "lz4",
    "linger.ms": 5,
    "batch.size": 65536,
}
