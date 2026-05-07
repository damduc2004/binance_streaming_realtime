"""
Binance WebSocket → Kafka Producer
Streams: @trade + @kline_1m + @bookTicker cho tất cả symbols
Topics:  binance.trades | binance.klines | binance.bookticker

Chạy: python -m ingestion.producer
"""
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone

import websocket
from confluent_kafka import Producer

from ingestion.config import SYMBOLS, TOPICS, PRODUCER_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

streams = "/".join(
    [f"{s}@trade"      for s in SYMBOLS] +
    [f"{s}@kline_1m"   for s in SYMBOLS] +
    [f"{s}@bookTicker" for s in SYMBOLS]
)
WS_URL = f"wss://stream.binance.com:9443/stream?streams={streams}"

producer = Producer(PRODUCER_CONFIG)
stats = {"trades": 0, "klines": 0, "bookticker": 0, "errors": 0, "last_report": time.time()}


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def delivery_report(err, msg):
    if err:
        log.error("Delivery failed [%s]: %s", msg.topic(), err)
        stats["errors"] += 1


def normalize_trade(raw: dict) -> dict:
    return {
        "event_type":  "trade",
        "symbol":      raw["s"].upper(),
        "trade_id":    raw["t"],
        "price":       float(raw["p"]),
        "quantity":    float(raw["q"]),
        "buyer_maker": raw["m"],
        "trade_time":  raw["T"],
        "ingested_at": _now_ms(),
    }


def normalize_kline(raw: dict) -> dict:
    k = raw["k"]
    return {
        "event_type":  "kline",
        "symbol":      raw["s"].upper(),
        "interval":    k["i"],
        "open_time":   k["t"],
        "close_time":  k["T"],
        "open":        float(k["o"]),
        "high":        float(k["h"]),
        "low":         float(k["l"]),
        "close":       float(k["c"]),
        "volume":      float(k["v"]),
        "trade_count": k["n"],
        "is_closed":   k["x"],
        "ingested_at": _now_ms(),
    }


def normalize_bookticker(raw: dict) -> dict:
    return {
        "event_type": "bookticker",
        "symbol":     raw["s"].upper(),
        "bid_price":  float(raw["b"]),
        "bid_qty":    float(raw["B"]),
        "ask_price":  float(raw["a"]),
        "ask_qty":    float(raw["A"]),
        "ingested_at": _now_ms(),
    }


def print_stats():
    now = time.time()
    if now - stats["last_report"] >= 10:
        log.info(
            "Stats: trades=%d, klines=%d, bookticker=%d, errors=%d",
            stats["trades"], stats["klines"], stats["bookticker"], stats["errors"],
        )
        stats["last_report"] = now


def on_message(ws, message):
    try:
        envelope = json.loads(message)
        stream = envelope.get("stream", "")
        data = envelope.get("data", {})
        event_type = data.get("e", "")

        if event_type == "trade":
            record = normalize_trade(data)
            topic = TOPICS["trades"]
            stats["trades"] += 1
        elif event_type == "kline":
            record = normalize_kline(data)
            topic = TOPICS["klines"]
            stats["klines"] += 1
        elif event_type == "bookTicker" or "bookTicker" in stream:
            record = normalize_bookticker(data)
            topic = TOPICS["bookticker"]
            stats["bookticker"] += 1
        else:
            return

        producer.produce(
            topic=topic,
            key=record["symbol"].encode(),
            value=json.dumps(record).encode(),
            callback=delivery_report,
        )
        producer.poll(0)
        print_stats()

    except (json.JSONDecodeError, KeyError) as e:
        log.warning("Parse error: %s | raw: %s", e, message[:200])
        stats["errors"] += 1


def on_error(ws, error):
    log.error("WebSocket error: %s", error)


def on_close(ws, code, msg):
    log.info("WebSocket closed: code=%s msg=%s", code, msg)


def on_open(ws):
    log.info("Connected to Binance — symbols=%d, streams=%d", len(SYMBOLS), len(SYMBOLS) * 3)


def run():
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            log.error("Connection dropped: %s — reconnecting in 5s", e)
        time.sleep(5)


def handle_shutdown(signum, frame):
    log.info("Shutting down... flushing producer")
    producer.flush(timeout=10)
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    log.info("Starting Binance WebSocket Producer — symbols: %s", SYMBOLS)
    run()
