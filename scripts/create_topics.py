"""
Tạo Kafka topics cho binance_streaming_realtime.
Chạy: python scripts/create_topics.py
"""
import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic

sys.path.insert(0, ".")
from ingestion.config import KAFKA_BOOTSTRAP, TOPICS, TOPIC_CONFIG


def create_topics():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})

    new_topics = []
    for topic_name in TOPICS.values():
        cfg = TOPIC_CONFIG.get(topic_name, {})
        new_topics.append(
            NewTopic(
                topic=topic_name,
                num_partitions=cfg.get("partitions", 3),
                replication_factor=1,
                config={"retention.ms": str(cfg.get("retention_ms", 604800000))},
            )
        )

    print(f"Creating {len(new_topics)} topics on {KAFKA_BOOTSTRAP}...")
    futures = admin.create_topics(new_topics)

    for topic_name, future in futures.items():
        try:
            future.result()
            print(f"  Created: {topic_name}")
        except Exception as e:
            if "TOPIC_ALREADY_EXISTS" in str(e):
                print(f"  Already exists: {topic_name}")
            else:
                print(f"  Failed: {topic_name} — {e}")

    time.sleep(1)
    metadata = admin.list_topics(timeout=10)
    print(f"\nBinance topics on broker:")
    for t in sorted(metadata.topics):
        if t.startswith("binance."):
            partitions = len(metadata.topics[t].partitions)
            print(f"  {t} ({partitions} partitions)")


if __name__ == "__main__":
    create_topics()
