"""
Kafka Producer & Consumer — Energy Fraud Detection
====================================================
Standalone consumer that reads from the 'energy_readings' topic
and prints a live stream of meter messages.

The simulator (smart_meter_simulator.py) already embeds a Kafka producer.
This file provides:
  1. A reusable KafkaConsumer wrapper (used by Spark and standalone testing)
  2. A standalone consumer script for testing and debugging
"""

import json
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from datetime import datetime


KAFKA_BROKER  = "localhost:9092"
KAFKA_TOPIC   = "energy_readings"
CONSUMER_GROUP = "energy_fraud_group"


# ── Topic Management ───────────────────────────────────────────────────────────

def create_topic_if_not_exists(num_partitions=3, replication_factor=1):
    """
    Create the Kafka topic with 3 partitions (one per zone group).
    Partitions allow Spark to consume in parallel.
    """
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
    existing = admin.list_topics()

    if KAFKA_TOPIC not in existing:
        topic = NewTopic(
            name               = KAFKA_TOPIC,
            num_partitions     = num_partitions,
            replication_factor = replication_factor,
        )
        admin.create_topics([topic])
        print(f"[KAFKA] Topic '{KAFKA_TOPIC}' created with {num_partitions} partitions.")
    else:
        print(f"[KAFKA] Topic '{KAFKA_TOPIC}' already exists.")

    admin.close()


# ── Standalone Consumer (for testing) ─────────────────────────────────────────

def consume_and_print():
    """
    Read messages from the topic and print them to stdout.
    Useful for verifying the simulator is working before starting Spark.
    """
    print(f"[CONSUMER] Listening on '{KAFKA_TOPIC}' (group: {CONSUMER_GROUP})")
    print("-" * 70)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers  = KAFKA_BROKER,
        group_id           = CONSUMER_GROUP,
        auto_offset_reset  = "latest",
        enable_auto_commit = True,
        value_deserializer = lambda m: json.loads(m.decode("utf-8")),
        key_deserializer   = lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms = 60_000,   # stop after 60s of silence
    )

    count = 0
    try:
        for record in consumer:
            msg = record.value
            fraud_flag = "⚠ FRAUD" if msg.get("_injected_fraud") else "OK"
            print(
                f"[{fraud_flag}] "
                f"Partition={record.partition} | "
                f"Offset={record.offset} | "
                f"{msg['meter_id']} | "
                f"kWh={msg['consumption_kwh']:.3f} | "
                f"V={msg['voltage_v']:.1f} | "
                f"{msg['timestamp']}"
            )
            count += 1
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print(f"\n[CONSUMER] Stopped after {count} messages.")


# ── Consumer for Offset Inspection ────────────────────────────────────────────

def print_topic_info():
    """Print partition offsets — useful for monitoring lag."""
    consumer = KafkaConsumer(
        bootstrap_servers  = KAFKA_BROKER,
        group_id           = "inspector_group",
        auto_offset_reset  = "earliest",
        consumer_timeout_ms = 5_000,
    )
    partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
    if partitions:
        print(f"[KAFKA] Topic '{KAFKA_TOPIC}' has {len(partitions)} partitions: {partitions}")
    else:
        print(f"[KAFKA] Topic '{KAFKA_TOPIC}' not found or has no partitions.")
    consumer.close()


# ── Entry Point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    create_topic_if_not_exists()
    print_topic_info()
    consume_and_print()
