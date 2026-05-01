import json
import os
import time

import pandas as pd
from kafka import KafkaConsumer, TopicPartition


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "energy_readings")
MAX_MESSAGES = int(os.getenv("DASHBOARD_MAX_MESSAGES", "2500"))
REFRESH_MS = int(os.getenv("DASHBOARD_REFRESH_MS", "3000"))

VOLTAGE_MAX_V = 245.0


def read_kafka_messages(max_messages: int) -> tuple[list[dict], str | None]:
    records: list[dict] = []
    consumer = None
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BROKER,
            enable_auto_commit=False,
            request_timeout_ms=10000,
            api_version_auto_timeout_ms=5000,
        )

        partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
        if not partitions:
            return [], f"Topic '{KAFKA_TOPIC}' has no partitions yet"

        topic_partitions = [TopicPartition(KAFKA_TOPIC, partition) for partition in sorted(partitions)]
        consumer.assign(topic_partitions)
        beginning_offsets = consumer.beginning_offsets(topic_partitions)
        end_offsets = consumer.end_offsets(topic_partitions)

        per_partition = max(1, (max_messages // len(topic_partitions)) + 1)
        for topic_partition in topic_partitions:
            start_offset = max(
                beginning_offsets.get(topic_partition, 0),
                end_offsets.get(topic_partition, 0) - per_partition,
            )
            consumer.seek(topic_partition, start_offset)

        deadline = time.monotonic() + 2.5
        while len(records) < max_messages and time.monotonic() < deadline:
            batch = consumer.poll(timeout_ms=350, max_records=max_messages - len(records))
            if not batch:
                break
            for messages in batch.values():
                for message in messages:
                    try:
                        value = json.loads(message.value.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        continue
                    if isinstance(value, dict):
                        records.append(value)

        return records[-max_messages:], None
    except Exception as exc:
        return [], str(exc)
    finally:
        if consumer is not None:
            consumer.close()


def score_records(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    for column in ["consumption_kwh", "voltage_v", "current_a", "frequency_hz"]:
        df[column] = pd.to_numeric(df.get(column), errors="coerce")

    df["event_time"] = pd.to_datetime(df.get("timestamp"), errors="coerce", utc=True)
    df["hour"] = pd.to_numeric(df.get("hour"), errors="coerce").fillna(df["event_time"].dt.hour)
    df["computed_power_w"] = df["voltage_v"] * df["current_a"]
    df["expected_power_w"] = df["consumption_kwh"] * 30 * 1000
    df["power_ratio"] = df["computed_power_w"] / df["expected_power_w"].where(df["expected_power_w"] > 0)

    df["rule_low_consumption"] = df["consumption_kwh"] < 0.30
    df["rule_voltage_spike"] = df["voltage_v"] > VOLTAGE_MAX_V
    df["rule_power_inconsistency"] = df["power_ratio"] > 1.15
    df["rule_odd_hour_spike"] = df["hour"].between(0, 5) & (df["consumption_kwh"] > 5.0)
    df["rule_frequency_anomaly"] = ~df["frequency_hz"].between(49.8, 50.2)

    rule_cols = [
        "rule_low_consumption",
        "rule_voltage_spike",
        "rule_power_inconsistency",
        "rule_odd_hour_spike",
        "rule_frequency_anomaly",
    ]
    df["rules_fired_count"] = df[rule_cols].fillna(False).sum(axis=1).astype(int)
    df["is_fraud_flagged"] = df["rules_fired_count"] > 0
    df["fraud_severity"] = pd.cut(
        df["rules_fired_count"],
        bins=[-1, 0, 1, 2, 99],
        labels=["NONE", "LOW", "MEDIUM", "HIGH"],
    ).astype(str)
    df["minute"] = df["event_time"].dt.floor("min")
    return df.sort_values("event_time")
