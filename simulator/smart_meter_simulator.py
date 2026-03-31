"""
Smart Meter Simulator — Energy Fraud Detection Project
=======================================================
Simulates 10 smart meters sending electricity consumption readings.
Some meters are pre-configured to exhibit fraudulent behavior patterns.

Fraud patterns simulated:
  - Abnormally low consumption (meter tampering / bypass)
  - Sudden voltage spikes (illegal device connection)
  - Irregular consumption at odd hours (illegal resale)
  - Consumption drops while peak usage is expected
"""

import json
import random
import time
import math
import os
from datetime import datetime, timezone
from kafka import KafkaProducer

# ── Configuration ──────────────────────────────────────────────────────────────
# KAFKA_BROKER is read from environment so the same script works both
# locally (localhost:9092) and inside Docker (kafka:9092).

KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC    = "energy_readings"
EMIT_INTERVAL  = 2          # seconds between each reading per meter
NUM_METERS     = 10

# Meter IDs and their fraud profiles
# fraud_type: None | "low_consumption" | "voltage_spike" | "odd_hour_usage" | "bypass"
METERS = [
    {"meter_id": "MTR_001", "location": "Zone_A", "normal_consumption": 3.5, "fraud_type": None},
    {"meter_id": "MTR_002", "location": "Zone_A", "normal_consumption": 4.2, "fraud_type": "low_consumption"},
    {"meter_id": "MTR_003", "location": "Zone_B", "normal_consumption": 5.0, "fraud_type": None},
    {"meter_id": "MTR_004", "location": "Zone_B", "normal_consumption": 6.1, "fraud_type": "voltage_spike"},
    {"meter_id": "MTR_005", "location": "Zone_C", "normal_consumption": 3.8, "fraud_type": None},
    {"meter_id": "MTR_006", "location": "Zone_C", "normal_consumption": 4.5, "fraud_type": "odd_hour_usage"},
    {"meter_id": "MTR_007", "location": "Zone_D", "normal_consumption": 5.3, "fraud_type": None},
    {"meter_id": "MTR_008", "location": "Zone_D", "normal_consumption": 4.0, "fraud_type": "bypass"},
    {"meter_id": "MTR_009", "location": "Zone_E", "normal_consumption": 3.2, "fraud_type": None},
    {"meter_id": "MTR_010", "location": "Zone_E", "normal_consumption": 4.8, "fraud_type": "low_consumption"},
]


# ── Reading Generator ──────────────────────────────────────────────────────────

def generate_normal_reading(meter, hour):
    """
    Generate a realistic consumption reading based on time of day.
    Follows a sinusoidal daily pattern: low at night, high during day.
    """
    base = meter["normal_consumption"]

    # Daily usage pattern: peak at 18h, trough at 4h
    daily_factor = 0.5 + 0.5 * math.sin((hour - 4) * math.pi / 12)
    noise        = random.gauss(0, 0.15)

    consumption  = round(max(0.1, base * daily_factor + noise), 3)
    voltage      = round(random.gauss(230.0, 2.0), 1)    # nominal 230V EU standard
    current      = round(consumption * 1000 / voltage, 3) # P = U × I

    return consumption, voltage, current


def generate_fraud_reading(meter, hour):
    """
    Generate a reading that mimics a fraudulent behavior pattern.
    """
    fraud = meter["fraud_type"]
    consumption, voltage, current = generate_normal_reading(meter, hour)

    if fraud == "low_consumption":
        # Meter tampered: actual usage is normal but reported value is very low
        consumption = round(consumption * random.uniform(0.05, 0.15), 3)
        current     = round(consumption * 1000 / voltage, 3)

    elif fraud == "voltage_spike":
        # Illegal high-power device connected periodically
        if random.random() < 0.3:
            voltage     = round(random.uniform(250.0, 280.0), 1)
            consumption = round(consumption * random.uniform(2.5, 4.0), 3)
            current     = round(consumption * 1000 / voltage, 3)

    elif fraud == "odd_hour_usage":
        # Heavy usage at night (illegal commercial activity / resale)
        if 0 <= hour <= 5 or 22 <= hour <= 23:
            consumption = round(meter["normal_consumption"] * random.uniform(3.0, 5.0), 3)
            current     = round(consumption * 1000 / voltage, 3)

    elif fraud == "bypass":
        # Meter completely bypassed some of the time
        if random.random() < 0.4:
            consumption = round(random.uniform(0.01, 0.08), 3)
            current     = 0.0

    return consumption, voltage, current


def build_message(meter):
    """Build a full JSON message for one meter at the current timestamp."""
    now  = datetime.now(timezone.utc)
    hour = now.hour

    if meter["fraud_type"] and random.random() < 0.7:
        # 70% of readings from fraud meters are fraudulent
        consumption, voltage, current = generate_fraud_reading(meter, hour)
        is_injected_fraud = True
    else:
        consumption, voltage, current = generate_normal_reading(meter, hour)
        is_injected_fraud = False

    return {
        "meter_id":           meter["meter_id"],
        "location":           meter["location"],
        "timestamp":          now.isoformat(),
        "date":               now.strftime("%Y-%m-%d"),
        "hour":               hour,
        "consumption_kwh":    consumption,
        "voltage_v":          voltage,
        "current_a":          current,
        "power_factor":       round(random.uniform(0.85, 1.0), 3),
        "frequency_hz":       round(random.gauss(50.0, 0.05), 2),
        # Ground-truth label (used for evaluation — NOT used by Spark detection logic)
        "_injected_fraud":    is_injected_fraud,
    }


# ── Kafka Producer ─────────────────────────────────────────────────────────────

def create_producer():
    return KafkaProducer(
        bootstrap_servers = KAFKA_BROKER,
        value_serializer  = lambda v: json.dumps(v).encode("utf-8"),
        key_serializer    = lambda k: k.encode("utf-8"),
        acks              = "all",        # wait for full replication
        retries           = 3,
    )


def run_simulator():
    print(f"[SIMULATOR] Starting — {NUM_METERS} meters → Kafka topic '{KAFKA_TOPIC}'")
    print(f"[SIMULATOR] Fraud meters: {[m['meter_id'] for m in METERS if m['fraud_type']]}\n")

    producer = create_producer()

    try:
        while True:
            for meter in METERS:
                msg = build_message(meter)
                producer.send(
                    topic = KAFKA_TOPIC,
                    key   = msg["meter_id"],
                    value = msg,
                )
                status = "⚠ FRAUD" if msg["_injected_fraud"] else "  OK   "
                print(
                    f"[{status}] {msg['meter_id']} | "
                    f"{msg['consumption_kwh']:6.3f} kWh | "
                    f"{msg['voltage_v']:5.1f} V | "
                    f"{msg['timestamp']}"
                )

            producer.flush()
            time.sleep(EMIT_INTERVAL)

    except KeyboardInterrupt:
        print("\n[SIMULATOR] Stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    run_simulator()
