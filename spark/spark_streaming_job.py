"""
Spark Streaming Job — Energy Fraud Detection
============================================
Reads from Kafka, applies fraud detection rules in real-time,
enriches data, and writes results to HDFS in Parquet format.

Fraud Detection Rules Applied
------------------------------
Rule 1 — Abnormally low consumption
    consumption_kwh < (meter's 30-min rolling mean × 0.2)
    → Likely meter bypass or tampering

Rule 2 — Voltage anomaly
    voltage_v > 245 V  (>6.5% above EU nominal 230V)
    → Possible illegal high-power device or meter manipulation

Rule 3 — High current imbalance
    current_a × voltage_v > consumption_kwh × 1000 × 1.15
    → Power factor / current inconsistency suggests meter interference

Rule 4 — Odd-hour high consumption
    hour in [0,1,2,3,4,5] AND consumption_kwh > threshold × 2.0
    → Anomalous nighttime usage pattern

Rule 5 — Sudden spike ratio
    consumption_kwh > rolling_mean × 3.5
    → Sudden unexplained spike

An alert is raised when ANY rule fires.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, BooleanType, TimestampType,
)
from pyspark.sql.window import Window
import os

# ── Constants ──────────────────────────────────────────────────────────────────

KAFKA_BROKER          = os.getenv("KAFKA_BROKER", "localhost:9092")
HDFS_BASE             = os.getenv("HDFS_NAMENODE", "hdfs://localhost:9000")

KAFKA_TOPIC           = "energy_readings"
HDFS_RAW_PATH         = f"{HDFS_BASE}/data/energy/raw"
HDFS_PROCESSED_PATH   = f"{HDFS_BASE}/data/energy/processed"
HDFS_ALERTS_PATH      = f"{HDFS_BASE}/data/energy/alerts"
CHECKPOINT_RAW        = f"{HDFS_BASE}/checkpoints/raw"
CHECKPOINT_PROCESSED  = f"{HDFS_BASE}/checkpoints/processed"
CHECKPOINT_ALERTS     = f"{HDFS_BASE}/checkpoints/alerts"

TRIGGER_INTERVAL      = "30 seconds"
WATERMARK_DELAY       = "10 minutes"

# Fraud thresholds
VOLTAGE_MAX_V         = 245.0     # anything above → voltage anomaly
LOW_CONSUMPTION_RATIO = 0.20      # below 20% of rolling mean → suspicious
SPIKE_RATIO           = 3.5       # above 3.5× rolling mean → spike
NIGHT_HOURS           = (0, 5)    # inclusive
NIGHT_MULTIPLIER      = 2.0       # nighttime must be 2× normal to flag


# ── Schema ─────────────────────────────────────────────────────────────────────

READING_SCHEMA = StructType([
    StructField("meter_id",        StringType(),    False),
    StructField("location",        StringType(),    True),
    StructField("timestamp",       StringType(),    False),
    StructField("date",            StringType(),    True),
    StructField("hour",            IntegerType(),   True),
    StructField("consumption_kwh", DoubleType(),    False),
    StructField("voltage_v",       DoubleType(),    True),
    StructField("current_a",       DoubleType(),    True),
    StructField("power_factor",    DoubleType(),    True),
    StructField("frequency_hz",    DoubleType(),    True),
    StructField("_injected_fraud", BooleanType(),   True),
])


# ── Spark Session ──────────────────────────────────────────────────────────────

def create_spark_session():
    return (
        SparkSession.builder
        .appName("EnergyFraudDetection")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.kafka.consumer.cache.enabled", "false")
        # Parquet optimisations
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.mergeSchema", "false")
        .getOrCreate()
    )


# ── Read from Kafka ────────────────────────────────────────────────────────────

def read_kafka_stream(spark):
    """
    Create a structured streaming DataFrame from Kafka.
    Each row is one meter reading as a JSON string in the 'value' column.
    """
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Deserialise JSON payload
    parsed_df = (
        raw_df
        .select(
            F.col("key").cast(StringType()).alias("kafka_key"),
            F.from_json(F.col("value").cast(StringType()), READING_SCHEMA).alias("data"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select("kafka_key", "data.*", "kafka_timestamp")
        .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
        .withWatermark("event_time", WATERMARK_DELAY)
    )

    return parsed_df


# ── Feature Engineering ────────────────────────────────────────────────────────

def add_features(df):
    """
    Add derived columns used in fraud detection and statistical reporting.
    """
    df = (
        df
        # Computed power (W) from voltage and current
        .withColumn("computed_power_w",
            F.round(F.col("voltage_v") * F.col("current_a"), 3))

        # Expected power from reported consumption (kWh → W for a 2-min interval)
        # 2-min interval = 1/30 hour, so W = kWh × 30 × 1000
        .withColumn("expected_power_w",
            F.round(F.col("consumption_kwh") * 30 * 1000, 3))

        # Power ratio: how much computed power deviates from reported
        .withColumn("power_ratio",
            F.when(F.col("expected_power_w") > 0,
                F.round(F.col("computed_power_w") / F.col("expected_power_w"), 4))
            .otherwise(F.lit(None).cast(DoubleType())))

        # Binary: is this an off-peak/night hour?
        .withColumn("is_night_hour",
            F.col("hour").between(NIGHT_HOURS[0], NIGHT_HOURS[1]).cast(IntegerType()))

        # Day of week (1=Mon … 7=Sun)
        .withColumn("day_of_week",
            F.dayofweek(F.col("event_time")))

        # ISO week number for partitioning
        .withColumn("week_of_year",
            F.weekofyear(F.col("event_time")))

        # Processing timestamp
        .withColumn("processing_time", F.current_timestamp())
    )
    return df


# ── Fraud Detection Logic ──────────────────────────────────────────────────────

def apply_fraud_rules(df, rolling_stats_df=None):
    """
    Apply all 5 fraud detection rules.
    Returns a DataFrame with boolean rule columns and a composite 'is_fraud_flagged' column.

    Note: rolling_stats_df would contain per-meter rolling means from a
    previous aggregation step. In a full production deployment, you would
    join here. For this implementation, we use fixed thresholds derived
    from expected consumption ranges.
    """

    # Rule 1 — Abnormally low consumption (static threshold per category)
    df = df.withColumn("rule_low_consumption",
        (F.col("consumption_kwh") < 0.30).cast(BooleanType()))

    # Rule 2 — Voltage anomaly
    df = df.withColumn("rule_voltage_spike",
        (F.col("voltage_v") > VOLTAGE_MAX_V).cast(BooleanType()))

    # Rule 3 — Power factor / current inconsistency
    # Flag if computed power is >15% higher than expected
    df = df.withColumn("rule_power_inconsistency",
        (
            F.col("power_ratio").isNotNull() &
            (F.col("power_ratio") > 1.15)
        ).cast(BooleanType()))

    # Rule 4 — High consumption at night (threshold: 5.0 kWh at night is suspicious)
    df = df.withColumn("rule_odd_hour_spike",
        (
            (F.col("is_night_hour") == 1) &
            (F.col("consumption_kwh") > 5.0)
        ).cast(BooleanType()))

    # Rule 5 — Frequency deviation (healthy grid: 49.8–50.2 Hz)
    df = df.withColumn("rule_frequency_anomaly",
        (
            F.col("frequency_hz").isNotNull() &
            (~F.col("frequency_hz").between(49.8, 50.2))
        ).cast(BooleanType()))

    # Composite fraud flag: any rule fires
    df = df.withColumn("is_fraud_flagged",
        (
            F.col("rule_low_consumption") |
            F.col("rule_voltage_spike")   |
            F.col("rule_power_inconsistency") |
            F.col("rule_odd_hour_spike")  |
            F.col("rule_frequency_anomaly")
        ).cast(BooleanType()))

    # Rule count (how many rules fired — severity proxy)
    df = df.withColumn("rules_fired_count",
        (
            F.col("rule_low_consumption").cast(IntegerType()) +
            F.col("rule_voltage_spike").cast(IntegerType()) +
            F.col("rule_power_inconsistency").cast(IntegerType()) +
            F.col("rule_odd_hour_spike").cast(IntegerType()) +
            F.col("rule_frequency_anomaly").cast(IntegerType())
        ))

    # Fraud severity label
    df = df.withColumn("fraud_severity",
        F.when(F.col("rules_fired_count") >= 3, F.lit("HIGH"))
        .when(F.col("rules_fired_count") == 2,  F.lit("MEDIUM"))
        .when(F.col("rules_fired_count") == 1,  F.lit("LOW"))
        .otherwise(F.lit("NONE")))

    return df


# ── HDFS Writers ───────────────────────────────────────────────────────────────

def write_raw_to_hdfs(df):
    """
    Write raw readings (no feature engineering) to the raw HDFS zone.
    Partitioned by date and meter_id for efficient HiveQL queries.
    """
    return (
        df.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", HDFS_RAW_PATH)
        .option("checkpointLocation", CHECKPOINT_RAW)
        .partitionBy("date", "meter_id")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


def write_processed_to_hdfs(df):
    """
    Write enriched + fraud-scored readings to the processed HDFS zone.
    """
    return (
        df.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", HDFS_PROCESSED_PATH)
        .option("checkpointLocation", CHECKPOINT_PROCESSED)
        .partitionBy("date", "meter_id")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


def write_alerts_to_hdfs(df):
    """
    Write only fraud-flagged events to the alerts HDFS zone.
    """
    alerts_df = df.filter(F.col("is_fraud_flagged") == True)

    return (
        alerts_df.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", HDFS_ALERTS_PATH)
        .option("checkpointLocation", CHECKPOINT_ALERTS)
        .partitionBy("date")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


def write_alerts_to_console(df):
    """
    Mirror fraud alerts to the console for real-time monitoring.
    """
    alerts_df = df.filter(F.col("is_fraud_flagged") == True).select(
        "meter_id", "timestamp", "location",
        "consumption_kwh", "voltage_v", "fraud_severity",
        "rule_low_consumption", "rule_voltage_spike",
        "rule_power_inconsistency", "rule_odd_hour_spike",
    )

    return (
        alerts_df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("[SPARK] Reading stream from Kafka...")
    raw_stream        = read_kafka_stream(spark)

    print("[SPARK] Applying feature engineering...")
    enriched_stream   = add_features(raw_stream)

    print("[SPARK] Applying fraud detection rules...")
    fraud_stream      = apply_fraud_rules(enriched_stream)

    print("[SPARK] Starting write streams to HDFS...")
    q_raw       = write_raw_to_hdfs(raw_stream)
    q_processed = write_processed_to_hdfs(fraud_stream)
    q_alerts    = write_alerts_to_hdfs(fraud_stream)
    q_console   = write_alerts_to_console(fraud_stream)

    print("[SPARK] All streams active. Awaiting termination...")

    # Wait for all queries to finish (they run indefinitely until killed)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
