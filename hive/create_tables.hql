-- =============================================================================
-- Hive Table Definitions — Energy Fraud Detection
-- =============================================================================
-- Run these DDL statements in the Hive shell or via beeline.
-- Tables are EXTERNAL: data lives in HDFS and is NOT deleted when you DROP TABLE.
-- Partitioning mirrors the Parquet directory structure written by Spark.
-- =============================================================================

-- Use or create a dedicated database
CREATE DATABASE IF NOT EXISTS energy_fraud
  COMMENT 'Energy fraud detection - smart meter data'
  LOCATION 'hdfs://namenode:9000/data/energy';

USE energy_fraud;


-- =============================================================================
-- Table 1: raw_readings
-- External table on top of the raw HDFS zone.
-- Mirrors the JSON fields produced by the simulator (no fraud scoring).
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS raw_readings (
    kafka_key          STRING    COMMENT 'Kafka message key',
    location           STRING    COMMENT 'Geographical zone',
    `timestamp`        STRING    COMMENT 'ISO-8601 event timestamp (UTC)',
    hour               INT       COMMENT 'Hour of the day 0-23',
    consumption_kwh    DOUBLE    COMMENT 'Reported electricity consumption (kWh)',
    voltage_v          DOUBLE    COMMENT 'Measured voltage (Volts)',
    current_a          DOUBLE    COMMENT 'Measured current (Amperes)',
    power_factor       DOUBLE    COMMENT 'Power factor 0..1',
    frequency_hz       DOUBLE    COMMENT 'Grid frequency (Hz)',
    `_injected_fraud`  BOOLEAN   COMMENT 'Simulator ground-truth fraud flag',
    kafka_timestamp    TIMESTAMP COMMENT 'Timestamp when Kafka received the message',
    event_time         TIMESTAMP COMMENT 'Parsed event timestamp'
)
PARTITIONED BY (
    `date`             STRING    COMMENT 'Partition: event date YYYY-MM-DD',
    meter_id           STRING    COMMENT 'Partition: meter identifier'
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/energy/raw'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'discover.partitions'='true'
);

-- Register existing Parquet partitions with the Hive metastore
MSCK REPAIR TABLE raw_readings;


-- =============================================================================
-- Table 2: meter_features
-- Enriched and fraud-scored readings produced by Spark Streaming.
-- Contains all fraud rule columns and severity labels.
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS meter_features (
    kafka_key               STRING,
    location                STRING,
    `timestamp`             STRING,
    hour                    INT,
    consumption_kwh         DOUBLE,
    voltage_v               DOUBLE,
    current_a               DOUBLE,
    power_factor            DOUBLE,
    frequency_hz            DOUBLE,
    `_injected_fraud`       BOOLEAN,
    kafka_timestamp         TIMESTAMP,
    event_time              TIMESTAMP,
    computed_power_w        DOUBLE    COMMENT 'Derived: V x I',
    expected_power_w        DOUBLE    COMMENT 'Derived from reported consumption',
    power_ratio             DOUBLE    COMMENT 'computed / expected',
    is_night_hour           INT       COMMENT '1 if hour between 0 and 5',
    day_of_week             INT       COMMENT '1=Sunday ... 7=Saturday',
    week_of_year            INT,
    -- Fraud rule flags
    rule_low_consumption    BOOLEAN   COMMENT 'Rule 1: abnormally low kWh',
    rule_voltage_spike      BOOLEAN   COMMENT 'Rule 2: voltage > 245 V',
    rule_power_inconsistency BOOLEAN  COMMENT 'Rule 3: power ratio > 1.15',
    rule_odd_hour_spike     BOOLEAN   COMMENT 'Rule 4: high night consumption',
    rule_frequency_anomaly  BOOLEAN   COMMENT 'Rule 5: frequency out of range',
    -- Composite fraud assessment
    is_fraud_flagged        BOOLEAN   COMMENT 'Any rule fired',
    rules_fired_count       INT       COMMENT 'Number of rules that fired',
    fraud_severity          STRING    COMMENT 'NONE | LOW | MEDIUM | HIGH',
    processing_time         TIMESTAMP COMMENT 'When Spark processed this record'
)
PARTITIONED BY (
    `date`    STRING,
    meter_id  STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/energy/processed'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'discover.partitions'='true'
);

MSCK REPAIR TABLE meter_features;


-- =============================================================================
-- Table 3: fraud_alerts
-- Subset of processed data — only fraud-flagged events.
-- Partitioned by date only for fast date-range scans.
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS fraud_alerts (
    kafka_key              STRING,
    meter_id              STRING,
    location              STRING,
    `timestamp`           STRING,
    hour                  INT,
    consumption_kwh       DOUBLE,
    voltage_v             DOUBLE,
    current_a             DOUBLE,
    power_factor          DOUBLE,
    frequency_hz          DOUBLE,
    `_injected_fraud`     BOOLEAN,
    kafka_timestamp       TIMESTAMP,
    event_time            TIMESTAMP,
    computed_power_w      DOUBLE,
    expected_power_w      DOUBLE,
    power_ratio           DOUBLE,
    is_night_hour         INT,
    day_of_week           INT,
    week_of_year          INT,
    rule_low_consumption  BOOLEAN,
    rule_voltage_spike    BOOLEAN,
    rule_power_inconsistency BOOLEAN,
    rule_odd_hour_spike   BOOLEAN,
    rule_frequency_anomaly BOOLEAN,
    is_fraud_flagged      BOOLEAN,
    rules_fired_count     INT,
    fraud_severity        STRING,
    processing_time       TIMESTAMP
)
PARTITIONED BY (`date` STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/energy/alerts'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'discover.partitions'='true'
);

MSCK REPAIR TABLE fraud_alerts;


-- =============================================================================
-- Optimisation: Enable ORC compression for analytic queries
-- (Optional - switch to ORC for Hive-intensive workloads)
-- =============================================================================
-- CREATE TABLE meter_features_orc LIKE meter_features STORED AS ORC;
-- INSERT INTO meter_features_orc SELECT * FROM meter_features;

-- =============================================================================
-- Verify tables
-- =============================================================================
SHOW TABLES;
DESCRIBE FORMATTED meter_features;
