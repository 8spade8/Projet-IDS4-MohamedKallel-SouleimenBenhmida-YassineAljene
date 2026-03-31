-- =============================================================================
-- Hive Fraud Detection Queries — Energy Fraud Detection
-- =============================================================================
-- Execute in Hive shell:  hive -f fraud_queries.hql
-- Or interactively:        beeline -u jdbc:hive2://localhost:10000
-- =============================================================================

USE energy_fraud;


-- =============================================================================
-- SECTION 1 — Overview / Sanity Checks
-- =============================================================================

-- Q1.1: Count total readings and flagged readings per day
SELECT
    date,
    COUNT(*)                                              AS total_readings,
    SUM(CAST(is_fraud_flagged AS INT))                   AS fraud_count,
    ROUND(
        SUM(CAST(is_fraud_flagged AS INT)) * 100.0 / COUNT(*), 2
    )                                                    AS fraud_rate_pct
FROM meter_features
GROUP BY date
ORDER BY date DESC;


-- Q1.2: Overview by meter — total readings, fraud count, fraud rate
SELECT
    meter_id,
    location,
    COUNT(*)                                              AS total_readings,
    SUM(CAST(is_fraud_flagged AS INT))                   AS fraud_flags,
    ROUND(AVG(consumption_kwh), 3)                       AS avg_kwh,
    ROUND(AVG(voltage_v), 2)                             AS avg_voltage,
    MAX(fraud_severity)                                   AS max_severity
FROM meter_features
GROUP BY meter_id, location
ORDER BY fraud_flags DESC;


-- =============================================================================
-- SECTION 2 — Rule-Level Analysis
-- =============================================================================

-- Q2.1: How often does each fraud rule fire?
SELECT
    'rule_low_consumption'     AS rule_name,
    SUM(CAST(rule_low_consumption AS INT))      AS fire_count
FROM meter_features
UNION ALL
SELECT
    'rule_voltage_spike',
    SUM(CAST(rule_voltage_spike AS INT))
FROM meter_features
UNION ALL
SELECT
    'rule_power_inconsistency',
    SUM(CAST(rule_power_inconsistency AS INT))
FROM meter_features
UNION ALL
SELECT
    'rule_odd_hour_spike',
    SUM(CAST(rule_odd_hour_spike AS INT))
FROM meter_features
UNION ALL
SELECT
    'rule_frequency_anomaly',
    SUM(CAST(rule_frequency_anomaly AS INT))
FROM meter_features
ORDER BY fire_count DESC;


-- Q2.2: Co-occurrence — which rules fire together most often?
SELECT
    rule_low_consumption,
    rule_voltage_spike,
    rule_power_inconsistency,
    rule_odd_hour_spike,
    rule_frequency_anomaly,
    COUNT(*) AS occurrences
FROM meter_features
WHERE is_fraud_flagged = TRUE
GROUP BY
    rule_low_consumption,
    rule_voltage_spike,
    rule_power_inconsistency,
    rule_odd_hour_spike,
    rule_frequency_anomaly
ORDER BY occurrences DESC
LIMIT 20;


-- =============================================================================
-- SECTION 3 — Time-Series Analysis
-- =============================================================================

-- Q3.1: Hourly average consumption per meter (detect unusual hour patterns)
SELECT
    meter_id,
    hour,
    ROUND(AVG(consumption_kwh), 4)           AS avg_kwh,
    ROUND(STDDEV_POP(consumption_kwh), 4)    AS stddev_kwh,
    COUNT(*)                                  AS reading_count
FROM meter_features
GROUP BY meter_id, hour
ORDER BY meter_id, hour;


-- Q3.2: Rolling 24-hour consumption trend per meter (self-join approach)
-- Compare today's total vs yesterday's total
SELECT
    a.meter_id,
    a.date                                    AS today,
    ROUND(SUM(a.consumption_kwh), 3)          AS today_kwh,
    ROUND(SUM(b.consumption_kwh), 3)          AS yesterday_kwh,
    ROUND(
        (SUM(a.consumption_kwh) - SUM(b.consumption_kwh))
        * 100.0 / NULLIF(SUM(b.consumption_kwh), 0), 2
    )                                         AS change_pct
FROM raw_readings a
LEFT JOIN raw_readings b
    ON  a.meter_id = b.meter_id
    AND DATE_SUB(a.date, 1) = b.date
GROUP BY a.meter_id, a.date
HAVING ABS(change_pct) > 30    -- flag >30% day-on-day change
ORDER BY ABS(change_pct) DESC;


-- Q3.3: Weekly consumption baseline per meter
-- Used to compute expected ranges for anomaly detection in Hive
SELECT
    meter_id,
    week_of_year,
    ROUND(AVG(consumption_kwh), 4)   AS weekly_avg_kwh,
    ROUND(STDDEV(consumption_kwh), 4) AS weekly_stddev_kwh,
    ROUND(MIN(consumption_kwh), 4)   AS weekly_min_kwh,
    ROUND(MAX(consumption_kwh), 4)   AS weekly_max_kwh
FROM meter_features
GROUP BY meter_id, week_of_year
ORDER BY meter_id, week_of_year;


-- =============================================================================
-- SECTION 4 — Statistical Fraud Scoring
-- =============================================================================

-- Q4.1: Z-score based anomaly detection (global average as reference)
--       Z > 3 is statistically extreme (>3 standard deviations from mean)
WITH stats AS (
    SELECT
        meter_id,
        AVG(consumption_kwh)     AS mean_kwh,
        STDDEV(consumption_kwh)  AS std_kwh
    FROM raw_readings
    GROUP BY meter_id
)
SELECT
    r.meter_id,
    r.`timestamp`,
    r.consumption_kwh,
    ROUND(s.mean_kwh, 4)         AS meter_mean_kwh,
    ROUND(s.std_kwh,  4)         AS meter_std_kwh,
    ROUND(
        (r.consumption_kwh - s.mean_kwh) / NULLIF(s.std_kwh, 0), 4
    )                            AS z_score,
    CASE
        WHEN ABS((r.consumption_kwh - s.mean_kwh) / NULLIF(s.std_kwh, 0)) > 3
        THEN 'EXTREME_ANOMALY'
        WHEN ABS((r.consumption_kwh - s.mean_kwh) / NULLIF(s.std_kwh, 0)) > 2
        THEN 'MILD_ANOMALY'
        ELSE 'NORMAL'
    END                          AS anomaly_label
FROM raw_readings r
JOIN stats s ON r.meter_id = s.meter_id
WHERE
    ABS((r.consumption_kwh - s.mean_kwh) / NULLIF(s.std_kwh, 0)) > 2
ORDER BY ABS(z_score) DESC
LIMIT 100;


-- Q4.2: IQR-based outlier detection per meter
--       Values below Q1 - 1.5×IQR or above Q3 + 1.5×IQR are outliers
WITH percentiles AS (
    SELECT
        meter_id,
        PERCENTILE_APPROX(consumption_kwh, 0.25) AS q1,
        PERCENTILE_APPROX(consumption_kwh, 0.50) AS median_kwh,
        PERCENTILE_APPROX(consumption_kwh, 0.75) AS q3
    FROM raw_readings
    GROUP BY meter_id
),
iqr_bounds AS (
    SELECT
        meter_id,
        q1,
        q3,
        q3 - q1                      AS iqr,
        q1 - 1.5 * (q3 - q1)        AS lower_fence,
        q3 + 1.5 * (q3 - q1)        AS upper_fence,
        median_kwh
    FROM percentiles
)
SELECT
    r.meter_id,
    r.`timestamp`,
    r.consumption_kwh,
    ROUND(b.lower_fence, 4)          AS lower_fence,
    ROUND(b.upper_fence, 4)          AS upper_fence,
    CASE
        WHEN r.consumption_kwh < b.lower_fence THEN 'BELOW_IQR_FENCE'
        WHEN r.consumption_kwh > b.upper_fence THEN 'ABOVE_IQR_FENCE'
    END                              AS iqr_anomaly_type
FROM raw_readings r
JOIN iqr_bounds b ON r.meter_id = b.meter_id
WHERE
    r.consumption_kwh < b.lower_fence
    OR r.consumption_kwh > b.upper_fence
ORDER BY r.meter_id, r.`timestamp`;


-- =============================================================================
-- SECTION 5 — Fraud Alert Analysis
-- =============================================================================

-- Q5.1: Most suspicious meters (ranked by fraud alert count)
SELECT
    meter_id,
    location,
    COUNT(*)                          AS alert_count,
    SUM(rules_fired_count)            AS total_rules_fired,
    ROUND(AVG(rules_fired_count), 2)  AS avg_rules_per_alert,
    MAX(fraud_severity)               AS max_severity,
    MIN(date)                         AS first_alert_date,
    MAX(date)                         AS last_alert_date
FROM fraud_alerts
GROUP BY meter_id, location
ORDER BY alert_count DESC;


-- Q5.2: Fraud alerts by hour of day (peak fraud hours)
SELECT
    hour,
    COUNT(*)                          AS alert_count,
    ROUND(AVG(consumption_kwh), 3)    AS avg_kwh_at_alert,
    ROUND(AVG(voltage_v), 2)          AS avg_voltage_at_alert
FROM fraud_alerts
GROUP BY hour
ORDER BY hour;


-- Q5.3: Fraud severity distribution
SELECT
    fraud_severity,
    COUNT(*)                          AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM fraud_alerts
GROUP BY fraud_severity
ORDER BY
    CASE fraud_severity
        WHEN 'HIGH'   THEN 1
        WHEN 'MEDIUM' THEN 2
        WHEN 'LOW'    THEN 3
        ELSE 4
    END;


-- =============================================================================
-- SECTION 6 — Final Statistical Report Query
-- =============================================================================

-- Q6: Master fraud summary (use this for the final report)
SELECT
    mf.meter_id,
    mf.location,

    -- Volume
    COUNT(*)                                              AS total_readings,
    SUM(CAST(mf.is_fraud_flagged AS INT))                AS fraud_flag_count,
    ROUND(SUM(CAST(mf.is_fraud_flagged AS INT)) * 100.0 / COUNT(*), 2)
                                                          AS fraud_rate_pct,

    -- Consumption statistics
    ROUND(AVG(mf.consumption_kwh), 4)                    AS avg_kwh,
    ROUND(STDDEV(mf.consumption_kwh), 4)                 AS stddev_kwh,
    ROUND(MIN(mf.consumption_kwh), 4)                    AS min_kwh,
    ROUND(MAX(mf.consumption_kwh), 4)                    AS max_kwh,
    ROUND(PERCENTILE_APPROX(mf.consumption_kwh, 0.5), 4) AS median_kwh,

    -- Voltage stats
    ROUND(AVG(mf.voltage_v), 2)                          AS avg_voltage,
    ROUND(MAX(mf.voltage_v), 2)                          AS max_voltage,

    -- Rule breakdown
    SUM(CAST(mf.rule_low_consumption AS INT))            AS rule1_fires,
    SUM(CAST(mf.rule_voltage_spike AS INT))              AS rule2_fires,
    SUM(CAST(mf.rule_power_inconsistency AS INT))        AS rule3_fires,
    SUM(CAST(mf.rule_odd_hour_spike AS INT))             AS rule4_fires,
    SUM(CAST(mf.rule_frequency_anomaly AS INT))          AS rule5_fires,

    -- Severity
    SUM(CASE WHEN mf.fraud_severity = 'HIGH'   THEN 1 ELSE 0 END) AS high_alerts,
    SUM(CASE WHEN mf.fraud_severity = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_alerts,
    SUM(CASE WHEN mf.fraud_severity = 'LOW'    THEN 1 ELSE 0 END) AS low_alerts

FROM meter_features mf
GROUP BY mf.meter_id, mf.location
ORDER BY fraud_flag_count DESC;
