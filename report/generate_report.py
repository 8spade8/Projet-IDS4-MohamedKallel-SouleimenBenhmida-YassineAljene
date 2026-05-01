"""
Final Statistical Report — Energy Fraud Detection
===================================================
Connects to Hive via PyHive, runs the master summary query,
and generates a formatted PDF/HTML report with charts.

Prerequisites:
  pip install pyhive thrift sasl thrift-sasl pandas matplotlib seaborn jinja2
"""

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns
from pyhive import hive
from datetime import datetime
import os

# ── Config ─────────────────────────────────────────────────────────────────────

HIVE_HOST   = "localhost"
HIVE_PORT   = 10000
HIVE_DB     = "energy_fraud"
REPORT_DIR  = "./report_output"
os.makedirs(REPORT_DIR, exist_ok=True)

sns.set_theme(style="whitegrid", palette="muted")
PALETTE = sns.color_palette("Set2")


# ── Hive Connection ────────────────────────────────────────────────────────────

def get_hive_connection():
    conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, database=HIVE_DB)
    cursor = conn.cursor()
    cursor.execute("set hive.vectorized.execution.enabled=false")
    cursor.execute("set hive.vectorized.execution.reduce.enabled=false")
    cursor.close()
    return conn


def run_query(conn, sql):
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        return pd.DataFrame(rows, columns=columns)
    finally:
        cursor.close()


# ── Queries ────────────────────────────────────────────────────────────────────

SQL_MASTER_SUMMARY = """
SELECT
    meter_id, location,
    COUNT(*) AS total_readings,
    SUM(CAST(is_fraud_flagged AS INT)) AS fraud_flag_count,
    ROUND(SUM(CAST(is_fraud_flagged AS INT)) * 100.0 / COUNT(*), 2) AS fraud_rate_pct,
    ROUND(AVG(consumption_kwh), 4)     AS avg_kwh,
    ROUND(STDDEV(consumption_kwh), 4)  AS stddev_kwh,
    ROUND(AVG(voltage_v), 2)           AS avg_voltage,
    ROUND(MAX(voltage_v), 2)           AS max_voltage,
    SUM(CAST(rule_low_consumption AS INT))      AS rule1_fires,
    SUM(CAST(rule_voltage_spike AS INT))        AS rule2_fires,
    SUM(CAST(rule_power_inconsistency AS INT))  AS rule3_fires,
    SUM(CAST(rule_odd_hour_spike AS INT))       AS rule4_fires,
    SUM(CAST(rule_frequency_anomaly AS INT))    AS rule5_fires,
    SUM(CASE WHEN fraud_severity = 'HIGH'   THEN 1 ELSE 0 END) AS high_alerts,
    SUM(CASE WHEN fraud_severity = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_alerts,
    SUM(CASE WHEN fraud_severity = 'LOW'    THEN 1 ELSE 0 END) AS low_alerts
FROM meter_features
GROUP BY meter_id, location
ORDER BY fraud_flag_count DESC
"""

SQL_HOURLY = """
SELECT hour, ROUND(AVG(consumption_kwh), 4) AS avg_kwh,
       SUM(CAST(is_fraud_flagged AS INT)) AS fraud_count
FROM meter_features
GROUP BY hour
ORDER BY hour
"""

SQL_DAILY_TREND = """
SELECT `date` AS event_date,
       ROUND(AVG(consumption_kwh), 4) AS daily_avg_kwh,
       SUM(CAST(is_fraud_flagged AS INT)) AS daily_fraud_count
FROM meter_features
GROUP BY `date`
ORDER BY `date`
"""

SQL_SEVERITY_DIST = """
SELECT fraud_severity, COUNT(*) AS count
FROM meter_features
WHERE is_fraud_flagged = TRUE
GROUP BY fraud_severity
"""


# ── Chart Generators ───────────────────────────────────────────────────────────

def plot_fraud_rate_by_meter(df_summary, ax):
    colors = ["#e74c3c" if r > 30 else "#f39c12" if r > 10 else "#2ecc71"
              for r in df_summary["fraud_rate_pct"]]
    bars = ax.barh(df_summary["meter_id"], df_summary["fraud_rate_pct"], color=colors)
    ax.set_xlabel("Fraud rate (%)")
    ax.set_title("Fraud rate per meter")
    ax.axvline(x=30, color="red", linestyle="--", linewidth=0.8, label="30% threshold")
    ax.legend(fontsize=8)
    for bar, val in zip(bars, df_summary["fraud_rate_pct"]):
        ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                f"{val:.1f}%", va="center", fontsize=8)


def plot_avg_consumption_by_meter(df_summary, ax):
    ax.bar(df_summary["meter_id"], df_summary["avg_kwh"],
           color=PALETTE, edgecolor="white", linewidth=0.5)
    ax.set_xlabel("Meter ID")
    ax.set_ylabel("Average kWh per reading")
    ax.set_title("Average consumption per meter")
    plt.setp(ax.get_xticklabels(), rotation=35, ha="right", fontsize=8)


def plot_hourly_pattern(df_hourly, ax):
    ax2 = ax.twinx()
    ax.plot(df_hourly["hour"], df_hourly["avg_kwh"],
            color="#3498db", marker="o", markersize=4, linewidth=1.5, label="Avg kWh")
    ax2.bar(df_hourly["hour"], df_hourly["fraud_count"],
            color="#e74c3c", alpha=0.35, label="Fraud alerts")
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Average kWh", color="#3498db")
    ax2.set_ylabel("Fraud alerts", color="#e74c3c")
    ax.set_title("Hourly consumption vs. fraud alerts")
    ax.set_xticks(range(0, 24))
    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, fontsize=7)


def plot_rule_contributions(df_summary, ax):
    rules = {
        "Rule 1\nLow consump.": df_summary["rule1_fires"].sum(),
        "Rule 2\nVoltage spike": df_summary["rule2_fires"].sum(),
        "Rule 3\nPower incons.": df_summary["rule3_fires"].sum(),
        "Rule 4\nOdd-hr spike": df_summary["rule4_fires"].sum(),
        "Rule 5\nFreq. anomaly": df_summary["rule5_fires"].sum(),
    }
    ax.bar(rules.keys(), rules.values(), color=PALETTE[:5], edgecolor="white")
    ax.set_title("Fraud rule fire count (all meters)")
    ax.set_ylabel("Times fired")


def plot_daily_trend(df_daily, ax):
    ax.plot(df_daily["event_date"], df_daily["daily_avg_kwh"],
            color="#3498db", linewidth=1.5, label="Daily avg kWh")
    ax.fill_between(df_daily["event_date"], df_daily["daily_avg_kwh"],
                    alpha=0.15, color="#3498db")
    ax2 = ax.twinx()
    ax2.bar(df_daily["event_date"], df_daily["daily_fraud_count"],
            color="#e74c3c", alpha=0.4, label="Fraud alerts")
    ax.set_xlabel("Date")
    ax.set_ylabel("Avg kWh", color="#3498db")
    ax2.set_ylabel("Fraud alerts", color="#e74c3c")
    ax.set_title("Daily consumption trend vs fraud alerts")
    plt.setp(ax.get_xticklabels(), rotation=35, ha="right", fontsize=7)


def plot_severity_pie(df_sev, ax):
    color_map = {"HIGH": "#e74c3c", "MEDIUM": "#f39c12", "LOW": "#2ecc71", "NONE": "#bdc3c7"}
    colors = [color_map.get(s, "#bdc3c7") for s in df_sev["fraud_severity"]]
    ax.pie(df_sev["count"], labels=df_sev["fraud_severity"], colors=colors,
           autopct="%1.1f%%", startangle=140, textprops={"fontsize": 9})
    ax.set_title("Fraud alert severity distribution")


# ── Report Assembly ────────────────────────────────────────────────────────────

def build_report(conn):
    print("[REPORT] Fetching data from Hive...")
    df_summary = run_query(conn, SQL_MASTER_SUMMARY)
    df_hourly  = run_query(conn, SQL_HOURLY)
    df_daily   = run_query(conn, SQL_DAILY_TREND)
    df_sev     = run_query(conn, SQL_SEVERITY_DIST)

    print("[REPORT] Building charts...")

    fig = plt.figure(figsize=(18, 22))
    fig.suptitle(
        "Energy Fraud Detection — Statistical Analysis Report\n"
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        fontsize=16, fontweight="bold", y=0.98
    )

    gs = gridspec.GridSpec(3, 2, figure=fig, hspace=0.45, wspace=0.35)

    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])
    ax3 = fig.add_subplot(gs[1, 0])
    ax4 = fig.add_subplot(gs[1, 1])
    ax5 = fig.add_subplot(gs[2, 0])
    ax6 = fig.add_subplot(gs[2, 1])

    plot_fraud_rate_by_meter(df_summary, ax1)
    plot_avg_consumption_by_meter(df_summary, ax2)
    plot_hourly_pattern(df_hourly, ax3)
    plot_rule_contributions(df_summary, ax4)
    plot_daily_trend(df_daily, ax5)
    if not df_sev.empty:
        plot_severity_pie(df_sev, ax6)
    else:
        ax6.text(0.5, 0.5, "No alert data yet", ha="center", va="center",
                 transform=ax6.transAxes, fontsize=12)

    chart_path = os.path.join(REPORT_DIR, "fraud_report.png")
    plt.savefig(chart_path, dpi=150, bbox_inches="tight")
    print(f"[REPORT] Chart saved to: {chart_path}")
    plt.close()

    # ── CSV export ────────────────────────────────────────────────────────────
    csv_path = os.path.join(REPORT_DIR, "fraud_summary.csv")
    df_summary.to_csv(csv_path, index=False)
    print(f"[REPORT] Summary CSV saved to: {csv_path}")

    # ── Console summary ───────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  FRAUD DETECTION SUMMARY REPORT")
    print("=" * 60)
    total_readings    = df_summary["total_readings"].sum()
    total_fraud       = df_summary["fraud_flag_count"].sum()
    overall_fraud_pct = total_fraud * 100.0 / total_readings if total_readings else 0

    print(f"  Total readings analysed : {total_readings:,}")
    print(f"  Fraud-flagged readings  : {total_fraud:,} ({overall_fraud_pct:.2f}%)")
    print(f"  Meters monitored        : {len(df_summary)}")
    print(f"  Meters with >30% fraud  : {len(df_summary[df_summary['fraud_rate_pct'] > 30])}")
    print()
    print(df_summary[["meter_id", "fraud_rate_pct", "avg_kwh", "fraud_flag_count"]].to_string(index=False))
    print("=" * 60)


# ── Entry Point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    conn = get_hive_connection()
    try:
        build_report(conn)
    finally:
        conn.close()
