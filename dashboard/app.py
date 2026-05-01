from datetime import datetime, timezone

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from data import KAFKA_BROKER, KAFKA_TOPIC, MAX_MESSAGES, REFRESH_MS, read_kafka_messages, score_records
from streamlit_autorefresh import st_autorefresh


st.set_page_config(
    page_title="Energy Fraud Live Dashboard",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)


st.markdown(
    """
    <style>
      .stApp {
        background:
          radial-gradient(circle at 18% 12%, rgba(0, 184, 148, .16), transparent 28%),
          radial-gradient(circle at 88% 16%, rgba(9, 132, 227, .12), transparent 26%),
          linear-gradient(135deg, #071013 0%, #101820 47%, #15171d 100%);
        color: #eef5f3;
      }
      [data-testid="stSidebar"] {
        background: rgba(7, 16, 19, .92);
        border-right: 1px solid rgba(255, 255, 255, .08);
      }
      [data-testid="stMetric"] {
        background: rgba(255, 255, 255, .07);
        border: 1px solid rgba(255, 255, 255, .10);
        border-radius: 8px;
        padding: 14px 16px;
        min-height: 108px;
      }
      [data-testid="stMetricLabel"] {
        color: rgba(238, 245, 243, .72);
      }
      .block-container {
        padding-top: 1.7rem;
        padding-bottom: 2rem;
      }
      .hero {
        border-bottom: 1px solid rgba(255, 255, 255, .10);
        padding-bottom: 1rem;
        margin-bottom: 1rem;
      }
      .hero h1 {
        font-size: clamp(2rem, 4vw, 4.2rem);
        line-height: 1.02;
        margin: 0 0 .35rem 0;
        letter-spacing: 0;
      }
      .hero p {
        color: rgba(238, 245, 243, .72);
        font-size: 1rem;
        margin: 0;
      }
      .status-pill {
        display: inline-flex;
        align-items: center;
        gap: .45rem;
        padding: .4rem .7rem;
        border-radius: 999px;
        background: rgba(0, 184, 148, .14);
        border: 1px solid rgba(0, 184, 148, .38);
        color: #b9fff1;
        font-weight: 600;
        font-size: .86rem;
      }
      .status-pill.warn {
        background: rgba(255, 118, 117, .14);
        border-color: rgba(255, 118, 117, .42);
        color: #ffd3d3;
      }
      .section-title {
        color: rgba(238, 245, 243, .86);
        font-size: .9rem;
        font-weight: 700;
        letter-spacing: .08em;
        text-transform: uppercase;
        margin: .6rem 0 .4rem;
      }
      div[data-testid="stDataFrame"] {
        border-radius: 8px;
        overflow: hidden;
      }
    </style>
    """,
    unsafe_allow_html=True,
)


def utc_now_label() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S UTC")


@st.cache_data(ttl=2, show_spinner=False)
def load_records(max_messages: int) -> tuple[list[dict], str | None]:
    return read_kafka_messages(max_messages)


def plot_empty(message: str):
    fig = go.Figure()
    fig.add_annotation(text=message, x=0.5, y=0.5, showarrow=False, font={"size": 18})
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    fig.update_layout(height=320, margin={"l": 0, "r": 0, "t": 0, "b": 0})
    st.plotly_chart(fig, use_container_width=True)


st_autorefresh(interval=REFRESH_MS, key="dashboard_refresh")

with st.sidebar:
    st.title("Controls")
    st.caption("Live data source")
    st.code(f"{KAFKA_BROKER}\n{KAFKA_TOPIC}", language="text")
    window = st.slider("Messages to analyze", 100, MAX_MESSAGES, min(1000, MAX_MESSAGES), step=100)
    refresh_seconds = REFRESH_MS / 1000
    st.caption(f"Auto refresh: every {refresh_seconds:.0f}s")
    st.divider()
    st.caption("Project dashboards")
    st.link_button("HDFS NameNode", "http://localhost:9870", use_container_width=True)
    st.link_button("Spark Master", "http://localhost:18080", use_container_width=True)


records, error = load_records(window)
df = score_records(records)

last_event = None
if not df.empty and df["event_time"].notna().any():
    last_event = df["event_time"].dropna().max().strftime("%H:%M:%S UTC")

status_text = "Kafka connected" if error is None else "Kafka unavailable"
status_class = "" if error is None else " warn"

st.markdown(
    f"""
    <div class="hero">
      <div class="status-pill{status_class}">{status_text} / refreshed {utc_now_label()}</div>
      <h1>Energy Fraud Live Dashboard</h1>
      <p>Real-time smart meter telemetry, anomaly rules, severity trends, and latest fraud alerts.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

if error:
    st.warning(f"Waiting for Kafka data: {error}")

total = len(df)
fraud = int(df["is_fraud_flagged"].sum()) if total else 0
rate = (fraud / total * 100) if total else 0
meters = int(df["meter_id"].nunique()) if total and "meter_id" in df else 0
avg_consumption = float(df["consumption_kwh"].mean()) if total else 0.0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Messages", f"{total:,}")
k2.metric("Fraud alerts", f"{fraud:,}")
k3.metric("Alert rate", f"{rate:.1f}%")
k4.metric("Active meters", f"{meters}")
k5.metric("Avg kWh", f"{avg_consumption:.2f}", help="Average consumption across loaded messages")

left, right = st.columns((1.7, 1.0))

with left:
    st.markdown('<div class="section-title">Telemetry Flow</div>', unsafe_allow_html=True)
    if df.empty:
        plot_empty("Waiting for meter readings")
    else:
        timeline = (
            df.groupby(["minute", "is_fraud_flagged"], as_index=False)
            .size()
            .rename(columns={"size": "events"})
        )
        fig = px.area(
            timeline,
            x="minute",
            y="events",
            color="is_fraud_flagged",
            color_discrete_map={False: "#00b894", True: "#ff7675"},
            labels={"minute": "", "events": "Events", "is_fraud_flagged": "Fraud"},
        )
        fig.update_layout(
            height=360,
            margin={"l": 10, "r": 10, "t": 20, "b": 10},
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(255,255,255,0.03)",
            legend_title_text="Flagged",
        )
        st.plotly_chart(fig, use_container_width=True)

with right:
    st.markdown('<div class="section-title">Severity Mix</div>', unsafe_allow_html=True)
    if df.empty:
        plot_empty("No severity data yet")
    else:
        severity_order = ["NONE", "LOW", "MEDIUM", "HIGH"]
        severity = df["fraud_severity"].value_counts().reindex(severity_order, fill_value=0).reset_index()
        severity.columns = ["severity", "events"]
        fig = px.bar(
            severity,
            x="severity",
            y="events",
            color="severity",
            color_discrete_map={
                "NONE": "#74b9ff",
                "LOW": "#fdcb6e",
                "MEDIUM": "#e17055",
                "HIGH": "#d63031",
            },
        )
        fig.update_layout(
            height=360,
            margin={"l": 10, "r": 10, "t": 20, "b": 10},
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(255,255,255,0.03)",
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

zone_col, meter_col = st.columns((1.0, 1.4))

with zone_col:
    st.markdown('<div class="section-title">Zone Risk</div>', unsafe_allow_html=True)
    if df.empty or "location" not in df:
        plot_empty("No zone activity")
    else:
        zone = (
            df.groupby("location", as_index=False)
            .agg(events=("meter_id", "count"), alerts=("is_fraud_flagged", "sum"))
            .sort_values("alerts", ascending=False)
        )
        zone["risk_rate"] = (zone["alerts"] / zone["events"] * 100).round(1)
        fig = px.treemap(
            zone,
            path=["location"],
            values="events",
            color="risk_rate",
            color_continuous_scale=["#00b894", "#fdcb6e", "#d63031"],
            hover_data={"alerts": True, "risk_rate": True},
        )
        fig.update_layout(
            height=330,
            margin={"l": 0, "r": 0, "t": 10, "b": 0},
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, use_container_width=True)

with meter_col:
    st.markdown('<div class="section-title">Meter Consumption</div>', unsafe_allow_html=True)
    if df.empty:
        plot_empty("No meter activity")
    else:
        fig = px.scatter(
            df.tail(450),
            x="event_time",
            y="consumption_kwh",
            color="fraud_severity",
            size="rules_fired_count",
            hover_name="meter_id",
            hover_data=["location", "voltage_v", "frequency_hz"],
            color_discrete_map={
                "NONE": "#74b9ff",
                "LOW": "#fdcb6e",
                "MEDIUM": "#e17055",
                "HIGH": "#d63031",
            },
            labels={"event_time": "", "consumption_kwh": "kWh"},
        )
        fig.update_layout(
            height=330,
            margin={"l": 10, "r": 10, "t": 10, "b": 10},
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(255,255,255,0.03)",
            legend_title_text="Severity",
        )
        st.plotly_chart(fig, use_container_width=True)

st.markdown('<div class="section-title">Latest Alerts</div>', unsafe_allow_html=True)
if df.empty:
    st.info("No readings are available yet. Start the Compose stack and wait for the simulator.")
else:
    alert_cols = [
        "event_time",
        "meter_id",
        "location",
        "consumption_kwh",
        "voltage_v",
        "frequency_hz",
        "fraud_severity",
        "rules_fired_count",
        "rule_low_consumption",
        "rule_voltage_spike",
        "rule_power_inconsistency",
        "rule_odd_hour_spike",
        "rule_frequency_anomaly",
    ]
    alerts = df[df["is_fraud_flagged"]].sort_values("event_time", ascending=False)
    st.dataframe(
        alerts[alert_cols].head(40),
        use_container_width=True,
        hide_index=True,
        height=360,
    )

st.caption(
    f"Last event: {last_event or 'none yet'} | Loaded from Kafka topic '{KAFKA_TOPIC}' | "
    "Fraud rules mirror the Spark streaming job."
)
