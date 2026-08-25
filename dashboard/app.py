import io
import os
import requests
import pandas as pd
import streamlit as st


# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Maritime Intelligence Platform",
    page_icon="🚢",
    layout="wide"
)


# ============================================================
# CONFIG
# ============================================================

HDFS_URL = os.getenv(
    "HDFS_URL",
    "http://namenode:9870"
)

HISTORICAL_PATH = "/maritime/processed/ais"
BASELINE_PATH = "/maritime/processed/baseline"

BATCH_ANOMALY_PATH = "/maritime/processed/anomalies"

STREAMING_PATH = "/maritime/streaming"
STREAMING_ANOMALY_PATH = "/maritime/streaming/anomalies"


# ============================================================
# DASHBOARD LIMITS
# ============================================================

# We only load a small recent sample for the dashboard.
# This makes Streamlit much faster while keeping HDFS/Spark
# as the full data-processing layer.

HISTORICAL_LIMIT = 2000
STREAMING_LIMIT = 1000
ANOMALY_LIMIT = 500
BASELINE_LIMIT = 500


# ============================================================
# HDFS HELPERS
# ============================================================

def hdfs_list(path):

    url = (
        f"{HDFS_URL}/webhdfs/v1"
        f"{path}"
        "?op=LISTSTATUS"
    )

    response = requests.get(
        url,
        timeout=15
    )

    response.raise_for_status()

    return response.json()[
        "FileStatuses"
    ][
        "FileStatus"
    ]


def read_hdfs_file(file_path):

    try:

        url = (
            f"{HDFS_URL}"
            f"/webhdfs/v1"
            f"{file_path}"
            "?op=OPEN"
        )

        response = requests.get(
            url,
            allow_redirects=True,
            timeout=30
        )

        if response.status_code != 200:
            return pd.DataFrame()

        return pd.read_parquet(
            io.BytesIO(
                response.content
            )
        )

    except Exception:

        return pd.DataFrame()


# ============================================================
# FAST PARQUET READER
# ============================================================

def read_parquet_directory(
    path,
    limit=1000
):

    frames = []

    try:

        files = hdfs_list(path)

    except Exception:

        return pd.DataFrame()

    # --------------------------------------------------------
    # Collect parquet files/directories
    # --------------------------------------------------------

    parquet_files = []

    for file_info in files:

        name = file_info["pathSuffix"]

        # Direct parquet file
        if (
            file_info["type"] == "FILE"
            and name.endswith(".parquet")
        ):

            parquet_files.append(
                f"{path}/{name}"
            )

        # Partition directory
        elif file_info["type"] == "DIRECTORY":

            sub_path = f"{path}/{name}"

            try:

                sub_files = hdfs_list(
                    sub_path
                )

                for sub_file in sub_files:

                    sub_name = (
                        sub_file["pathSuffix"]
                    )

                    if (
                        sub_file["type"] == "FILE"
                        and sub_name.endswith(".parquet")
                    ):

                        parquet_files.append(
                            f"{sub_path}/{sub_name}"
                        )

            except Exception:

                continue

    # --------------------------------------------------------
    # Read newest files first
    # --------------------------------------------------------

    parquet_files = parquet_files[-20:]

    # --------------------------------------------------------
    # Read only until we have enough records
    # --------------------------------------------------------

    total_rows = 0

    for file_path in reversed(parquet_files):

        df = read_hdfs_file(
            file_path
        )

        if df.empty:
            continue

        frames.append(df)

        total_rows += len(df)

        if total_rows >= limit:
            break

    if not frames:

        return pd.DataFrame()

    result = pd.concat(
        frames,
        ignore_index=True
    )

    # --------------------------------------------------------
    # Keep only required dashboard sample
    # --------------------------------------------------------

    if len(result) > limit:

        result = result.tail(
            limit
        )

    return result.reset_index(
        drop=True
    )


# ============================================================
# LOAD DASHBOARD DATA
# ============================================================

@st.cache_data(ttl=10)
def load_dashboard_data():

    historical = read_parquet_directory(
        HISTORICAL_PATH,
        HISTORICAL_LIMIT
    )

    baseline = read_parquet_directory(
        BASELINE_PATH,
        BASELINE_LIMIT
    )

    batch_anomalies = read_parquet_directory(
        BATCH_ANOMALY_PATH,
        ANOMALY_LIMIT
    )

    streaming = read_parquet_directory(
        STREAMING_PATH,
        STREAMING_LIMIT
    )

    streaming_anomalies = read_parquet_directory(
        STREAMING_ANOMALY_PATH,
        ANOMALY_LIMIT
    )

    return (
        historical,
        baseline,
        batch_anomalies,
        streaming,
        streaming_anomalies
    )


(
    historical_df,
    baseline_df,
    batch_anomalies_df,
    streaming_df,
    streaming_anomalies_df
) = load_dashboard_data()


# ============================================================
# HEADER
# ============================================================

st.title(
    "🚢 Maritime Intelligence Platform"
)

st.caption(
    "Big Data Maritime Analytics — "
    "Kafka + Spark + HDFS + Airflow"
)


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.title(
    "⚙️ Platform"
)

st.sidebar.success(
    "System Online"
)

st.sidebar.info(
    "Dashboard uses a recent sample "
    "for fast visualization."
)

if st.sidebar.button(
    "🔄 Refresh Data"
):

    st.cache_data.clear()

    st.rerun()


# ============================================================
# KPI DATA
# ============================================================

stream_events = len(
    streaming_df
)

historical_records = len(
    historical_df
)

streaming_vessels = (
    streaming_df["mmsi"].nunique()
    if "mmsi" in streaming_df.columns
    else 0
)

historical_vessels = (
    historical_df["mmsi"].nunique()
    if "mmsi" in historical_df.columns
    else 0
)

batch_anomaly_count = len(
    batch_anomalies_df
)

streaming_anomaly_count = len(
    streaming_anomalies_df
)


# ============================================================
# TOP KPIs
# ============================================================

c1, c2, c3, c4, c5, c6 = st.columns(6)

c1.metric(
    "📡 Streaming Events",
    f"{stream_events:,}"
)

c2.metric(
    "🚢 Streaming Vessels",
    f"{streaming_vessels:,}"
)

c3.metric(
    "📊 Historical Records",
    f"{historical_records:,}"
)

c4.metric(
    "🚢 Historical Vessels",
    f"{historical_vessels:,}"
)

c5.metric(
    "🚨 Batch Anomalies",
    f"{batch_anomaly_count:,}"
)

c6.metric(
    "🚨 Streaming Anomalies",
    f"{streaming_anomaly_count:,}"
)


st.divider()


# ============================================================
# TABS
# ============================================================

(
    live_tab,
    historical_tab,
    anomaly_tab,
    baseline_tab,
    pipeline_tab
) = st.tabs(
    [
        "📡 Live Streaming",
        "📊 Historical / Batch",
        "🚨 Anomaly Comparison",
        "🧠 Vessel Baseline",
        "⚙️ Pipeline"
    ]
)


# ============================================================
# LIVE STREAMING
# ============================================================

with live_tab:

    st.header(
        "📡 Real-Time AIS Streaming"
    )

    st.caption(
        "Recent streaming events received through "
        "Kafka and processed by Spark Streaming."
    )

    if streaming_df.empty:

        st.warning(
            "No streaming data available."
        )

    else:

        # ----------------------------------------------------
        # Streaming metrics
        # ----------------------------------------------------

        s1, s2, s3 = st.columns(3)

        s1.metric(
            "Events Loaded",
            f"{len(streaming_df):,}"
        )

        if "mmsi" in streaming_df.columns:

            s2.metric(
                "Unique Vessels",
                f"{streaming_df['mmsi'].nunique():,}"
            )

        if "sog" in streaming_df.columns:

            streaming_df["sog"] = pd.to_numeric(
                streaming_df["sog"],
                errors="coerce"
            )

            avg_speed = (
                streaming_df["sog"].mean()
            )

        else:

            avg_speed = 0

        s3.metric(
            "⚡ Average Speed",
            f"{avg_speed:.2f} knots"
        )

        st.divider()

        # ----------------------------------------------------
        # Map
        # ----------------------------------------------------

        st.subheader(
            "🗺️ Live Vessel Positions"
        )

        if {
            "latitude",
            "longitude"
        }.issubset(
            streaming_df.columns
        ):

            map_df = streaming_df[
                [
                    "latitude",
                    "longitude"
                ]
            ].copy()

            map_df["latitude"] = pd.to_numeric(
                map_df["latitude"],
                errors="coerce"
            )

            map_df["longitude"] = pd.to_numeric(
                map_df["longitude"],
                errors="coerce"
            )

            map_df = map_df.dropna()

            if not map_df.empty:

                st.map(
                    map_df,
                    latitude="latitude",
                    longitude="longitude",
                    zoom=2
                )

        # ----------------------------------------------------
        # Speed chart
        # ----------------------------------------------------

        st.subheader(
            "⚡ Vessel Speed"
        )

        if {
            "mmsi",
            "sog"
        }.issubset(
            streaming_df.columns
        ):

            speed_df = (
                streaming_df
                .groupby("mmsi")["sog"]
                .mean()
                .sort_values(
                    ascending=False
                )
                .head(15)
            )

            st.bar_chart(
                speed_df
            )

        # ----------------------------------------------------
        # Latest events
        # ----------------------------------------------------

        st.subheader(
            "📋 Latest AIS Events"
        )

        st.dataframe(
            streaming_df.tail(50).iloc[::-1],
            use_container_width=True,
            height=350
        )


# ============================================================
# HISTORICAL / BATCH
# ============================================================

with historical_tab:

    st.header(
        "📊 Historical / Batch Analytics"
    )

    st.caption(
        "Historical AIS data processed using Apache Spark Batch."
    )

    if historical_df.empty:

        st.warning(
            "No historical data available."
        )

    else:

        h1, h2, h3 = st.columns(3)

        h1.metric(
            "Records Loaded",
            f"{len(historical_df):,}"
        )

        h2.metric(
            "Unique Vessels",
            f"{historical_vessels:,}"
        )

        if "sog" in historical_df.columns:

            historical_df["sog"] = pd.to_numeric(
                historical_df["sog"],
                errors="coerce"
            )

            historical_avg_speed = (
                historical_df["sog"].mean()
            )

        else:

            historical_avg_speed = 0

        h3.metric(
            "Average Speed",
            f"{historical_avg_speed:.2f} knots"
        )

        st.divider()

        # ----------------------------------------------------
        # Historical speed
        # ----------------------------------------------------

        st.subheader(
            "⚡ Historical Vessel Speed"
        )

        if {
            "mmsi",
            "sog"
        }.issubset(
            historical_df.columns
        ):

            speed = (
                historical_df
                .groupby("mmsi")["sog"]
                .mean()
                .sort_values(
                    ascending=False
                )
                .head(15)
            )

            st.bar_chart(
                speed
            )

        # ----------------------------------------------------
        # Historical records
        # ----------------------------------------------------

        st.subheader(
            "📋 Historical AIS Records"
        )

        st.dataframe(
            historical_df.tail(50).iloc[::-1],
            use_container_width=True,
            height=350
        )


# ============================================================
# ANOMALY COMPARISON
# ============================================================

with anomaly_tab:

    st.header(
        "🚨 Batch vs Streaming Anomaly Detection"
    )

    st.caption(
        "Comparison between anomalies detected from "
        "historical batch processing and real-time streaming."
    )

    # --------------------------------------------------------
    # Main anomaly KPIs
    # --------------------------------------------------------

    a1, a2, a3 = st.columns(3)

    a1.metric(
        "📊 Batch Anomalies",
        f"{batch_anomaly_count:,}"
    )

    a2.metric(
        "📡 Streaming Anomalies",
        f"{streaming_anomaly_count:,}"
    )

    total_anomalies = (
        batch_anomaly_count
        + streaming_anomaly_count
    )

    a3.metric(
        "🚨 Total Detected",
        f"{total_anomalies:,}"
    )

    st.divider()

    # --------------------------------------------------------
    # Comparison chart
    # --------------------------------------------------------

    comparison = pd.DataFrame(
        {
            "Pipeline": [
                "Batch",
                "Streaming"
            ],
            "Anomalies": [
                batch_anomaly_count,
                streaming_anomaly_count
            ]
        }
    )

    st.subheader(
        "📈 Batch vs Streaming"
    )

    st.bar_chart(
        comparison.set_index(
            "Pipeline"
        )
    )

    st.divider()

    # --------------------------------------------------------
    # Anomaly types
    # --------------------------------------------------------

    col_a, col_b = st.columns(2)

    with col_a:

        st.subheader(
            "📊 Batch Anomaly Types"
        )

        if (
            not batch_anomalies_df.empty
            and "anomaly_type"
            in batch_anomalies_df.columns
        ):

            batch_types = (
                batch_anomalies_df[
                    "anomaly_type"
                ]
                .value_counts()
            )

            st.bar_chart(
                batch_types
            )

        else:

            st.info(
                "No batch anomaly types available."
            )

    with col_b:

        st.subheader(
            "📡 Streaming Anomaly Types"
        )

        if (
            not streaming_anomalies_df.empty
            and "anomaly_type"
            in streaming_anomalies_df.columns
        ):

            stream_types = (
                streaming_anomalies_df[
                    "anomaly_type"
                ]
                .value_counts()
            )

            st.bar_chart(
                stream_types
            )

        else:

            st.info(
                "No streaming anomaly types available."
            )

    st.divider()

    # --------------------------------------------------------
    # Batch anomalies table
    # --------------------------------------------------------

    st.subheader(
        "🚨 Recent Batch Anomalies"
    )

    if batch_anomalies_df.empty:

        st.info(
            "No batch anomalies detected."
        )

    else:

        st.dataframe(
            batch_anomalies_df.tail(50).iloc[::-1],
            use_container_width=True,
            height=300
        )

    # --------------------------------------------------------
    # Streaming anomalies table
    # --------------------------------------------------------

    st.subheader(
        "🚨 Recent Streaming Anomalies"
    )

    if streaming_anomalies_df.empty:

        st.info(
            "No streaming anomalies detected."
        )

    else:

        st.dataframe(
            streaming_anomalies_df.tail(50).iloc[::-1],
            use_container_width=True,
            height=300
        )


# ============================================================
# BASELINE
# ============================================================

with baseline_tab:

    st.header(
        "🧠 Historical Vessel Behavioral Baseline"
    )

    st.caption(
        "Baseline generated by Spark Batch from historical AIS behavior."
    )

    if baseline_df.empty:

        st.warning(
            "No baseline available."
        )

    else:

        b1, b2, b3 = st.columns(3)

        b1.metric(
            "Vessels in Baseline",
            f"{len(baseline_df):,}"
        )

        if "avg_speed" in baseline_df.columns:

            baseline_avg = pd.to_numeric(
                baseline_df["avg_speed"],
                errors="coerce"
            ).mean()

        else:

            baseline_avg = 0

        b2.metric(
            "Average Vessel Speed",
            f"{baseline_avg:.2f} knots"
        )

        if "record_count" in baseline_df.columns:

            total_records = pd.to_numeric(
                baseline_df["record_count"],
                errors="coerce"
            ).sum()

        else:

            total_records = 0

        b3.metric(
            "Baseline Records",
            f"{int(total_records):,}"
        )

        st.divider()

        st.subheader(
            "🧠 Vessel Behavioral Baseline"
        )

        st.dataframe(
            baseline_df.head(BASELINE_LIMIT),
            use_container_width=True,
            height=450
        )


# ============================================================
# PIPELINE
# ============================================================

with pipeline_tab:

    st.header(
        "⚙️ Data Engineering Pipeline"
    )

    # --------------------------------------------------------
    # Historical
    # --------------------------------------------------------

    st.subheader(
        "📊 Historical / Batch Pipeline"
    )

    st.code(
        """
Historical AIS
      ↓
HDFS Data Lake
      ↓
Apache Spark Batch
      ↓
Data Cleaning & Validation
      ↓
Feature Engineering
      ↓
Vessel Behavioral Baseline
      ↓
Batch Anomaly Detection
      ↓
HDFS
        """,
        language="text"
    )

    # --------------------------------------------------------
    # Streaming
    # --------------------------------------------------------

    st.subheader(
        "📡 Real-Time Streaming Pipeline"
    )

    st.code(
        """
AISStream
    ↓
Kafka
    ↓
Spark Structured Streaming
    ↓
Validation & Parsing
    ↓
Real-Time Anomaly Detection
    ↓
HDFS
    ↓
Streamlit Dashboard
        """,
        language="text"
    )

    # --------------------------------------------------------
    # Airflow
    # --------------------------------------------------------

    st.subheader(
        "⏱️ Airflow Orchestration"
    )

    st.code(
        """
Airflow
   ↓
Schedule
   ↓
Trigger & Monitor
   ↓
Spark Batch Processing
   ↓
Historical Analytics
        """,
        language="text"
    )

    st.divider()

    p1, p2, p3, p4 = st.columns(4)

    p1.success(
        "HDFS\n\nData Lake"
    )

    p2.success(
        "Apache Spark\n\nBatch + Streaming"
    )

    p3.success(
        "Apache Kafka\n\nReal-Time"
    )

    p4.success(
        "Apache Airflow\n\nOrchestration"
    )

    st.info(
        "The platform combines historical batch analytics "
        "with real-time streaming analytics. Historical "
        "behavior is used as a baseline, while streaming "
        "events are monitored for anomalies."
    )


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.caption(
    "Maritime Intelligence Platform | "
    "Kafka • Spark • HDFS • Airflow • Streamlit"
)