"""
========================================================================================
Public Transport Passenger Demand Analytics Using Apache Spark and PySpark
Module: app.py
Description: Interactive Streamlit Web Dashboard for visualizing and exploring
             public transit passenger demand, route efficiency, and peak hours.
========================================================================================
"""

import os
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Import analysis core engine
import analysis

# Set Streamlit Page Configuration
st.set_page_config(
    page_title="Public Transport Demand Analytics | PySpark",
    page_icon="🚌",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for polished college lab presentation
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #1e3a8a 0%, #0284c7 100%);
        padding: 24px;
        border-radius: 12px;
        color: white;
        margin-bottom: 24px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }
    .main-header h1 {
        color: white;
        margin: 0;
        font-size: 2.1rem;
        font-weight: 700;
    }
    .main-header p {
        color: #e0f2fe;
        margin: 6px 0 0 0;
        font-size: 1rem;
    }
    .metric-card {
        background-color: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 16px;
        text-align: center;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    .badge-pyspark {
        display: inline-block;
        background-color: #ea580c;
        color: white;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
    }
    .badge-pandas {
        display: inline-block;
        background-color: #0284c7;
        color: white;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
    }
    .synthetic-disclaimer {
        background-color: #fffbeb;
        border-left: 4px solid #f59e0b;
        padding: 12px 16px;
        margin: 16px 0;
        border-radius: 4px;
        font-size: 0.9rem;
        color: #92400e;
    }
    .footer {
        text-align: center;
        padding: 20px;
        color: #64748b;
        font-size: 0.85rem;
        border-top: 1px solid #e2e8f0;
        margin-top: 40px;
    }
</style>
""", unsafe_allow_html=True)


# --------------------------------------------------------------------------------------
# Sidebar: Configuration, File Upload, and Filters
# --------------------------------------------------------------------------------------
st.sidebar.image("https://img.icons8.com/clouds/200/bus.png", width=90)
st.sidebar.title("Transit Analytics")
st.sidebar.caption("Big Data Analytics Lab Project (PySpark)")

# Engine selection option
engine_choice = st.sidebar.radio(
    "Execution Engine Mode",
    options=["Auto-Detect (PySpark Preferred)", "Force Pandas Compatibility"],
    help="Attempts PySpark SparkSession first. If Java/JVM is missing on Windows, gracefully falls back to Pandas."
)
force_pandas = (engine_choice == "Force Pandas Compatibility")

# Data Source Selection
data_source = st.sidebar.radio(
    "Data Source",
    options=["Bundled Synthetic Dataset", "Upload Custom CSV"],
    help="Use the bundled realistic transit dataset or upload your own CSV."
)

raw_df = None
data_load_error = None

if data_source == "Bundled Synthetic Dataset":
    sample_path = os.path.join(os.path.dirname(__file__), "data", "passenger_data.csv")
    if os.path.exists(sample_path):
        try:
            raw_df = pd.read_csv(sample_path)
        except Exception as e:
            data_load_error = f"Error reading bundled dataset: {e}"
    else:
        data_load_error = f"Sample dataset not found at {sample_path}. Please generate it first."
else:
    uploaded_file = st.sidebar.file_uploader(
        "Upload Transit CSV",
        type=["csv"],
        help="Upload a CSV with Date, Route, TimeSlot, Passengers, BusesOperating"
    )
    if uploaded_file is not None:
        try:
            raw_df = pd.read_csv(uploaded_file)
            st.sidebar.success(f"Uploaded: {uploaded_file.name} ({len(raw_df)} rows)")
        except Exception as e:
            data_load_error = f"Failed to parse uploaded CSV: {e}"
    else:
        st.info("👈 Please upload a CSV file or switch to 'Bundled Synthetic Dataset' in the sidebar.")

# --------------------------------------------------------------------------------------
# Top Banner / Header
# --------------------------------------------------------------------------------------
st.markdown("""
<div class="main-header">
    <h1>Public Transport Passenger Demand Analytics</h1>
    <p>Large-scale Urban Transit Load Processing & Fleet Allocation Insights using Apache Spark & PySpark</p>
</div>
""", unsafe_allow_html=True)

# Synthetic data disclaimer
st.markdown("""
<div class="synthetic-disclaimer">
    <strong>⚠️ Educational & Academic Disclaimer:</strong> The bundled dataset is synthetically generated for academic demonstration and laboratory evaluation purposes. It must not be cited or represented as official public transportation statistics.
</div>
""", unsafe_allow_html=True)

if data_load_error:
    st.error(data_load_error)
    st.stop()

if raw_df is None:
    st.stop()

# --------------------------------------------------------------------------------------
# Dataset Validation Step
# --------------------------------------------------------------------------------------
is_valid, validation_errors, val_summary = analysis.validate_dataset_schema(raw_df)

with st.expander("🔍 Dataset Health & Schema Validation Report", expanded=(not is_valid)):
    col_v1, col_v2, col_v3 = st.columns(3)
    col_v1.metric("Total Raw Records", f"{val_summary['total_rows']:,}")
    col_v2.metric("Total Columns", val_summary['total_columns'])
    col_v3.metric("Required Columns Status", "✅ All Present" if not val_summary['missing_columns'] else "❌ Missing Columns")

    if not is_valid:
        st.error("### Schema Validation Errors:")
        for err in validation_errors:
            st.markdown(f"- ❌ {err}")
        st.warning("Please correct the CSV schema to match: `Date`, `Route`, `TimeSlot`, `Passengers`, `BusesOperating`.")
        st.stop()
    else:
        st.success("✅ Dataset schema verified! All required transit analytics columns are valid.")
        if val_summary["missing_values"]:
            st.warning(f"Note: Some columns have missing values: {val_summary['missing_values']}. Cleaned automatically.")

# --------------------------------------------------------------------------------------
# Sidebar Filtering (Route and Date range)
# --------------------------------------------------------------------------------------
st.sidebar.markdown("---")
st.sidebar.subheader("Filter Transit Data")

# Clean data first for filtering
df_clean = analysis.clean_dataset(raw_df)

# Route Filter
all_routes = sorted(df_clean["Route"].unique().tolist())
select_all_routes = st.sidebar.checkbox("Select All Routes", value=True)
if select_all_routes:
    selected_routes = all_routes
else:
    selected_routes = st.sidebar.multiselect(
        "Select Specific Routes",
        options=all_routes,
        default=[all_routes[0]] if all_routes else []
    )

if not selected_routes:
    st.warning("Please select at least one route in the sidebar filter.")
    st.stop()

# Date Range Filter
min_date_str = df_clean["Date"].min()
max_date_str = df_clean["Date"].max()

try:
    min_date = datetime.strptime(min_date_str, "%Y-%m-%d").date()
    max_date = datetime.strptime(max_date_str, "%Y-%m-%d").date()
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = min_date, max_date
except Exception:
    start_date, end_date = None, None

# Apply Filters
filtered_df = df_clean[df_clean["Route"].isin(selected_routes)]
if start_date and end_date:
    filtered_df = filtered_df[
        (filtered_df["Date"] >= start_date.strftime("%Y-%m-%d")) &
        (filtered_df["Date"] <= end_date.strftime("%Y-%m-%d"))
    ]

if filtered_df.empty:
    st.warning("No records found for the selected filter combination. Please broaden your selection.")
    st.stop()

# --------------------------------------------------------------------------------------
# Run Big Data Analytics Pipeline
# --------------------------------------------------------------------------------------
with st.spinner("Executing PySpark distributed aggregations..."):
    results = analysis.analyze_transport_demand(filtered_df, force_pandas=force_pandas)

engine_used = results["engine"]
kpis = results["kpis"]

# Engine Badge & Status Banner
col_status1, col_status2 = st.columns([3, 1])
with col_status1:
    if "Spark" in engine_used:
        st.markdown(f'<span class="badge-pyspark">⚡ Active Engine: {engine_used} (v{results.get("spark_version")})</span>', unsafe_allow_html=True)
        st.caption("Running distributed DataFrame aggregations using Apache Spark local cluster.")
    else:
        st.markdown(f'<span class="badge-pandas">⚙️ Active Engine: {engine_used}</span>', unsafe_allow_html=True)
        fallback_msg = results.get("fallback_reason", "")
        if fallback_msg:
            st.caption(f"Note: {fallback_msg}")

with col_status2:
    st.caption(f"Filtered Records: **{kpis['total_records']:,}** / {len(raw_df):,}")

st.markdown("<br>", unsafe_allow_html=True)

# --------------------------------------------------------------------------------------
# Section 1: Top KPI Cards
# --------------------------------------------------------------------------------------
kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

kpi1.metric(
    label="Total Passengers",
    value=f"{kpis['total_passengers']:,}",
    help="Cumulative ridership across all selected routes and time slots."
)

kpi2.metric(
    label="Buses Dispatched",
    value=f"{kpis['total_buses']:,}",
    help="Total number of operating bus trips scheduled."
)

kpi3.metric(
    label="Avg Load / Bus",
    value=f"{kpis['overall_load_per_bus']}",
    help="Average number of passengers served per operating bus."
)

kpi4.metric(
    label="Highest Demand Route",
    value=kpis['top_route'].split(" - ")[0] if " - " in kpis['top_route'] else kpis['top_route'],
    delta=f"{kpis['top_route_volume']:,} riders",
    help=kpis['top_route']
)

kpi5.metric(
    label="Peak Time Slot",
    value=kpis['peak_timeslot'].split(" ")[0] if " " in kpis['peak_timeslot'] else kpis['peak_timeslot'],
    delta=f"{kpis['peak_timeslot_volume']:,} riders",
    help=kpis['peak_timeslot']
)

st.markdown("---")

# --------------------------------------------------------------------------------------
# Section 2: Visual Analytics Tabs
# --------------------------------------------------------------------------------------
tab_routes, tab_timeslot, tab_trends, tab_pivot, tab_data, tab_hdfs = st.tabs([
    "🚌 Route Analytics",
    "⏰ Peak Time Slots",
    "📈 Daily Trends",
    "🧩 Route × Time Matrix",
    "📋 Data Explorer & Export",
    "🐘 Hadoop HDFS Module"
])

# ------------------------- TAB 1: Route Analytics -------------------------
with tab_routes:
    st.subheader("Passenger Demand & Fleet Allocation by Route")
    route_df = results["route_summary"]

    col_r1, col_r2 = st.columns([3, 2])

    with col_r1:
        # Plotly Bar Chart: Total Passenger Demand by Route
        fig_route = px.bar(
            route_df,
            x="TotalPassengers",
            y="Route",
            orientation="h",
            text="TotalPassengers",
            title="Total Passenger Demand by Route",
            labels={"TotalPassengers": "Cumulative Passengers", "Route": "Transit Route"},
            color="TotalPassengers",
            color_continuous_scale="Viridis"
        )
        fig_route.update_layout(yaxis=dict(autorange="reversed"), height=380, margin=dict(l=20, r=20, t=40, b=20))
        fig_route.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
        st.plotly_chart(fig_route, use_container_width=True)

    with col_r2:
        # Plotly Bar Chart: Average Passengers per Bus (Efficiency)
        fig_eff = px.bar(
            route_df,
            x="Route",
            y="AvgPassengersPerBus",
            title="Fleet Efficiency (Passengers per Bus)",
            labels={"AvgPassengersPerBus": "Avg Load / Bus", "Route": "Route"},
            color="AvgPassengersPerBus",
            color_continuous_scale="Blues"
        )
        fig_eff.update_layout(height=380, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig_eff, use_container_width=True)

    st.markdown("#### Route Aggregations Table (Computed via Spark `groupBy('Route').agg(...)`)")
    st.dataframe(
        route_df.style.format({
            "TotalPassengers": "{:,}",
            "AvgPassengersPerSlot": "{:.1f}",
            "PeakPassengerLoad": "{:,}",
            "TotalBusesOperating": "{:,}",
            "AvgPassengersPerBus": "{:.1f}"
        }),
        use_container_width=True
    )


# ------------------------- TAB 2: Peak Time Slots -------------------------
with tab_timeslot:
    st.subheader("Time Slot Demand & Commuter Peak Hour Analysis")
    ts_df = results["timeslot_summary"]

    col_t1, col_t2 = st.columns([3, 2])

    with col_t1:
        # Time slot demand bar chart
        fig_ts = px.bar(
            ts_df,
            x="TimeSlot",
            y="TotalPassengers",
            title="Aggregate Ridership Across Time Slots",
            labels={"TotalPassengers": "Total Passengers", "TimeSlot": "Time Slot"},
            color="TotalPassengers",
            color_continuous_scale="Turbo",
            text="TotalPassengers"
        )
        fig_ts.update_layout(height=380, margin=dict(l=20, r=20, t=40, b=20))
        fig_ts.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
        st.plotly_chart(fig_ts, use_container_width=True)

    with col_t2:
        # Donut Chart for Share of Commuters by Slot
        fig_pie = px.pie(
            ts_df,
            names="TimeSlot",
            values="TotalPassengers",
            title="Commuter Share Distribution",
            hole=0.45
        )
        fig_pie.update_layout(height=380, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig_pie, use_container_width=True)

    # Matplotlib Chart alternative for college lab viva demonstration
    with st.expander("📊 View Matplotlib Static Chart (Standard Lab Submission Format)"):
        fig_mpl, ax = plt.subplots(figsize=(10, 4))
        ax.bar(ts_df["TimeSlot"], ts_df["TotalPassengers"] / 1000, color="#0284c7", edgecolor="black")
        ax.set_ylabel("Passengers (in Thousands)")
        ax.set_title("Peak-Demand Time Slots (Matplotlib Plot)")
        plt.xticks(rotation=25, ha="right")
        plt.tight_layout()
        st.pyplot(fig_mpl)
        plt.close(fig_mpl)


# ------------------------- TAB 3: Daily Trends -------------------------
with tab_trends:
    st.subheader("Daily Passenger Volume & Bus Deployment Trends")
    daily_df = results["daily_summary"]

    # Line Chart with 2 traces: Passengers and Buses
    fig_daily = go.Figure()
    fig_daily.add_trace(go.Scatter(
        x=daily_df["Date"],
        y=daily_df["DailyPassengers"],
        mode="lines+markers",
        name="Daily Passengers",
        line=dict(color="#2563eb", width=3)
    ))
    fig_daily.add_trace(go.Scatter(
        x=daily_df["Date"],
        y=daily_df["DailyBuses"] * 35,  # scaled for dual comparison
        mode="lines",
        name="Bus Capacity (Buses × 35 cap)",
        line=dict(color="#10b981", width=2, dash="dash")
    ))
    fig_daily.update_layout(
        title="Ridership vs Available Fleet Capacity Over Time",
        xaxis_title="Date",
        yaxis_title="Volume / Capacity",
        height=420,
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    st.plotly_chart(fig_daily, use_container_width=True)

    # Crowding distribution
    st.markdown("#### Transit Crowding Level Classification (Spark `withColumn` & `when`)")
    crowd_df = results["crowding_summary"]
    fig_crowd = px.bar(
        crowd_df,
        x="CrowdingLevel",
        y="Frequency",
        color="CrowdingLevel",
        title="Distribution of Trip Crowding Levels",
        color_discrete_map={
            "High Crowding (Overcrowded)": "#ef4444",
            "Optimal Load": "#22c55e",
            "Low Demand (Underutilized)": "#eab308"
        }
    )
    fig_crowd.update_layout(height=300, margin=dict(l=20, r=20, t=40, b=20))
    st.plotly_chart(fig_crowd, use_container_width=True)


# ------------------------- TAB 4: Route x Time Matrix -------------------------
with tab_pivot:
    st.subheader("Route × Time Slot Cross-Tabulation Matrix")
    st.caption("Generated using Apache Spark DataFrame `groupBy('Route').pivot('TimeSlot').sum('Passengers')`")
    pivot_df = results["pivot_matrix"]

    # Heatmap visualization
    heatmap_data = pivot_df.set_index("Route")
    fig_heat = px.imshow(
        heatmap_data,
        labels=dict(x="Operating Time Slot", y="Transit Route", color="Total Riders"),
        x=heatmap_data.columns,
        y=heatmap_data.index,
        color_continuous_scale="YlGnBu",
        text_auto=True,
        aspect="auto"
    )
    fig_heat.update_layout(height=450, margin=dict(l=20, r=20, t=40, b=20))
    st.plotly_chart(fig_heat, use_container_width=True)

    st.dataframe(pivot_df, use_container_width=True)


# ------------------------- TAB 5: Data Explorer & Export -------------------------
with tab_data:
    st.subheader("Data Explorer & PySpark Transformations Preview")
    sample_df = results["processed_sample"]

    st.write(f"Showing sample of processed transit records with Spark-derived columns (`PassengersPerBus`, `CrowdingLevel`):")
    st.dataframe(sample_df, use_container_width=True)

    # CSV Download Button
    csv_bytes = filtered_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download Filtered Data as CSV",
        data=csv_bytes,
        file_name="filtered_passenger_analytics.csv",
        mime="text/csv"
    )


# ------------------------- TAB 6: Hadoop HDFS Module -------------------------
with tab_hdfs:
    st.subheader("🐘 Hadoop HDFS Integration Architecture (Big Data Lab Module)")
    st.markdown("""
    In enterprise Big Data architectures, raw transit telemetry is staged into **HDFS (Hadoop Distributed File System)**
    and processed in parallel across worker nodes using **Apache Spark**.

    #### HDFS Storage & Ingestion Commands:
    ```bash
    # 1. Start Hadoop HDFS daemons
    start-dfs.cmd        # On Windows (Hadoop sbin)
    # or: start-dfs.sh   # On Linux / Mac

    # 2. Create project directory in HDFS
    hdfs dfs -mkdir -p /bda_project/data

    # 3. Ingest local passenger dataset into HDFS
    hdfs dfs -put data/passenger_data.csv /bda_project/data/

    # 4. Verify file exists in HDFS
    hdfs dfs -ls /bda_project/data/
    ```

    #### PySpark HDFS Code Integration:
    ```python
    from pyspark.sql import SparkSession

    # Connect to HDFS NameNode
    spark = SparkSession.builder \\
        .appName("HDFS-Transit-Analytics") \\
        .master("local[*]") \\
        .getOrCreate()

    # Read distributed dataset directly from HDFS URI
    hdfs_uri = "hdfs://localhost:9000/bda_project/data/passenger_data.csv"
    spark_df = spark.read.csv(hdfs_uri, header=True, inferSchema=True)

    # Save aggregated analytics to HDFS as columnar Parquet
    spark_df.write.mode("overwrite").parquet("hdfs://localhost:9000/bda_project/output/demand_summary.parquet")
    ```
    """)
    st.info("💡 **College Lab Viva Tip:** If asked about scalability, explain how HDFS splits large datasets into 128MB distributed blocks across cluster nodes, and PySpark reads partitions concurrently in memory using Resilient Distributed Datasets (RDDs) and Catalyst Optimizer DataFrames.")


# --------------------------------------------------------------------------------------
# Lab Evaluation & Student Details Footer
# --------------------------------------------------------------------------------------
st.markdown("""
<div class="footer">
    <p><strong>Big Data Analytics (BDA) Laboratory Project | Academic Year 2026-2027</strong></p>
    <p>Developed for Two-Student Lab Evaluation (15 Marks)</p>
    <p>Technologies: Python 3.11 | Apache Spark & PySpark | Streamlit | Pandas | Plotly | Matplotlib</p>
</div>
""", unsafe_allow_html=True)
