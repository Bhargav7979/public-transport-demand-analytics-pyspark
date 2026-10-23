"""
========================================================================================
Public Transport Passenger Demand Analytics
Module: analysis.py
Description: Big Data Analytics module using Apache Spark and PySpark.
             Performs data extraction, schema validation, transformations, and
             aggregations on passenger transit datasets.
========================================================================================
"""

import os
import sys
import logging
import pandas as pd
from typing import Dict, Any, Tuple, Optional

# Setup logger for analytics diagnostics
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("TransportAnalytics")

# Required columns for schema validation
REQUIRED_COLUMNS = ["Date", "Route", "TimeSlot", "Passengers", "BusesOperating"]

# Global Spark Session reference
_spark_session = None
_spark_init_error = None


def get_spark_session(app_name: str = "PublicTransportDemandAnalytics"):
    """
    Initializes and returns a singleton PySpark SparkSession configured for local execution.
    Handles environment checks and catches JVM initialization errors gracefully.
    """
    global _spark_session, _spark_init_error

    if _spark_session is not None:
        return _spark_session, None

    if _spark_init_error is not None:
        return None, _spark_init_error

    try:
        import pyspark
        from pyspark.sql import SparkSession

        # Suppress verbose Spark logs in terminal
        builder = (
            SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.host", "localhost")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", "1g")
        )

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        _spark_session = spark
        logger.info(f"Apache Spark Session successfully created (Spark version {spark.version})")
        return _spark_session, None

    except Exception as e:
        _spark_init_error = str(e)
        logger.warning(f"Could not initialize PySpark SparkSession: {e}")
        return None, _spark_init_error


def validate_dataset_schema(df: pd.DataFrame) -> Tuple[bool, list, Dict[str, Any]]:
    """
    Validates uploaded or loaded dataset for required columns, data types, and missing values.

    Returns:
        (is_valid, list_of_errors, validation_summary)
    """
    errors = []
    summary = {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "missing_values": {},
        "missing_columns": [],
        "sample_preview": None
    }

    # 1. Check for required columns
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        errors.append(f"Missing required column(s): {', '.join(missing_cols)}")
        summary["missing_columns"] = missing_cols

    # 2. Check for missing values / NaNs
    for col in df.columns:
        null_count = int(df[col].isnull().sum())
        if null_count > 0:
            summary["missing_values"][col] = null_count
            if col in REQUIRED_COLUMNS:
                errors.append(f"Column '{col}' has {null_count} missing (NaN/null) values.")

    # 3. Check numeric constraints if columns exist
    if "Passengers" in df.columns:
        non_numeric = pd.to_numeric(df["Passengers"], errors='coerce').isnull().sum()
        if non_numeric > 0:
            errors.append(f"'Passengers' column contains {non_numeric} non-numeric values.")

    if "BusesOperating" in df.columns:
        non_numeric = pd.to_numeric(df["BusesOperating"], errors='coerce').isnull().sum()
        if non_numeric > 0:
            errors.append(f"'BusesOperating' column contains {non_numeric} non-numeric values.")

    is_valid = len(errors) == 0
    return is_valid, errors, summary


def clean_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans raw dataframe: coerces types, drops na rows for required fields, sorts by date.
    """
    cleaned = df.copy()
    cleaned = cleaned.dropna(subset=[col for col in REQUIRED_COLUMNS if col in cleaned.columns])
    if "Passengers" in cleaned.columns:
        cleaned["Passengers"] = pd.to_numeric(cleaned["Passengers"], errors="coerce").fillna(0).astype(int)
    if "BusesOperating" in cleaned.columns:
        cleaned["BusesOperating"] = pd.to_numeric(cleaned["BusesOperating"], errors="coerce").fillna(1).astype(int)
        # Ensure buses operating is at least 1 to avoid division by zero
        cleaned["BusesOperating"] = cleaned["BusesOperating"].apply(lambda x: max(1, x))
    if "Date" in cleaned.columns:
        cleaned["Date"] = cleaned["Date"].astype(str)

    return cleaned


# ======================================================================================
# PySpark Analytics Implementation
# ======================================================================================

def run_pyspark_analytics(spark, df_pandas: pd.DataFrame) -> Dict[str, Any]:
    """
    Executes actual Apache Spark transformations and actions on the PySpark DataFrame.
    Calculates:
      1. Overall KPIs (Total Passengers, Dispatched Buses, Avg Load/Bus, etc.)
      2. Route-wise demand and efficiency aggregations
      3. Time-slot peak demand analysis
      4. Daily demand and bus dispatch trends
      5. Congestion/crowding classification via withColumn and when()
      6. Route x TimeSlot pivot matrix
    """
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    from pyspark.sql import functions as F

    # 1. Define explicit PySpark schema for optimal performance
    schema = StructType([
        StructField("Date", StringType(), False),
        StructField("Route", StringType(), False),
        StructField("TimeSlot", StringType(), False),
        StructField("Passengers", IntegerType(), False),
        StructField("BusesOperating", IntegerType(), False)
    ])

    # Convert Pandas DataFrame into PySpark DataFrame
    spark_df = spark.createDataFrame(df_pandas[REQUIRED_COLUMNS], schema=schema)

    # 2. Add derived column for Bus Capacity Utilization / Load (Spark transformation)
    # Load per bus = Passengers / BusesOperating
    transformed_df = spark_df.withColumn(
        "PassengersPerBus",
        F.round(F.col("Passengers") / F.col("BusesOperating"), 1)
    ).withColumn(
        "CrowdingLevel",
        F.when(F.col("PassengersPerBus") >= 55, "High Crowding (Overcrowded)")
        .when(F.col("PassengersPerBus") >= 30, "Optimal Load")
        .otherwise("Low Demand (Underutilized)")
    )

    # 3. Overall KPI Aggregations (Spark Action)
    kpi_row = transformed_df.select(
        F.sum("Passengers").alias("total_passengers"),
        F.sum("BusesOperating").alias("total_buses"),
        F.count("*").alias("total_trips"),
        F.round(F.avg("Passengers"), 1).alias("avg_passengers_per_slot"),
        F.max("Passengers").alias("max_single_slot_passengers"),
        F.round(F.sum("Passengers") / F.sum("BusesOperating"), 1).alias("overall_load_per_bus")
    ).collect()[0]

    kpis = {
        "total_passengers": int(kpi_row["total_passengers"] or 0),
        "total_buses": int(kpi_row["total_buses"] or 0),
        "total_records": int(kpi_row["total_trips"] or 0),
        "avg_passengers_per_slot": float(kpi_row["avg_passengers_per_slot"] or 0.0),
        "max_single_slot_passengers": int(kpi_row["max_single_slot_passengers"] or 0),
        "overall_load_per_bus": float(kpi_row["overall_load_per_bus"] or 0.0),
    }

    # 4. Route-wise Aggregation (Spark groupBy + agg + orderBy)
    route_agg = transformed_df.groupBy("Route").agg(
        F.sum("Passengers").alias("TotalPassengers"),
        F.round(F.avg("Passengers"), 1).alias("AvgPassengersPerSlot"),
        F.max("Passengers").alias("PeakPassengerLoad"),
        F.sum("BusesOperating").alias("TotalBusesOperating"),
        F.round(F.sum("Passengers") / F.sum("BusesOperating"), 1).alias("AvgPassengersPerBus")
    ).orderBy(F.col("TotalPassengers").desc())

    route_summary_df = route_agg.toPandas()

    # Identify Top Route
    kpis["top_route"] = route_summary_df.iloc[0]["Route"] if not route_summary_df.empty else "N/A"
    kpis["top_route_volume"] = int(route_summary_df.iloc[0]["TotalPassengers"]) if not route_summary_df.empty else 0

    # 5. TimeSlot Aggregation (Peak-Demand analysis)
    timeslot_agg = transformed_df.groupBy("TimeSlot").agg(
        F.sum("Passengers").alias("TotalPassengers"),
        F.round(F.avg("Passengers"), 1).alias("AvgPassengers"),
        F.max("Passengers").alias("MaxSlotPassengers"),
        F.sum("BusesOperating").alias("TotalBuses")
    ).orderBy(F.col("TotalPassengers").desc())

    timeslot_summary_df = timeslot_agg.toPandas()

    # Identify Peak TimeSlot
    kpis["peak_timeslot"] = timeslot_summary_df.iloc[0]["TimeSlot"] if not timeslot_summary_df.empty else "N/A"
    kpis["peak_timeslot_volume"] = int(timeslot_summary_df.iloc[0]["TotalPassengers"]) if not timeslot_summary_df.empty else 0

    # 6. Daily Demand Trends (Spark groupBy Date + orderBy Date ASC)
    daily_agg = transformed_df.groupBy("Date").agg(
        F.sum("Passengers").alias("DailyPassengers"),
        F.sum("BusesOperating").alias("DailyBuses"),
        F.round(F.avg("Passengers"), 1).alias("AvgSlotPassengers")
    ).orderBy(F.col("Date").asc())

    daily_summary_df = daily_agg.toPandas()

    # 7. Crowding Distribution (Spark groupBy CrowdingLevel)
    crowding_agg = transformed_df.groupBy("CrowdingLevel").agg(
        F.count("*").alias("Frequency")
    ).orderBy(F.col("Frequency").desc())

    crowding_summary_df = crowding_agg.toPandas()

    # 8. Route x TimeSlot Pivot Matrix (Spark pivot operation)
    pivot_df = transformed_df.groupBy("Route").pivot("TimeSlot").sum("Passengers").toPandas().fillna(0)

    # 9. Detailed Processed Data sample for viewing
    processed_sample = transformed_df.limit(100).toPandas()

    return {
        "engine": "Apache Spark (PySpark)",
        "spark_version": spark.version,
        "kpis": kpis,
        "route_summary": route_summary_df,
        "timeslot_summary": timeslot_summary_df,
        "daily_summary": daily_summary_df,
        "crowding_summary": crowding_summary_df,
        "pivot_matrix": pivot_df,
        "processed_sample": processed_sample
    }


# ======================================================================================
# Pandas Compatibility Analytics (Seamless fallback if Spark JVM is absent)
# ======================================================================================

def run_pandas_analytics(df: pd.DataFrame, fallback_reason: str = "") -> Dict[str, Any]:
    """
    Executes identical transit analytics logic using Pandas.
    Serves as an automatic fail-safe when running in environments where Java/PySpark JVM
    is not installed or misconfigured, preserving 100% dashboard functionality.
    """
    df_clean = df[REQUIRED_COLUMNS].copy()

    # Derived calculations
    df_clean["PassengersPerBus"] = (df_clean["Passengers"] / df_clean["BusesOperating"]).round(1)

    def classify_crowding(load):
        if load >= 55:
            return "High Crowding (Overcrowded)"
        elif load >= 30:
            return "Optimal Load"
        else:
            return "Low Demand (Underutilized)"

    df_clean["CrowdingLevel"] = df_clean["PassengersPerBus"].apply(classify_crowding)

    # KPIs
    tot_pass = int(df_clean["Passengers"].sum())
    tot_buses = int(df_clean["BusesOperating"].sum())
    kpis = {
        "total_passengers": tot_pass,
        "total_buses": tot_buses,
        "total_records": len(df_clean),
        "avg_passengers_per_slot": round(float(df_clean["Passengers"].mean()), 1),
        "max_single_slot_passengers": int(df_clean["Passengers"].max()),
        "overall_load_per_bus": round(tot_pass / max(1, tot_buses), 1),
    }

    # Route summary
    route_summary = df_clean.groupby("Route").agg(
        TotalPassengers=("Passengers", "sum"),
        AvgPassengersPerSlot=("Passengers", "mean"),
        PeakPassengerLoad=("Passengers", "max"),
        TotalBusesOperating=("BusesOperating", "sum")
    ).reset_index()
    route_summary["AvgPassengersPerSlot"] = route_summary["AvgPassengersPerSlot"].round(1)
    route_summary["AvgPassengersPerBus"] = (route_summary["TotalPassengers"] / route_summary["TotalBusesOperating"]).round(1)
    route_summary = route_summary.sort_values(by="TotalPassengers", ascending=False).reset_index(drop=True)

    kpis["top_route"] = route_summary.iloc[0]["Route"] if not route_summary.empty else "N/A"
    kpis["top_route_volume"] = int(route_summary.iloc[0]["TotalPassengers"]) if not route_summary.empty else 0

    # TimeSlot summary
    timeslot_summary = df_clean.groupby("TimeSlot").agg(
        TotalPassengers=("Passengers", "sum"),
        AvgPassengers=("Passengers", "mean"),
        MaxSlotPassengers=("Passengers", "max"),
        TotalBuses=("BusesOperating", "sum")
    ).reset_index()
    timeslot_summary["AvgPassengers"] = timeslot_summary["AvgPassengers"].round(1)
    timeslot_summary = timeslot_summary.sort_values(by="TotalPassengers", ascending=False).reset_index(drop=True)

    kpis["peak_timeslot"] = timeslot_summary.iloc[0]["TimeSlot"] if not timeslot_summary.empty else "N/A"
    kpis["peak_timeslot_volume"] = int(timeslot_summary.iloc[0]["TotalPassengers"]) if not timeslot_summary.empty else 0

    # Daily trends
    daily_summary = df_clean.groupby("Date").agg(
        DailyPassengers=("Passengers", "sum"),
        DailyBuses=("BusesOperating", "sum"),
        AvgSlotPassengers=("Passengers", "mean")
    ).reset_index()
    daily_summary["AvgSlotPassengers"] = daily_summary["AvgSlotPassengers"].round(1)
    daily_summary = daily_summary.sort_values(by="Date", ascending=True).reset_index(drop=True)

    # Crowding summary
    crowding_summary = df_clean["CrowdingLevel"].value_counts().reset_index()
    crowding_summary.columns = ["CrowdingLevel", "Frequency"]

    # Pivot matrix
    pivot_matrix = df_clean.pivot_table(index="Route", columns="TimeSlot", values="Passengers", aggfunc="sum", fill_value=0).reset_index()

    return {
        "engine": "Pandas (Compatibility Mode)",
        "spark_version": None,
        "fallback_reason": fallback_reason,
        "kpis": kpis,
        "route_summary": route_summary,
        "timeslot_summary": timeslot_summary,
        "daily_summary": daily_summary,
        "crowding_summary": crowding_summary,
        "pivot_matrix": pivot_matrix,
        "processed_sample": df_clean.head(100)
    }


# ======================================================================================
# Main Analytics Entry Point
# ======================================================================================

def analyze_transport_demand(df: pd.DataFrame, force_pandas: bool = False) -> Dict[str, Any]:
    """
    Main orchestrator for passenger demand analytics.
    Attempts PySpark execution first; falls back gracefully if PySpark/JVM is unavailable.
    """
    cleaned_df = clean_dataset(df)

    if cleaned_df.empty:
        raise ValueError("The provided dataset contains 0 valid rows after cleaning.")

    if not force_pandas:
        spark, error = get_spark_session()
        if spark is not None:
            try:
                logger.info("Executing pipeline with Apache Spark engine...")
                return run_pyspark_analytics(spark, cleaned_df)
            except Exception as e:
                logger.error(f"PySpark transformation failed: {e}. Falling back to Pandas mode.")
                return run_pandas_analytics(cleaned_df, fallback_reason=f"Spark execution runtime error: {e}")
        else:
            logger.info("Spark session unavailable. Executing pipeline in Pandas compatibility mode.")
            return run_pandas_analytics(cleaned_df, fallback_reason=f"Spark JVM unavailable: {error}")
    else:
        return run_pandas_analytics(cleaned_df, fallback_reason="User selected Pandas engine mode.")


# ======================================================================================
# Optional Module: Hadoop HDFS Integration Interface
# ======================================================================================

def export_to_hdfs(spark, df, hdfs_path: str = "hdfs://localhost:9000/bda_project/passenger_data.parquet"):
    """
    [Optional BDA Module]
    Exports processed Spark DataFrame to Hadoop Distributed File System (HDFS).
    Requires a running Hadoop cluster (NameNode & DataNode).

    Usage:
        export_to_hdfs(spark_session, spark_dataframe, 'hdfs://namenode:9000/data/passengers.parquet')
    """
    logger.info(f"Writing DataFrame to HDFS target: {hdfs_path}")
    df.write.mode("overwrite").parquet(hdfs_path)
    logger.info("Export to HDFS completed successfully.")


def read_from_hdfs(spark, hdfs_path: str = "hdfs://localhost:9000/bda_project/passenger_data.parquet"):
    """
    [Optional BDA Module]
    Reads partitioned Parquet dataset from Hadoop Distributed File System (HDFS).

    Usage:
        df = read_from_hdfs(spark_session, 'hdfs://namenode:9000/data/passengers.parquet')
    """
    logger.info(f"Reading dataset from HDFS target: {hdfs_path}")
    return spark.read.parquet(hdfs_path)


if __name__ == "__main__":
    # Test script directly when executed via command line
    print("Testing analysis.py standalone...")
    sample_csv = os.path.join(os.path.dirname(__file__), "data", "passenger_data.csv")
    if os.path.exists(sample_csv):
        raw_df = pd.read_csv(sample_csv)
        valid, errors, summary = validate_dataset_schema(raw_df)
        print(f"Schema valid: {valid}, errors: {errors}")
        results = analyze_transport_demand(raw_df)
        print(f"Engine used: {results['engine']}")
        print(f"Total Passengers: {results['kpis']['total_passengers']:,}")
        print(f"Top Route: {results['kpis']['top_route']} ({results['kpis']['top_route_volume']:,} passengers)")
        print(f"Peak TimeSlot: {results['kpis']['peak_timeslot']}")
        print("Analysis test complete.")
    else:
        print(f"Sample data file not found at {sample_csv}")
