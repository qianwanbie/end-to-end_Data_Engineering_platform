"""Data lake utilities for the AeroSense REST API.
Reads Parquet files directly via pyarrow to avoid PySpark serialization overhead.
"""

import os
import glob
from datetime import datetime, timedelta, timezone

import pandas as pd
import pyarrow.parquet as pq

DATA_LAKE_ROOT = os.environ.get("DATA_LAKE_ROOT", "/tmp/datalake")


def _norm(path):
    return path.replace("\\", "/")


def find_parquet_files(base_dir, sensor_type):
    """Walk Hive-partitioned directory to find parquet files for a sensor type."""
    curated_root = _norm(os.path.join(base_dir, "curated", "domain=iot"))
    sensor_dir = _norm(os.path.join(curated_root, f"sensor_type={sensor_type}"))
    if not os.path.isdir(sensor_dir):
        # Also try Windows path
        sensor_dir_win = sensor_dir.replace("/", "\\")
        if os.path.isdir(sensor_dir_win):
            sensor_dir = sensor_dir_win
        else:
            return []
    pattern = os.path.join(sensor_dir, "**", "*.parquet")
    return sorted(glob.glob(pattern, recursive=True))


def get_daily_stats(sensor_type, days=7):
    """
    Return daily statistics for a given sensor type over the last N days.
    Reads Parquet files directly with pyarrow.
    """
    files = find_parquet_files(DATA_LAKE_ROOT, sensor_type)
    if not files:
        return []

    try:
        table = pq.read_table(files)
    except Exception as exc:
        print(f"[lake_utils] Error reading parquet: {exc}")
        return None

    df = table.to_pandas()
    if df.empty:
        return []

    # Convert event_time from timestamp to datetime
    df["event_time"] = pd.to_datetime(df["event_time"])

    # Filter by sensor_type (belt and suspenders)
    if "sensor_type" in df.columns:
        df = df[df["sensor_type"] == sensor_type]

    if df.empty:
        return []

    # Filter to last N days
    max_ts = df["event_time"].max()
    cutoff = max_ts - timedelta(days=days)
    recent = df[df["event_time"] >= cutoff]

    if recent.empty:
        return []

    recent["date"] = recent["event_time"].dt.date.astype(str)

    # Aggregate per day
    stats = (
        recent.groupby("date")
        .agg(
            mean_value=("value", "mean"),
            min_value=("value", "min"),
            max_value=("value", "max"),
            stddev=("value", "std"),
            observation_count=("value", "count"),
            anomaly_count=("is_anomaly", lambda x: x.sum() if x.dtype == bool else (x.astype(bool).sum())),
        )
        .reset_index()
    )

    stats = stats.round({"mean_value": 2, "min_value": 2, "max_value": 2, "stddev": 2})
    stats["observation_count"] = stats["observation_count"].astype(int)
    stats["anomaly_count"] = stats["anomaly_count"].astype(int)

    return stats.to_dict(orient="records")
