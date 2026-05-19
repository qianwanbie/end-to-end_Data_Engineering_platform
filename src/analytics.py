"""
Spark SQL analytical queries on the AeroSense data lake.

Produces CSV outputs in outputs/analytics/ and prints results to stdout.
Includes a quantified partition-pruning demonstration.
"""

import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    hour,
    max as spark_max,
    min as spark_min,
    stddev,
    sum as spark_sum,
    year,
    month,
    dayofmonth,
)

DATA_LAKE_ROOT = os.environ.get("DATA_LAKE_ROOT", "/tmp/datalake")
OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "outputs", "analytics"
).replace("\\", "/")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def _norm(path):
    return path.replace("\\", "/")


def write_csv(df, name):
    path = _norm(os.path.join(OUTPUT_DIR, name))
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
    print(f"[analytics] Wrote {path}")


def query1_top_anomaly_hours(spark):
    """
    Top 5 hours with the highest number of anomalies, all sensors combined.
    """
    curated = spark.read.parquet(
        _norm(os.path.join(DATA_LAKE_ROOT, "curated", "domain=iot"))
    )
    result = (
        curated
        .filter(col("is_anomaly") == True)
        .groupBy(year("event_time").alias("year"),
                 month("event_time").alias("month"),
                 dayofmonth("event_time").alias("day"),
                 hour("event_time").alias("hour"))
        .agg(count("*").alias("anomaly_count"))
        .orderBy(col("anomaly_count").desc())
        .limit(5)
    )
    result.show(5, truncate=False)
    write_csv(result, "top_anomaly_hours")


def query2_global_stats(spark):
    """
    For each sensor type: global mean, min, max, stddev, anomaly rate %.
    """
    curated = spark.read.parquet(
        _norm(os.path.join(DATA_LAKE_ROOT, "curated", "domain=iot"))
    )
    totals = curated.groupBy("sensor_type").agg(
        count("*").alias("total_count"),
        spark_sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
    )
    stats = curated.groupBy("sensor_type").agg(
        avg("value").alias("mean"),
        spark_min("value").alias("min"),
        spark_max("value").alias("max"),
        stddev("value").alias("stddev"),
    )
    result = (
        totals.join(stats, "sensor_type")
        .withColumn(
            "anomaly_rate_pct",
            (col("anomaly_count") / col("total_count") * 100).cast("decimal(10,2)")
        )
        .select("sensor_type", "mean", "min", "max", "stddev",
                "total_count", "anomaly_count", "anomaly_rate_pct")
        .orderBy("sensor_type")
    )
    result.show(10, truncate=False)
    write_csv(result, "global_stats")


def query3_daily_temperature_evolution(spark):
    """
    Daily evolution of the mean and anomaly count for the temperature sensor.
    """
    curated = spark.read.parquet(
        _norm(os.path.join(DATA_LAKE_ROOT, "curated", "domain=iot"))
    )
    temp_data = curated.filter(col("sensor_type") == "temperature")
    result = (
        temp_data
        .groupBy(
            year("event_time").alias("year"),
            month("event_time").alias("month"),
            dayofmonth("event_time").alias("day"),
        )
        .agg(
            avg("value").alias("daily_mean"),
            spark_sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
            count("*").alias("observation_count"),
        )
        .orderBy("year", "month", "day")
    )
    result.show(30, truncate=False)
    write_csv(result, "daily_temperature_evolution")


def query4_partition_pruning(spark):
    """
    Demonstrate partition pruning: run the same count query with and
    without a filter on partition columns, measure execution times,
    and compute the speedup factor.
    """
    curated_path = _norm(os.path.join(DATA_LAKE_ROOT, "curated", "domain=iot"))

    # Baseline: read everything
    t0 = time.time()
    all_data = spark.read.parquet(curated_path)
    total_count = all_data.count()
    t1 = time.time()
    baseline_time = t1 - t0

    # Partition-pruned: filter on sensor_type, year, month
    t2 = time.time()
    pruned_data = spark.read.parquet(curated_path).filter(
        (col("sensor_type") == "temperature")
        & (col("year") == 2026)
        & (col("month") == 5)
    )
    pruned_count = pruned_data.count()
    t3 = time.time()
    pruned_time = t3 - t2

    speedup = baseline_time / pruned_time if pruned_time > 0 else float("inf")

    print(f"""
=== Partition Pruning Demonstration ===
Full scan:
  Count = {total_count} rows
  Time  = {baseline_time:.3f}s

Partition-filtered scan (sensor_type=temperature, year=2026, month=5):
  Count = {pruned_count} rows
  Time  = {pruned_time:.3f}s

Speedup factor: {speedup:.1f}x
=======================================
""")

    import csv as csv_mod
    result_path = os.path.join(OUTPUT_DIR, "partition_pruning.csv")
    with open(result_path, "w", newline="") as f:
        w = csv_mod.writer(f)
        w.writerow(["scan_type", "row_count", "time_seconds"])
        w.writerow(["full", total_count, f"{baseline_time:.3f}"])
        w.writerow(["pruned", pruned_count, f"{pruned_time:.3f}"])
        w.writerow(["speedup", speedup, ""])
    print(f"[analytics] Wrote {result_path}")


def main():
    # Windows: ensure hadoop.dll is on PATH for Spark native IO
    hadoop_home = os.environ.get("HADOOP_HOME", "C:/hadoop")
    os.environ.setdefault("HADOOP_HOME", hadoop_home)
    hadoop_bin = os.path.join(hadoop_home, "bin")
    if os.path.isdir(hadoop_bin):
        current_path = os.environ.get("PATH", "")
        if hadoop_bin.replace("/", "\\") not in current_path.replace("/", "\\"):
            os.environ["PATH"] = hadoop_bin + ";" + current_path

    spark = (
        SparkSession.builder
        .appName("AeroSense-Analytics")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=== Query 1: Top 5 hours with highest anomaly count ===")
    query1_top_anomaly_hours(spark)

    print("\n=== Query 2: Global statistics per sensor type ===")
    query2_global_stats(spark)

    print("\n=== Query 3: Daily temperature evolution ===")
    query3_daily_temperature_evolution(spark)

    print("\n=== Query 4: Partition pruning demonstration ===")
    query4_partition_pruning(spark)

    print("[analytics] All queries completed.")
    spark.stop()


if __name__ == "__main__":
    main()
