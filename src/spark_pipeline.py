"""
Spark Structured Streaming pipeline for AeroSense IoT sensor data.

Consumes sensor-events from Kafka, parses JSON, validates values,
detects anomalies, computes 5-minute windowed aggregates, and writes
results to a three-zone data lake (raw / curated / consumption).
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    dayofmonth,
    from_json,
    hour,
    lit,
    max as spark_max,
    min as spark_min,
    month,
    sum as spark_sum,
    window,
    year,
)

# ---------------------------------------------------------------------------
# Configuration — change these constants if needed; no absolute paths
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,localhost:29094,localhost:29096"
)
KAFKA_TOPIC = "sensor-events"
DATA_LAKE_ROOT = os.environ.get("DATA_LAKE_ROOT", "/tmp/datalake")
CHECKPOINT_ROOT = os.environ.get("CHECKPOINT_ROOT", "/tmp/spark-checkpoints")

# Physical plausibility bounds (values outside these are rejected)
PLAUSIBILITY = {
    "temperature": (-10.0, 70.0),
    "humidity": (0.0, 105.0),
    "pressure": (500.0, 1100.0),
}

# JSON schema matching the agreed sensor-event format
SENSOR_SCHEMA = StructType(
    [
        StructField("sensor", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("source", StringType(), True),
        StructField("anomaly", BooleanType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Sink paths (normalise to forward slashes for cross-platform compatibility)
# ---------------------------------------------------------------------------
def _norm(path):
    return path.replace("\\", "/")


def raw_path():
    return _norm(os.path.join(DATA_LAKE_ROOT, "raw", "source=kafka",
                               "topic=sensor-events"))


def curated_path():
    return _norm(os.path.join(DATA_LAKE_ROOT, "curated", "domain=iot"))


def consumption_path():
    return _norm(os.path.join(DATA_LAKE_ROOT, "consumption",
                               "use_case=sensor_averages"))


def checkpoint_path(name):
    return _norm(os.path.join(CHECKPOINT_ROOT, name))


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def build_pipeline():
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
        .appName("AeroSense-Spark-Pipeline")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Read from Kafka --------------------------------------------------
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # --- Parse JSON & enrich with time columns ----------------------------
    parsed = kafka_stream.select(
        col("value").cast("string").alias("raw_json"),
        from_json(col("value").cast("string"), SENSOR_SCHEMA).alias("data"),
        current_timestamp().alias("ingestion_time"),
    ).select(
        "raw_json",
        "data.*",
        "ingestion_time",
    ).withColumn(
        "event_time",
        (col("timestamp") / 1000).cast("timestamp"),
    )

    # --- Validation: filter implausible values ----------------------------
    validated = parsed.filter(
        ((col("sensor") == "temperature")
         & col("value").between(PLAUSIBILITY["temperature"][0],
                                PLAUSIBILITY["temperature"][1]))
        | ((col("sensor") == "humidity")
           & col("value").between(PLAUSIBILITY["humidity"][0],
                                  PLAUSIBILITY["humidity"][1]))
        | ((col("sensor") == "pressure")
           & col("value").between(PLAUSIBILITY["pressure"][0],
                                  PLAUSIBILITY["pressure"][1]))
    )

    # --- Anomaly detection (independent of producer's self-declared flag) --
    cleaned = validated.withColumn(
        "is_anomaly",
        ((col("sensor") == "temperature") & (col("value") > 35.0))
        | ((col("sensor") == "humidity") & (col("value") > 90.0))
        | ((col("sensor") == "pressure")
           & ((col("value") < 990.0) | (col("value") > 1030.0))),
    )

    # --- Partition columns -------------------------------------------------
    ready = (
        cleaned.withColumn("sensor_type", col("sensor"))
        .withColumn("evt_year", year(col("event_time")))
        .withColumn("evt_month", month(col("event_time")))
        .withColumn("evt_day", dayofmonth(col("event_time")))
        .withColumn("ing_year", year(col("ingestion_time")))
        .withColumn("ing_month", month(col("ingestion_time")))
        .withColumn("ing_day", dayofmonth(col("ingestion_time")))
        .withColumn("ing_hour", hour(col("ingestion_time")))
    )

    # --- Sink 1: Raw zone (ingestion-time partition: year/month/day/hour) ---
    raw_cols = [
        "raw_json", "sensor", "value", "unit", "timestamp", "source",
        "is_anomaly", "event_time", "ingestion_time",
        "ing_year", "ing_month", "ing_day", "ing_hour",
    ]
    raw_write = (
        ready.select(*raw_cols)
        .withColumnRenamed("ing_year", "year")
        .withColumnRenamed("ing_month", "month")
        .withColumnRenamed("ing_day", "day")
        .withColumnRenamed("ing_hour", "hour")
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", raw_path())
        .option("checkpointLocation", checkpoint_path("raw"))
        .partitionBy("year", "month", "day", "hour")
        .queryName("raw-sink")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # --- Sink 2: Curated zone (event-time partition: sensor_type/year/month/day) ---
    curated_cols = [
        "sensor_type", "value", "unit", "timestamp", "source",
        "is_anomaly", "event_time", "evt_year", "evt_month", "evt_day",
    ]
    curated_write = (
        ready.select(*curated_cols)
        .withColumnRenamed("evt_year", "year")
        .withColumnRenamed("evt_month", "month")
        .withColumnRenamed("evt_day", "day")
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", curated_path())
        .option("checkpointLocation", checkpoint_path("curated"))
        .option("compression", "snappy")
        .partitionBy("sensor_type", "year", "month", "day")
        .queryName("curated-sink")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # --- Sink 3: Consumption zone (5-min windowed aggregates) --------------
    windowed = (
        ready
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("sensor_type"),
        )
        .agg(
            avg("value").alias("mean_value"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
            count("*").alias("observation_count"),
            spark_sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
        )
        .withColumn("year", year(col("window.start")))
        .withColumn("month", month(col("window.start")))
        .select(
            "sensor_type",
            "window.start",
            "window.end",
            "mean_value",
            "min_value",
            "max_value",
            "observation_count",
            "anomaly_count",
            "year",
            "month",
        )
    )

    consumption_write = (
        windowed.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", consumption_path())
        .option("checkpointLocation", checkpoint_path("consumption"))
        .option("compression", "snappy")
        .partitionBy("sensor_type", "year", "month")
        .queryName("consumption-sink")
        .trigger(processingTime="30 seconds")
        .start()
    )

    print("[pipeline] All three sinks started. Waiting for data...", file=sys.stderr)
    print(f"[pipeline] Raw:         {raw_path()}", file=sys.stderr)
    print(f"[pipeline] Curated:     {curated_path()}", file=sys.stderr)
    print(f"[pipeline] Consumption: {consumption_path()}", file=sys.stderr)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    build_pipeline()
