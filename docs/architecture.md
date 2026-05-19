# Architecture

## Pipeline Diagram

```
+-------------------------+
|   Python Generator      |
|   (src/producer.py)     |
+-----------+-------------+
            |
            v
+-------------------------+       +----------------------+
| Kafka Cluster (3 br.)   |<------|    Kafka UI          |
| topic: sensor-events    |       | localhost:8080       |
| 3 partitions, RF=3      |       +----------------------+
+-----------+-------------+
            |
    +-------+--------+
    |                |
    v                v
+-----------------------+    +----------------------+
| Spark Structured Str. |    |   REST API           |
| (src/spark_pipeline)  |    | (src/api/app.py)     |
|                       |    | Flask 3.0            |
| - parse JSON          |    |                      |
| - validate + anomaly  |    | GET /health          |
| - 5-min window avg    |    | GET /sensors         |
| - triple sink         |    | GET /sensors/<t>/    |
+-----------+-----------+    |   latest             |
            |                | GET /sensors/<t>/    |
            v                |   stats?days=N       |
+-----------------------+    | GET /anomalies       |
|     Data Lake         |<---+ POST /readings       |
| /tmp/datalake/        |    +----------------------+
|                       |
| raw/       (JSON)     |
| curated/   (Parquet)  |
| consumption/ (Parquet)|
+-----------------------+
```

## Component Descriptions

### Kafka Cluster
Three brokers in KRaft mode (no ZooKeeper). Topic `sensor-events` is configured with 3 partitions, replication factor 3, and `min.insync.replicas=2`. Key-based partitioning on sensor type guarantees per-type ordering. Kafka UI provides observability at `localhost:8080`.

### Python Producer (`src/producer.py`)
Generates realistic sensor events with configurable count, rate, and source. Uses `acks=all`, `retries=5`, and `max_in_flight_requests_per_connection=1` for reliable delivery. At least 10% of generated values are anomalies for testing the detection pipeline.

### Spark Streaming Pipeline (`src/spark_pipeline.py`)
Spark Structured Streaming job that:
1. Reads from Kafka in streaming mode
2. Parses JSON with an explicit schema (`StructType`)
3. Validates values against physical plausibility ranges (outliers rejected)
4. Detects anomalies independently of the producer flag
5. Computes 5-minute tumbling window aggregates (mean, min, max, count, anomaly count) with 2-minute watermark
6. Writes to three data lake zones (see below)

### Data Lake
Three-zone layout under `/tmp/datalake/`:
- **Raw zone**: Original JSON payloads with ingestion-time partition (`year/month/day/hour`)
- **Curated zone**: Cleaned Parquet (Snappy) with event-time partition (`sensor_type/year/month/day`)
- **Consumption zone**: Windowed aggregates in Parquet, partition by `sensor_type`

### REST API (`src/api/app.py`)
Flask application exposing 6 endpoints. Reads from both the data lake (Spark SQL, for stats) and Kafka (for latest readings and anomalies). Consistent JSON error responses with proper HTTP status codes.

### Analytics (`src/analytics.py`)
Batch Spark SQL queries on the curated zone: top anomaly hours, global stats per sensor, daily temperature evolution, and quantified partition pruning demonstration.
