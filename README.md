# AeroSense IoT Data Engineering Platform

## 1. Overview

This project implements an end-to-end Data Engineering platform for AeroSense, a company specialising in industrial environmental monitoring. IoT sensors deployed at customer sites continuously measure **temperature**, **relative humidity**, and **atmospheric pressure**. The platform covers the full data lifecycle:

**Generation → Ingestion (Kafka) → Processing (Spark) → Storage (Data Lake) → Exposure (REST API) → Consumption**

### Scope
- Real-time ingestion of sensor events into a fault-tolerant Kafka cluster
- Stream processing with Spark Structured Streaming (parsing, validation, anomaly detection, windowed aggregation)
- Three-zone data lake storage (raw / curated / consumption) in Parquet format
- REST API for programmatic access to statistics, latest readings, and anomalies
- Analytical SQL queries with partition pruning demonstration

### Technologies
| Component | Technology | Version |
|-----------|-----------|---------|
| Message broker | Apache Kafka (Confluent) | 7.5.0 |
| Stream processing | PySpark Structured Streaming | 3.5.x |
| Data lake | Apache Parquet + Snappy | — |
| REST API | Flask | 3.0+ |
| Kafka client | kafka-python-ng | latest |
| Containerisation | Docker / Docker Compose | 20.10+ / v2.0+ |

## 2. Architecture

See [docs/architecture.md](docs/architecture.md) for the full pipeline diagram and component descriptions.

```
Python Producer  →  Kafka (3 brokers, RF=3)  →  Spark Streaming  →  Data Lake
                                                    │
                                              REST API (Flask)
```

### Data Lake Layout
```
/tmp/datalake/
├── raw/       source=kafka/topic=sensor-events/  year=YYYY/month=MM/day=DD/hour=HH/
├── curated/   domain=iot/sensor_type=.../year=YYYY/month=MM/day=DD/
└── consumption/ use_case=sensor_averages/sensor_type=.../year=YYYY/month=MM/
```

## 3. Instructions

### Prerequisites
- Docker 20.10+ with Docker Compose v2.0+
- Python 3.9+
- Apache Spark 3.5.x (with Kafka connector)

### Installation

```bash
# Clone or extract the project
cd Boyuan_Liu_exam

# Install Python dependencies
pip install -r requirements.txt
```

### Step-by-Step Execution

**1. Start the Kafka cluster**
```bash
docker compose up -d
# Wait ~30 seconds for all brokers to be ready
docker ps --filter "name=kafka"
# Kafka UI available at http://localhost:8080
```

**2. Create the topic**
```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 \
  --create --topic sensor-events \
  --partitions 3 --replication-factor 3
```

**3. Verify topic configuration**
```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 \
  --describe --topic sensor-events
```

**4. Run the producer**
```bash
python src/producer.py --count 1000 --rate 100 --source site-A-rack-12
```

**5. Run the Spark streaming pipeline** (in a separate terminal)
```bash
# If spark-submit is available:
#   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 src/spark_pipeline.py
# Otherwise, run directly with Python (the Kafka connector is configured in-code):
python src/spark_pipeline.py
```

**6. Run analytical queries** (after pipeline has processed data)
```bash
python src/analytics.py
```

**7. Start the REST API** (in a separate terminal)
```bash
cd src/api && python app.py
# API available at http://localhost:5000
```

### Test Commands

```bash
# Health check
curl -s http://localhost:5000/api/v1/health | python3 -m json.tool

# List sensors
curl -s http://localhost:5000/api/v1/sensors | python3 -m json.tool

# Latest temperature reading
curl -s http://localhost:5000/api/v1/sensors/temperature/latest | python3 -m json.tool

# Temperature stats (last 7 days)
curl -s "http://localhost:5000/api/v1/sensors/temperature/stats?days=7" | python3 -m json.tool

# Recent anomalies
curl -s "http://localhost:5000/api/v1/anomalies?sensor=temperature&limit=10" | python3 -m json.tool

# Publish a reading
curl -s -X POST http://localhost:5000/api/v1/readings \
  -H "Content-Type: application/json" \
  -d '{"sensor":"temperature","value":25.5,"unit":"C","timestamp":1737543600000,"source":"test","anomaly":false}' \
  | python3 -m json.tool
```

## 4. Technical Choices

### Partitioning strategy for the curated zone
**Choice**: Hive-style partitioning on `sensor_type/year/month/day` based on event time.

**Why**: `sensor_type` is the primary access pattern — queries and the API almost always filter by sensor type. Year/month/day granularity balances partition size (avoiding too many small files) with query selectivity (a day-level filter eliminates ~96% of a monthly partition). Event time (not ingestion time) is used because analytical queries care about when the measurement was taken, not when it arrived in the system.

**Alternatives considered**: hour-level partitioning (too many small files for infrequent sensors); ingestion-time partitioning (misaligned with query patterns).

### Spark Structured Streaming outputMode
**Choice**: `append` for all three sinks.

**Why**: Raw and curated zones only insert new rows — no updates to existing records are needed, making `append` the most efficient mode. For the consumption zone with windowed aggregates, `append` emits each window result once when the watermark passes the window end, ensuring each aggregate is written exactly once. `update` mode would emit intermediate results (increasing write volume), while `complete` would rewrite all windows on every trigger (untenable for a growing dataset).

### Replication factor and min.insync.replicas
**Choice**: `replication-factor=3`, `min.insync.replicas=2`.

**Why**: With 3 brokers, RF=3 means every partition has a copy on each broker. `min.insync.replicas=2` ensures that at least 2 replicas (including the leader) acknowledge each write before the producer considers it successful. This tolerates one broker failure without losing the ability to produce with `acks=all`. Setting it to 1 would risk data loss (leader failure before replication); setting it to 3 would block writes during any single-broker outage (unacceptable for IoT ingestion).

**Alternatives considered**: RF=1 (no fault tolerance — prohibited by the exam); RF=2 with minISR=2 (no tolerance for single failure with `acks=all`).

### event_time vs ingestion_time across zones
**Choice**: `ingestion_time` for the raw zone, `event_time` for curated and consumption zones.

**Why**: The raw zone is an immutable audit trail of what the platform received and when — ingestion time serves that purpose. The curated zone partitions by event time (the sensor's measurement timestamp) because that is what downstream analytics query on. This separation also means that backfills or delayed data (late-arriving messages) are naturally handled: the curated zone places them by event time while the raw zone records when they actually arrived.

**Alternatives considered**: using ingestion time everywhere (breaks time-based queries on measurement data); using event time for raw zone (loses the audit trail property).

### End-to-end delivery semantics
**Choice**: At-least-once.

**Why**: The producer is configured with `acks=all` and `retries=5`, so every message reaches Kafka at least once. Spark Structured Streaming with Kafka source and checkpointing provides at-least-once semantics — on failure, it replays from the last checkpointed offset, potentially re-processing the last micro-batch. Exactly-once would require idempotent writes and transactional Kafka producers, adding significant latency (50-100ms per batch) for a benefit that is marginal for IoT monitoring use cases where occasional duplicates are tolerable and can be de-duplicated at query time via `DISTINCT`.

**Limitations**: Duplicates can occur in the data lake after a pipeline crash. Downstream consumers must handle this with deduplication logic (e.g., using event timestamp + sensor + source as a composite key).

## 5. Results

### Analytical Query Excerpts

**Global statistics (Query 2)** — run `spark-submit src/analytics.py`:

| sensor_type | mean | min | max | stddev | total | anomalies | rate |
|-------------|------|-----|-----|--------|-------|-----------|------|
| humidity | 62.07 | 10.09 | 104.67 | 14.71 | 483 | 15 | 3.11% |
| pressure | 1009.94 | 970.81 | 1049.83 | 14.11 | 460 | 64 | 13.91% |
| temperature | 29.72 | -3.75 | 64.65 | 9.30 | 441 | 68 | 15.42% |

**Partition pruning (Query 4)**: Full scan 1384 rows / 0.179s vs Pruned 441 rows / 0.125s — **1.4x speedup** on a small dataset; grows linearly with data volume.

### Kafka UI Screenshots
Open http://localhost:8080 after `docker compose up -d`. Place screenshots in `outputs/screenshots/`.

### Sample API Responses
```json
// GET /api/v1/health
{"status":"success","data":{"service":"AeroSense Data Platform API","version":"1.0.0"}}

// GET /api/v1/sensors/temperature/latest
{"status":"success","data":{"sensor":"temperature","value":27.76,"unit":"C","timestamp":1779151874074,"source":"site-A-rack-12","anomaly":false}}

// GET /api/v1/anomalies?limit=3
{"status":"success","data":{"count":3,"anomalies":[...]}}
```

## 6. Limitations and Improvements

### Current Limitations
- The window duration (5 minutes) and watermark (2 minutes) are hardcoded — configurable via environment variables would improve flexibility.
- The REST API's `/latest` endpoint polls Kafka directly rather than serving from a materialized view, which adds latency under load.
- No data retention/compaction policy is configured for the data lake — old raw files accumulate indefinitely.

### What I Would Do with Two Extra Days
1. **Add a monitoring dashboard** (Prometheus + Grafana) tracking pipeline lag, anomaly rate trends, and API latency.
2. **Implement exactly-once semantics** using Kafka transactions and delta lake with `_batch_id` deduplication.
3. **Add a notification service** that pushes anomaly alerts to Slack/email via a webhook triggered by the anomaly detection rules.
4. **Data lake compaction**: a scheduled job merging small Parquet files in the consumption zone to improve query performance.
5. **Integration tests**: end-to-end tests using `pytest` that spin up a test Kafka topic, produce known events, run the pipeline, and assert on API responses and lake contents.
