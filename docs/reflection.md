# Reflection Questions

## Q1: Pipeline crash between raw and curated writes

**Impact on data**: Data already written to the raw zone is safe (Parquet files are immutable once written). However, data from the last micro-batch that was in-flight between raw and curated is lost — it was read from Kafka, written to raw, but the curated write and Kafka offset commit didn't happen.

**Checkpoint strategy**: Spark Structured Streaming checkpoints track the Kafka offset up to which data has been successfully processed through the entire sink pipeline. If the pipeline crashes, on restart it resumes from the last committed offset, re-processing (and re-writing) the affected micro-batch to all sinks. This gives at-least-once semantics — the raw zone may see duplicates for the recovered batch, but no data is permanently lost.

To prevent raw-zone duplicates, use `foreachBatch` to write to both raw and curated within the same batch transaction. Combined with idempotent writes (overwrite by partition), this ensures consistency between zones.

## Q2: Bottlenecks at 50,000 msg/s

**First bottleneck — Network & Kafka broker I/O**: At 50k msg/s, each broker handles ~16k writes/s. Disk I/O for replication (3x write amplification) becomes the first bottleneck. Mitigation: increase `num.network.threads` and use dedicated disks per broker.

**Second bottleneck — Spark micro-batch processing**: The default trigger interval may not keep up. Mitigation: increase `spark.sql.shuffle.partitions`, tune `trigger(processingTime)`, and scale Spark executors horizontally.

**Third bottleneck — Small Parquet writes**: Frequent micro-batches produce many small Parquet files, causing metadata overhead. Mitigation: increase batch duration or use periodic compaction.

## Q3: Kafka vs Parquet Data Lake as source of truth

| Aspect | Kafka | Parquet Data Lake |
|--------|-------|-------------------|
| Retention | Time/size-limited | Infinite (cheap storage) |
| Access pattern | Sequential streaming | Random access, columnar reads |
| Query performance | O(n) scan | Partition pruning, predicate pushdown |
| Schema evolution | Difficult (protocol level) | Easy (schema-on-read, merge) |

**Kafka preferred when**: Real-time consumers need sub-second latency, event ordering matters, and retention is short (hours to days).

**Parquet preferred when**: Historical analysis is needed, queries are selective (not full scans), and data must be retained for months or years.

The best architecture uses both: Kafka as the real-time buffer, Parquet as the durable historical store.

## Q4: Broken sensor emitting aberrant values for 2 hours

**Detection**: The Spark pipeline's validation layer catches physically implausible values (outside [-10, 70]°C for temperature, etc.) and rejects them. The anomaly detection rules flag values that are physically possible but statistically extreme (>35°C).

For a broken sensor emitting values within the plausible range but consistently wrong (e.g., 30°C when it's actually 20°C), the windowed statistics layer helps: a sudden mean shift compared to historical baselines can trigger alerts.

**Isolation without deletion**: Add a `data_quality` column to the curated zone with values: `valid`, `outlier`, `suspect`. During the broken period, tag affected records as `suspect`. The consumption zone filters out `suspect` records for reporting but keeps them available for investigation. Partition pruning on the quality flag keeps queries efficient.

## Q5: Adding a new sensor type "co2"

Files to modify:

1. **`src/producer.py`**: Add `"co2"` to `SENSOR_TYPES`, ranges (e.g., 400-2000 ppm), unit `"ppm"`, and anomaly thresholds.

2. **`src/spark_pipeline.py`**: Add `co2` to the `PLAUSIBILITY` bounds and anomaly detection rules in the pipeline.

3. **`src/analytics.py`**: No structural changes needed — queries are generic on `sensor_type`. New data will be included automatically.

4. **`src/api/kafka_utils.py`**: Add `"co2"` to `VALID_SENSOR_TYPES`.

5. **`src/api/app.py`**: Add `"co2": "ppm"` to `VALID_SENSOR_UNITS` and update the sensor list in `list_sensors()`.

6. **`docs/architecture.md`**: Update documentation to reflect the new sensor type.

No changes needed to: `docker-compose.yml`, data lake layout, or API route structure (all are sensor-type-agnostic by design).
