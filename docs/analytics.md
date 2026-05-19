# Analytical Query Results

## Query 1: Top 5 Hours with Highest Anomaly Count

```
+----+-----+---+----+-------------+
|year|month|day|hour|anomaly_count|
+----+-----+---+----+-------------+
|2026|5    |19 |9   |105          |
+----+-----+---+----+-------------+
```

## Query 2: Global Statistics per Sensor Type

```
+-----------+--------+--------+--------+--------+-----------+-------------+----------------+
|sensor_type|mean    |min     |max     |stddev  |total_count|anomaly_count|anomaly_rate_pct|
+-----------+--------+--------+--------+--------+-----------+-------------+----------------+
|humidity   |59.68   |10.15   |102.07  |14.92   |323        |3            |0.93            |
|pressure   |1009.12 |970.15  |1049.11 |12.64   |330        |34           |10.30           |
|temperature|29.01   |-4.79   |64.70   |10.60   |344        |68           |19.77           |
+-----------+--------+--------+--------+--------+-----------+-------------+----------------+
```

**Observations:**
- Temperature has the highest anomaly rate (19.77%) because the producer's 10% baseline plus Spark's independent >35°C detection both contribute.
- Humidity anomalies are rare (0.93%) — values naturally cluster within the normal range.
- Pressure anomaly rate (10.30%) reflects both <990 hPa and >1030 hPa triggers firing.
- Global min/max for each sensor exceed normal ranges, confirming anomaly injection works.

## Query 3: Daily Temperature Evolution

```
+----+-----+---+----------+-------------+-----------------+
|year|month|day|daily_mean|anomaly_count|observation_count|
+----+-----+---+----------+-------------+-----------------+
|2026|5    |19 |29.01     |68           |344              |
+----+-----+---+----------+-------------+-----------------+
```

## Query 4: Partition Pruning Demonstration

```
Full scan:  997 rows / 0.334s
Pruned:     344 rows / 0.166s  (sensor_type=temperature, year=2026, month=5)
Speedup:    2.0x
```

**Analysis:** With the current dataset (~1000 rows), partition pruning yields a 2.0x speedup. As the data lake grows to millions of rows across months, the speedup factor increases proportionally — Spark skips entire directory trees by reading only matching Hive partitions. The benefit scales with data volume.
