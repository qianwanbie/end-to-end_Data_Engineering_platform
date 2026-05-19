"""Kafka utilities for the AeroSense REST API."""

import json
import os

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092,localhost:29094,localhost:29096"
)
TOPIC = "sensor-events"
VALID_SENSOR_TYPES = ["temperature", "humidity", "pressure"]


def get_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        max_in_flight_requests_per_connection=1,
        request_timeout_ms=10000,
    )


def get_latest_consumer(sensor_type):
    """Return the latest message for a sensor type by scanning from near the
    end of each partition."""
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
    )
    consumer.poll(timeout_ms=2000)
    partitions = consumer.assignment()
    if not partitions:
        consumer.close()
        return None

    end_offsets = consumer.end_offsets(partitions)
    found = None
    found_ts = 0

    for tp in partitions:
        end_offset = end_offsets.get(tp, 0)
        if end_offset == 0:
            continue
        start_offset = max(0, end_offset - 200)
        consumer.seek(tp, start_offset)

        exhausted = False
        while not exhausted:
            records = consumer.poll(timeout_ms=2000, max_records=200)
            exhausted = True
            for _, msgs in records.items():
                if msgs:
                    exhausted = False
                for msg in msgs:
                    if msg.value and msg.value.get("sensor") == sensor_type:
                        ts = msg.value.get("timestamp", 0)
                        if ts > found_ts:
                            found_ts = ts
                            found = msg.value

    consumer.close()
    return found


def get_anomalies_consumer(sensor_type=None, limit=20):
    """Return a consumer for browsing recent anomalies."""
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=False,
        consumer_timeout_ms=3000,
    )
    consumer.poll(timeout_ms=1000)
    partitions = consumer.assignment()
    if not partitions:
        consumer.close()
        return []

    anomalies = []
    for tp in consumer.end_offsets(partitions):
        end_offset = consumer.end_offsets([tp])[tp]
        if end_offset > 0:
            start_offset = max(0, end_offset - 500)
            consumer.seek(tp, start_offset)
            records = consumer.poll(timeout_ms=3000, max_records=500)
            for _, msgs in records.items():
                for msg in msgs:
                    val = msg.value
                    if val and val.get("anomaly", False):
                        # Filter by sensor_type if specified
                        if sensor_type and val.get("sensor") != sensor_type:
                            continue
                        anomalies.append(val)
    consumer.close()

    anomalies.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
    return anomalies[:limit]


def publish_reading(reading):
    """Publish a single reading to Kafka. Returns (success, error_message)."""
    producer = get_producer()
    try:
        future = producer.send(
            TOPIC,
            key=reading.get("sensor"),
            value=reading,
        )
        future.get(timeout=10)
        return True, None
    except KafkaError as e:
        return False, str(e)
    finally:
        producer.flush()
        producer.close()
