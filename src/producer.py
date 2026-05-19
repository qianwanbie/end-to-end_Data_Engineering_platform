"""
IoT Sensor Event Producer for AeroSense platform.
Generates realistic temperature, humidity, and pressure readings
and publishes them to the sensor-events Kafka topic.
"""

import argparse
import json
import random
import signal
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = ["localhost:29092", "localhost:29094", "localhost:29096"]
TOPIC = "sensor-events"
SENSOR_TYPES = ["temperature", "humidity", "pressure"]

# Realistic ranges per sensor type
RANGES = {
    "temperature": {"min": 15.0, "max": 45.0, "unit": "C"},
    "humidity": {"min": 30.0, "max": 95.0, "unit": "%"},
    "pressure": {"min": 980.0, "max": 1040.0, "unit": "hPa"},
}

# Anomaly thresholds (for self-declared anomaly flag in the producer)
ANOMALY_THRESHOLDS = {
    "temperature": (15.0, 45.0),
    "humidity": (30.0, 95.0),
    "pressure": (990.0, 1030.0),
}

ANOMALY_RATE = 0.10  # At least 10% of messages must be anomalies

running = True


def shutdown(signum, frame):
    global running
    running = False
    print("\n[producer] Graceful shutdown initiated...", file=sys.stderr)


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


# ---------------------------------------------------------------------------
# Value generation
# ---------------------------------------------------------------------------
def normal_value(lo, hi):
    """Return a value drawn from a normal distribution centred in [lo, hi]."""
    mu = (lo + hi) / 2.0
    sigma = (hi - lo) / 6.0
    return max(lo, min(hi, random.gauss(mu, sigma)))


def anomalous_value(lo, hi):
    """Return a value that is clearly outside [lo, hi]."""
    if random.random() < 0.5:
        return lo - random.uniform(5.0, 20.0)
    else:
        return hi + random.uniform(5.0, 20.0)


def generate_event(sensor_type, source):
    """Generate a single sensor event dict."""
    r = RANGES[sensor_type]
    thresholds = ANOMALY_THRESHOLDS[sensor_type]
    is_anomaly = random.random() < ANOMALY_RATE

    if is_anomaly:
        value = round(anomalous_value(thresholds[0], thresholds[1]), 2)
    else:
        value = round(normal_value(r["min"], r["max"]), 2)

    return {
        "sensor": sensor_type,
        "value": value,
        "unit": r["unit"],
        "timestamp": int(time.time() * 1000),
        "source": source,
        "anomaly": is_anomaly,
    }


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------
def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        linger_ms=5,
        batch_size=16384,
    )


def run(count, rate, source):
    global running
    producer = create_producer()
    print(f"[producer] Connected. Sending {count} events at {rate} eps "
          f"from source '{source}'", file=sys.stderr)

    produced = 0
    t_start = time.time()

    try:
        while running and produced < count:
            sensor_type = random.choice(SENSOR_TYPES)
            event = generate_event(sensor_type, source)

            future = producer.send(
                TOPIC,
                key=sensor_type,
                value=event,
            )

            try:
                future.get(timeout=10)
            except KafkaError as exc:
                print(f"[producer] Delivery failed: {exc}", file=sys.stderr)

            produced += 1

            if rate > 0:
                expected_elapsed = produced / rate
                actual_elapsed = time.time() - t_start
                sleep_time = expected_elapsed - actual_elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)

        elapsed = time.time() - t_start
        print(f"[producer] Done. {produced} events in {elapsed:.1f}s "
              f"({produced / elapsed:.1f} eps)", file=sys.stderr)

    finally:
        producer.flush()
        producer.close()
        print("[producer] Flushed and closed.", file=sys.stderr)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="AeroSense IoT sensor event producer"
    )
    parser.add_argument(
        "--count", "-N", type=int, default=1000,
        help="Number of events to produce (default: 1000)"
    )
    parser.add_argument(
        "--rate", "-r", type=float, default=100.0,
        help="Events per second (default: 100)"
    )
    parser.add_argument(
        "--source", "-s", type=str, default="site-A-rack-12",
        help="Source site identifier (default: site-A-rack-12)"
    )
    args = parser.parse_args()

    if args.count <= 0:
        print("Error: --count must be positive", file=sys.stderr)
        sys.exit(1)

    run(args.count, args.rate, args.source)


if __name__ == "__main__":
    main()
