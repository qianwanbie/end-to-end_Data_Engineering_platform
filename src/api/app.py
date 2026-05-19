"""
Flask REST API for the AeroSense IoT sensor data platform.

Endpoints:
  GET  /api/v1/health
  GET  /api/v1/sensors
  GET  /api/v1/sensors/<type>/latest
  GET  /api/v1/sensors/<type>/stats?days=N
  GET  /api/v1/anomalies?sensor=<type>&limit=N
  POST /api/v1/readings
"""

import time
from flask import Flask, jsonify, request

from kafka_utils import (
    VALID_SENSOR_TYPES,
    get_anomalies_consumer,
    get_latest_consumer,
    publish_reading,
)
from lake_utils import get_daily_stats

app = Flask(__name__)

VALID_SENSOR_UNITS = {
    "temperature": "C",
    "humidity": "%",
    "pressure": "hPa",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def success_response(data, status=200):
    return jsonify({"status": "success", "data": data}), status


def error_response(message, status=400, error_type=None):
    body = {"status": "error", "message": message}
    if error_type:
        body["error_type"] = error_type
    return jsonify(body), status


# ---------------------------------------------------------------------------
# Global error handlers
# ---------------------------------------------------------------------------
@app.errorhandler(404)
def not_found(_):
    return jsonify({"status": "error", "message": "Resource not found"}), 404


@app.errorhandler(405)
def method_not_allowed(_):
    return jsonify({"status": "error", "message": "Method not allowed"}), 405


@app.errorhandler(500)
def internal_error(_):
    return jsonify({"status": "error", "message": "Internal server error"}), 500


# ---------------------------------------------------------------------------
# GET /api/v1/health
# ---------------------------------------------------------------------------
@app.route("/api/v1/health", methods=["GET"])
def health():
    return success_response({
        "service": "AeroSense Data Platform API",
        "version": "1.0.0",
        "timestamp": int(time.time() * 1000),
    })


# ---------------------------------------------------------------------------
# GET /api/v1/sensors
# ---------------------------------------------------------------------------
@app.route("/api/v1/sensors", methods=["GET"])
def list_sensors():
    sensors = [
        {"type": "temperature", "unit": "C", "range": "15 - 45"},
        {"type": "humidity", "unit": "%", "range": "30 - 95"},
        {"type": "pressure", "unit": "hPa", "range": "980 - 1040"},
    ]
    return success_response({"sensors": sensors})


# ---------------------------------------------------------------------------
# GET /api/v1/sensors/<type>/latest
# ---------------------------------------------------------------------------
@app.route("/api/v1/sensors/<sensor_type>/latest", methods=["GET"])
def latest_reading(sensor_type):
    if sensor_type not in VALID_SENSOR_TYPES:
        return error_response(
            f"Invalid sensor type: '{sensor_type}'. "
            f"Allowed: {', '.join(VALID_SENSOR_TYPES)}",
            status=404,
        )

    reading = get_latest_consumer(sensor_type)
    if reading is None:
        return error_response(
            f"No data available for sensor type '{sensor_type}'",
            status=404,
        )

    return success_response(reading)


# ---------------------------------------------------------------------------
# GET /api/v1/sensors/<type>/stats?days=N
# ---------------------------------------------------------------------------
@app.route("/api/v1/sensors/<sensor_type>/stats", methods=["GET"])
def sensor_stats(sensor_type):
    if sensor_type not in VALID_SENSOR_TYPES:
        return error_response(
            f"Invalid sensor type: '{sensor_type}'",
            status=404,
        )

    days_str = request.args.get("days", "7")
    try:
        days = int(days_str)
    except ValueError:
        return error_response(
            f"'days' must be an integer, got '{days_str}'",
            status=400,
        )

    if days < 1 or days > 90:
        return error_response(
            f"'days' must be between 1 and 90, got {days}",
            status=400,
        )

    stats = get_daily_stats(sensor_type, days=days)
    if stats is None:
        return error_response(
            "Data lake not available — run the Spark pipeline first",
            status=500,
        )

    if not stats:
        return error_response(
            f"No statistics available for '{sensor_type}' "
            f"in the last {days} days",
            status=404,
        )

    return success_response({
        "sensor_type": sensor_type,
        "days": days,
        "daily_stats": stats,
    })


# ---------------------------------------------------------------------------
# GET /api/v1/anomalies?sensor=<type>&limit=N
# ---------------------------------------------------------------------------
@app.route("/api/v1/anomalies", methods=["GET"])
def list_anomalies():
    sensor_type = request.args.get("sensor", None)
    if sensor_type and sensor_type not in VALID_SENSOR_TYPES:
        return error_response(
            f"Invalid sensor type: '{sensor_type}'",
            status=400,
        )

    limit_str = request.args.get("limit", "20")
    try:
        limit = int(limit_str)
    except ValueError:
        return error_response(
            f"'limit' must be an integer, got '{limit_str}'",
            status=400,
        )

    if limit < 1 or limit > 200:
        return error_response(
            f"'limit' must be between 1 and 200, got {limit}",
            status=400,
        )

    anomalies = get_anomalies_consumer(sensor_type=sensor_type, limit=limit)
    return success_response({
        "count": len(anomalies),
        "anomalies": anomalies,
    })


# ---------------------------------------------------------------------------
# POST /api/v1/readings
# ---------------------------------------------------------------------------
@app.route("/api/v1/readings", methods=["POST"])
def post_reading():
    body = request.get_json(silent=True)
    if body is None:
        return error_response(
            "Request body must be valid JSON",
            status=400,
            error_type="malformed_request",
        )

    # Validate required fields
    for field in ("sensor", "value", "unit", "timestamp", "source"):
        if field not in body:
            return error_response(
                f"Missing required field: '{field}'",
                status=422,
                error_type="validation_error",
            )

    sensor = body["sensor"]
    if sensor not in VALID_SENSOR_TYPES:
        return error_response(
            f"Invalid sensor type '{sensor}'. "
            f"Allowed: {', '.join(VALID_SENSOR_TYPES)}",
            status=422,
            error_type="validation_error",
        )

    if not isinstance(body["value"], (int, float)):
        return error_response(
            "'value' must be numeric",
            status=422,
            error_type="validation_error",
        )

    if not isinstance(body["timestamp"], int):
        return error_response(
            "'timestamp' must be an integer (epoch ms)",
            status=422,
            error_type="validation_error",
        )

    # Build the reading payload
    reading = {
        "sensor": sensor,
        "value": float(body["value"]),
        "unit": VALID_SENSOR_UNITS.get(sensor, body.get("unit", "")),
        "timestamp": body["timestamp"],
        "source": body["source"],
        "anomaly": body.get("anomaly", False),
    }

    success, err = publish_reading(reading)
    if not success:
        return error_response(
            f"Failed to publish to Kafka: {err}",
            status=500,
        )

    app.logger.info(f"Published reading: {reading}")
    return success_response({
        "message": "Reading published successfully",
        "reading": reading,
    }, status=201)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
