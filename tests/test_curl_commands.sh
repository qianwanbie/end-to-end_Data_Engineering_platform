#!/usr/bin/env bash
# AeroSense REST API — curl test commands
# Usage: bash tests/test_curl_commands.sh

API="http://localhost:5000"

echo "=========================================="
echo "AeroSense API Test Suite"
echo "=========================================="

# 1. Health check
echo -e "\n--- [1/6] GET /api/v1/health ---"
curl -s -w "\nHTTP Status: %{http_code}\n" "$API/api/v1/health" | python3 -m json.tool 2>/dev/null || echo "(json tool unavailable — raw response above)"

# 2. List sensor types
echo -e "\n--- [2/6] GET /api/v1/sensors ---"
curl -s -w "\nHTTP Status: %{http_code}\n" "$API/api/v1/sensors" | python3 -m json.tool 2>/dev/null || echo "(raw response above)"

# 3. Latest temperature reading
echo -e "\n--- [3/6] GET /api/v1/sensors/temperature/latest ---"
curl -s -w "\nHTTP Status: %{http_code}\n" "$API/api/v1/sensors/temperature/latest" | python3 -m json.tool 2>/dev/null || echo "(raw response above)"

# 4. Temperature stats (7 days)
echo -e "\n--- [4/6] GET /api/v1/sensors/temperature/stats?days=7 ---"
curl -s -w "\nHTTP Status: %{http_code}\n" "$API/api/v1/sensors/temperature/stats?days=7" | python3 -m json.tool 2>/dev/null || echo "(raw response above)"

# 5. List anomalies
echo -e "\n--- [5/6] GET /api/v1/anomalies?sensor=temperature&limit=5 ---"
curl -s -w "\nHTTP Status: %{http_code}\n" "$API/api/v1/anomalies?sensor=temperature&limit=5" | python3 -m json.tool 2>/dev/null || echo "(raw response above)"

# 6. Publish a reading
echo -e "\n--- [6/6] POST /api/v1/readings ---"
curl -s -w "\nHTTP Status: %{http_code}\n" -X POST "$API/api/v1/readings" \
  -H "Content-Type: application/json" \
  -d '{"sensor":"temperature","value":28.3,"unit":"C","timestamp":1737543600000,"source":"test-script","anomaly":false}' \
  | python3 -m json.tool 2>/dev/null || echo "(raw response above)"

# Error cases
echo -e "\n--- Error: Invalid sensor type ---"
curl -s -w "\nHTTP Status: %{http_code}\n" "$API/api/v1/sensors/co2/latest" | python3 -m json.tool 2>/dev/null || echo "(raw response above)"

echo -e "\n--- Error: Invalid days parameter ---"
curl -s -w "\nHTTP Status: %{http_code}\n" "$API/api/v1/sensors/temperature/stats?days=abc" | python3 -m json.tool 2>/dev/null || echo "(raw response above)"

echo -e "\n--- Error: Missing required field ---"
curl -s -w "\nHTTP Status: %{http_code}\n" -X POST "$API/api/v1/readings" \
  -H "Content-Type: application/json" \
  -d '{"sensor":"temperature"}' \
  | python3 -m json.tool 2>/dev/null || echo "(raw response above)"

echo -e "\n--- Error: 404 on unknown route ---"
curl -s -w "\nHTTP Status: %{http_code}\n" "$API/api/v1/nonexistent" | python3 -m json.tool 2>/dev/null || echo "(raw response above)"

echo -e "\n=========================================="
echo "Test suite completed."
echo "=========================================="
