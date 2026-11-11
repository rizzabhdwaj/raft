#!/bin/bash
# curl-tests.sh - Test raft_db manager and backend servers
# Usage: ./curl-tests.sh
# Prerequisite: Start servers with ./run-all.sh

set -euo pipefail

BASE="http://localhost:8080"
BACKEND_BASE="http://127.0.0.1"

echo "========================================="
echo "  raft_db API Curl Test Script"
echo "========================================="
echo ""

# Wait for servers to be ready
echo "Waiting for servers to start..."
for i in {1..10}; do
    if curl -s "${BASE}/health" > /dev/null 2>&1; then
        echo "  Manager server is ready!"
        break
    fi
    if [ "$i" -eq 10 ]; then
        echo "  ERROR: Manager server not responding after 10 attempts"
        exit 1
    fi
    sleep 1
done

echo ""
echo "=== Manager Server Endpoints (port 8080) ==="
echo ""

# Health check - default port
echo "1. GET /health (default port 3000)"
curl -s "${BASE}/health"
echo ""
echo ""

# Health check - specified port
echo "2. GET /health/3001 (specified port)"
curl -s "${BASE}/health/3001"
echo ""
echo ""

# Values - default port
echo "3. GET /values (default port 3000)"
curl -s "${BASE}/values"
echo ""
echo ""

# Values - specified port
echo "4. GET /values/3002 (specified port 3002)"
curl -s "${BASE}/values/3002"
echo ""
echo ""

# Command - default port (POST)
echo "5. POST /command (default port 3000)"
curl -s -X POST "${BASE}/command" \
  -H "Content-Type: application/json" \
  -d '{"Command": "CreateTable", "table_id": 1, "table_name": "test"}'
echo ""
echo ""

# Command - specified port (POST)
echo "6. POST /command/3001 (specified port 3001)"
curl -s -X POST "${BASE}/command/3001" \
  -H "Content-Type: application/json" \
  -d '{"Command": "CreateTable", "table_id": 2, "table_name": "orders"}'
echo ""
echo ""

echo ""
echo "=== Backend Server Endpoints (ports 3000-3004) ==="
echo ""

# Direct backend health checks
for port in 3000 3001 3002 3003 3004; do
    echo "6.p. Direct backend health - port ${port}"
    curl -s "${BACKEND_BASE}:${port}/health"
    echo ""
done

echo ""
echo "=== Direct Database Commands on Port 3000 ==="

# CreateTable
echo "7. POST /command CreateTable on port 3000"
curl -s -X POST "${BACKEND_BASE}:3000/command" \
  -H "Content-Type: application/json" \
  -d '{"Command": "CreateTable", "table_id": 1, "table_name": "users"}'
echo ""

# Insert
echo "8. POST /command Insert on port 3000"
curl -s -X POST "${BACKEND_BASE}:3000/command" \
  -H "Content-Type: application/json" \
  -d '{"Command": "Insert", "table_id": 1, "schema": {"id": 1, "col1": "John", "col2": "Doe", "col3": "john@example.com"}}'
echo ""

# Delete
echo "9. POST /command Delete on port 3000"
curl -s -X POST "${BACKEND_BASE}:3000/command" \
  -H "Content-Type: application/json" \
  -d '{"Command": "Delete", "table_id": 1, "id": 1}'
echo ""

# Get values
echo "10. GET /values on port 3000"
curl -s "${BACKEND_BASE}:3000/values"
echo ""

echo ""
echo "=== Direct Backend Health (all ports) ==="

for port in 3000 3001 3002 3003 3004; do
    echo "Port ${port}: $(curl -s "${BACKEND_BASE}:${port}/health")"
done

echo ""
echo "========================================="
echo "  Test Complete"
echo "========================================="