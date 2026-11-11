#!/bin/bash
set -euo pipefail

for port in 3000 3001 3002 3003 3004; do
    echo "Port $port health:"
    curl -s "http://127.0.0.1:$port/health"
    echo
done

echo "Leader values:"
curl -s http://127.0.0.1:3000/values
echo

echo "Leader log:"
curl -s http://127.0.0.1:3000/log
echo

echo "Logs fetched from every server through gRPC:"
curl -s http://127.0.0.1:3000/cluster/logs
echo
