#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"
cargo build --quiet --bins

cleanup() {
    kill $(jobs -p) 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Start 5 database backend servers on HTTP ports 3000-3004 and gRPC ports 4000-4004
PEERS="127.0.0.1:4000,127.0.0.1:4001,127.0.0.1:4002,127.0.0.1:4003,127.0.0.1:4004"
for port in 3000 3001 3002 3003 3004; do
    PORT="$port" GRPC_PORT=$((port + 1000)) PEERS="$PEERS" target/debug/db_server &
done

# Start manager server with default backend port = 3000
# All /values, /command, /health without {port} will use port 3000 by default
# But can specify different port: /values/3001, /health/3002, etc.
MANAGER_PORT=8080 MANAGER_DEFAULT_PORT=3000 target/debug/manager &

wait
