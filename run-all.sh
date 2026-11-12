#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"
cargo build --quiet --bins

cleanup() {
    kill $(jobs -p) 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Start the gRPC-only role assignment server.
ROLE_NODES="node-3000,node-3001,node-3002,node-3003,node-3004" \
ROLE_GRPC_PORT=5000 target/debug/assign_role &
sleep 1

# Start 5 database servers on HTTP ports 3000-3004 and gRPC ports 4000-4004
PEERS="127.0.0.1:4000,127.0.0.1:4001,127.0.0.1:4002,127.0.0.1:4003,127.0.0.1:4004"
PEER_INFO="node-3000|127.0.0.1:3000|127.0.0.1:4000,node-3001|127.0.0.1:3001|127.0.0.1:4001,node-3002|127.0.0.1:3002|127.0.0.1:4002,node-3003|127.0.0.1:3003|127.0.0.1:4003,node-3004|127.0.0.1:3004|127.0.0.1:4004"
for port in 3000 3001 3002 3003 3004; do
    PORT="$port" GRPC_PORT=$((port + 1000)) PEERS="$PEERS" PEER_INFO="$PEER_INFO" ROLE_SERVER_ADDR=127.0.0.1:5000 target/debug/db_server &
done

wait
