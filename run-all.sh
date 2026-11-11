#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"
cargo build --quiet --bins

cleanup() {
    kill $(jobs -p) 2>/dev/null || true
}
trap cleanup EXIT INT TERM

for port in 3000 3001 3002 3003 3004; do
    PORT="$port" target/debug/db_server &
done

MANAGER_PORT=8080 target/debug/manager &
wait