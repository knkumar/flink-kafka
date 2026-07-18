#!/usr/bin/env bash
set -euo pipefail

# Ablation script to test Kafka Streams cache enabled vs disabled
# Runs sliding_sum workload with cache enabled (default 10MB) and disabled (0 bytes).

WORKLOAD="sliding_sum"
EVENTS="${EVENTS:-50000}"

echo "=== Running Cache Enabled (10MB) ==="
export WORKLOAD="$WORKLOAD"
export EVENTS="$EVENTS"
export CACHE_MAX_BYTES="10485760"
export RUN_LABEL="cache-enabled"
./scripts/run-kafka-streams-w1.sh

echo "=== Running Cache Disabled (0MB) ==="
export CACHE_MAX_BYTES="0"
export RUN_LABEL="cache-disabled"
./scripts/run-kafka-streams-w1.sh

echo "=== Cache Ablation Tests Complete ==="
