#!/usr/bin/env bash
set -euo pipefail

# Ablation script to test Kafka Streams commit intervals
# Runs tumbling_count workload with varied commit intervals (100ms, 1000ms, 10000ms).

WORKLOAD="tumbling_count"
EVENTS="${EVENTS:-50000}"

for interval in 100 1000 10000; do
    echo "=== Running Commit Interval: ${interval}ms ==="
    export WORKLOAD="$WORKLOAD"
    export EVENTS="$EVENTS"
    export COMMIT_INTERVAL_MS="${interval}"
    export RUN_LABEL="commit-${interval}ms"
    ./scripts/run-kafka-streams-w1.sh
done

echo "=== Commit Interval Ablation Tests Complete ==="
