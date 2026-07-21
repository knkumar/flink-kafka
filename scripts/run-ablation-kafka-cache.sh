#!/usr/bin/env bash
set -euo pipefail

# Ablation script to test Kafka Streams cache enabled vs disabled across intervals
# Runs sliding_sum workload with varying cache sizes (0MB and 10MB) and commit intervals.

export WORKLOAD="sliding_sum"
export DURATION_SEC=60
export RATE_PER_SEC=100
export EVENTS=$(( DURATION_SEC * RATE_PER_SEC ))
export TRIALS=1

echo "Generating randomized execution plan for KS cache ablation..."
PLAN=$(mktemp)

for cache in 0 10485760; do
    for cp in 1000 5000 10000; do
        for trial in {1..5}; do
            echo "$cache $cp $trial" >> "$PLAN"
        done
    done
done

echo "Running randomized cache ablation jobs..."
shuf "$PLAN" | while read -r cache cp trial; do
    echo "KS Cache=$cache CP=$cp trial $trial"
    export CACHE_MAX_BYTES=$cache
    export COMMIT_INTERVAL_MS=$cp
    export RUN_LABEL="cache-${cache}-commit-${cp}_trial${trial}"
    ./scripts/run-kafka-streams-w1.sh >/dev/null 2>&1 || true
done

rm -f "$PLAN"
echo "=== Cache Ablation Tests Complete ==="
