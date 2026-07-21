#!/usr/bin/env bash
set -euo pipefail

export DURATION_SEC=60
export RATE_PER_SEC=100
export EVENTS=$(( DURATION_SEC * RATE_PER_SEC ))
export TRIALS=1

echo "Generating randomized execution plan for tuning..."
PLAN=$(mktemp)

for cp in 1000 5000 10000; do
    for trial in {1..5}; do
        echo "flink $cp $trial" >> "$PLAN"
        echo "kafka-streams $cp $trial" >> "$PLAN"
    done
done

echo "Running randomized tuning jobs..."
shuf "$PLAN" | while read -r engine cp trial; do
    if [ "$engine" = "flink" ]; then
        echo "Flink $cp trial $trial"
        export CHECKPOINT_INTERVAL_MS=$cp
        export RUN_LABEL="tuning-cp-${cp}_trial${trial}"
        ./scripts/run-stability-tests.sh flink w3 >/dev/null 2>&1 || true
    else
        echo "KS $cp trial $trial"
        export COMMIT_INTERVAL_MS=$cp
        export RUN_LABEL="tuning-commit-${cp}_trial${trial}"
        ./scripts/run-stability-tests.sh kafka-streams w3 >/dev/null 2>&1 || true
    fi
done

rm -f "$PLAN"
