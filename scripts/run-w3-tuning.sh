#!/usr/bin/env bash
set -euo pipefail

export DURATION_SEC=60
export RATE_PER_SEC=100
export EVENTS=$(( DURATION_SEC * RATE_PER_SEC ))

echo "Running Flink tuning (1000ms vs 10000ms checkpoint)"
for cp in 1000 10000; do
    export CHECKPOINT_INTERVAL_MS=$cp
    for trial in {1..3}; do
        echo "Flink $cp trial $trial"
        export RUN_LABEL="tuning-cp-${cp}_trial${trial}"
        export TRIALS=1 # prevent inner loop
        ./scripts/run-stability-tests.sh flink w3 >/dev/null 2>&1 || true
    done
done

echo "Running Kafka Streams tuning (1000ms vs 10000ms commit)"
for cp in 1000 10000; do
    export COMMIT_INTERVAL_MS=$cp
    for trial in {1..3}; do
        echo "KS $cp trial $trial"
        export RUN_LABEL="tuning-commit-${cp}_trial${trial}"
        export TRIALS=1 # prevent inner loop
        ./scripts/run-stability-tests.sh kafka-streams w3 >/dev/null 2>&1 || true
    done
done
