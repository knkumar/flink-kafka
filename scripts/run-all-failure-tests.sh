#!/usr/bin/env bash
set -euo pipefail

ENGINES=("kafka-streams" "flink")
WORKLOADS=("w1" "w3" "w4")
FAILURES=("jvm_kill" "broker_kill" "node_loss" "kraft_failover" "s3_throttling" "changelog_restore")
TRIALS=5
RATE=20
EVENTS=2000

echo "Starting all failure tests..."

for engine in "${ENGINES[@]}"; do
    for workload in "${WORKLOADS[@]}"; do
        for failure in "${FAILURES[@]}"; do
            for trial in $(seq 1 $TRIALS); do
                echo "=========================================================="
                echo "Running Failure Test: $engine | $workload | $failure | Trial $trial"
                echo "=========================================================="
                
                export TRIAL="$trial"
                ./scripts/run-failure-test.sh "$engine" "$workload" "$failure" "$RATE" "$EVENTS"
                
                echo "Waiting 10 seconds before next test..."
                sleep 10
            done
        done
    done
done

echo "All failure tests completed!"
