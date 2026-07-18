#!/usr/bin/env bash
set -euo pipefail

ENGINES=("kafka-streams" "flink")
WORKLOADS=("w1" "w2" "w3" "w4" "w5")
RATE_PER_SEC="${RATE_PER_SEC:-100}"
DURATION_SEC=1800 # 30 minutes

echo "Starting 30-minute stability sweeps..."
echo "Rate: $RATE_PER_SEC events/sec"
echo "Duration per test: $DURATION_SEC seconds (events per test: $(( RATE_PER_SEC * DURATION_SEC )))"

for engine in "${ENGINES[@]}"; do
    for workload in "${WORKLOADS[@]}"; do
        for trial in {1..5}; do
            echo "=========================================================="
            echo "Running 30m stability sweep for $engine $workload (Trial $trial)"
            echo "=========================================================="
            
            # Run the stability test wrapper (it uses RATE_PER_SEC and DURATION_SEC)
            RATE_PER_SEC=$RATE_PER_SEC DURATION_SEC=$DURATION_SEC TRIAL=$trial ./scripts/run-stability-tests.sh "$engine" "$workload"
            
            # Give docker some time to tear down properly between full sweeps
            echo "Waiting 10 seconds before next test..."
            sleep 10
        done
    done
done

echo "All 30m stability sweeps completed!"
