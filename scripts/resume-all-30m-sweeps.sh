#!/usr/bin/env bash
set -euo pipefail

RATE_PER_SEC="${RATE_PER_SEC:-100}"
DURATION_SEC=1800 # 30 minutes

echo "Resuming 30-minute stability sweeps..."
echo "Rate: $RATE_PER_SEC events/sec"
echo "Duration per test: $DURATION_SEC seconds (events per test: $(( RATE_PER_SEC * DURATION_SEC )))"

for engine in "kafka-streams" "flink"; do
    for workload in "w1" "w2" "w3" "w4" "w5"; do
        if [[ "$engine" == "kafka-streams" && ( "$workload" == "w1" || "$workload" == "w2" || "$workload" == "w3" ) ]]; then
            echo "Skipping already completed $engine $workload"
            continue
        fi
        
        echo "=========================================================="
        echo "Running 30m stability sweep for $engine $workload"
        echo "=========================================================="
        
        # Run the stability test wrapper (it uses RATE_PER_SEC and DURATION_SEC)
        RATE_PER_SEC=$RATE_PER_SEC DURATION_SEC=$DURATION_SEC ./scripts/run-stability-tests.sh "$engine" "$workload"
        
        # Give docker some time to tear down properly between full sweeps
        echo "Waiting 10 seconds before next test..."
        sleep 10
    done
done

echo "All 30m stability sweeps completed!"
