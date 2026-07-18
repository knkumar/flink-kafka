#!/usr/bin/env bash
set -euo pipefail

ENGINES=("kafka-streams" "flink")
WORKLOADS=("w1" "w2" "w3" "w4" "w5")
RATE_PER_SEC="${RATE_PER_SEC:-100}"
DURATION_SEC="${DURATION_SEC:-1800}" # 30 minutes
TRIALS="${TRIALS:-5}"

echo "Starting 30-minute stability sweeps..."
echo "Rate: $RATE_PER_SEC events/sec"
echo "Duration per test: $DURATION_SEC seconds (events per test: $(( RATE_PER_SEC * DURATION_SEC )))"

# Generate all combinations
combinations=()
for engine in "${ENGINES[@]}"; do
    for workload in "${WORKLOADS[@]}"; do
        for trial in $(seq 1 "$TRIALS"); do
            combinations+=("$engine $workload $trial")
        done
    done
done

# Shuffle combinations to randomize experiment execution order
readarray -t shuffled < <(printf "%s\n" "${combinations[@]}" | shuf)

for combo in "${shuffled[@]}"; do
    engine=$(echo "$combo" | awk "{print \$1}")
    workload=$(echo "$combo" | awk "{print \$2}")
    trial=$(echo "$combo" | awk "{print \$3}")
    
    echo "=========================================================="
    echo "Running 30m stability sweep for $engine $workload (Trial $trial)"
    echo "=========================================================="
    
    # Tear down completely to ensure genuinely independent trials
    for compose_file in $(find experiments -name docker-compose.yml); do
        docker compose -f "$compose_file" down -v --remove-orphans >/dev/null 2>&1 || true
    done
    
    # Run the stability test wrapper for a SINGLE trial by setting TRIALS=1
    RATE_PER_SEC=$RATE_PER_SEC DURATION_SEC=$DURATION_SEC TRIAL=$trial TRIALS=1 ./scripts/run-stability-tests.sh "$engine" "$workload"
    
    echo "Waiting 10 seconds before next test..."
    sleep 10
done

echo "All 30m stability sweeps completed!"
