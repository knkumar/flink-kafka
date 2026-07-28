#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/lib/workload-ids.sh"

ENGINES=("kafka-streams" "flink")
WORKLOADS=("w1" "w3" "w4")
FAILURES=("jvm_kill" "broker_kill" "node_loss" "kraft_failover" "s3_throttling" "changelog_restore")
TRIALS="${TRIALS:-5}"
RATE=20
EVENTS=2000

echo "Starting all failure tests..."

combinations=()
for engine in "${ENGINES[@]}"; do
    for workload in "${WORKLOADS[@]}"; do
        for failure in "${FAILURES[@]}"; do
            for trial in $(seq 1 "$TRIALS"); do
                combinations+=("$engine $workload $failure $trial")
            done
        done
    done
done

readarray -t shuffled < <(printf "%s\n" "${combinations[@]}" | shuf)

for combo in "${shuffled[@]}"; do
    engine=$(echo "$combo" | awk "{print \$1}")
    workload=$(echo "$combo" | awk "{print \$2}")
    failure=$(echo "$combo" | awk "{print \$3}")
    trial=$(echo "$combo" | awk "{print \$4}")
    
    local_workload_name="$(workload_name_for_id "$workload")"
    engine_name="${engine//-/_}"
    RESULT_DIR="experiments/results/${engine_name}_${local_workload_name}_latency_failure_${failure}_trial${trial}"
    if [[ -f "$RESULT_DIR/verification.json" ]]; then
        echo "Skipping Failure Test: $engine | $workload | $failure | Trial $trial (already completed)"
        continue
    fi
    
    echo "=========================================================="
    echo "Running Failure Test: $engine | $workload | $failure | Trial $trial"
    echo "=========================================================="
    
    # Tear down completely to ensure genuinely independent trials
    for compose_file in $(find experiments -name docker-compose.yml); do
        docker compose -f "$compose_file" down -v --remove-orphans >/dev/null 2>&1 || true
    done

    export TRIAL="$trial"
    ./scripts/run-failure-test.sh "$engine" "$workload" "$failure" "$RATE" "$EVENTS"
    
    echo "Waiting 10 seconds before next test..."
    sleep 10
done

echo "All failure tests completed!"
