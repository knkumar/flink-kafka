#!/usr/bin/env bash
set -euo pipefail

ENGINES=("kafka-streams" "flink")
WORKLOADS=("w3" "w4")
FAILURES=("jvm_kill" "node_loss")
KEYS_LIST=(100 1000 10000)
TRIALS="${TRIALS:-1}"  # For sweeps, maybe 1 trial per condition is fine unless specified
RATE_PER_SEC=20
EVENTS=5000

echo "Starting state-size sweep..."

combinations=()
for engine in "${ENGINES[@]}"; do
    for workload in "${WORKLOADS[@]}"; do
        for failure in "${FAILURES[@]}"; do
            for keys in "${KEYS_LIST[@]}"; do
                for trial in $(seq 1 "$TRIALS"); do
                    combinations+=("$engine $workload $failure $keys $trial")
                done
            done
        done
    done
done

readarray -t shuffled < <(printf "%s\n" "${combinations[@]}" | shuf)

for combo in "${shuffled[@]}"; do
    engine=$(echo "$combo" | awk '{print $1}')
    workload=$(echo "$combo" | awk '{print $2}')
    failure=$(echo "$combo" | awk '{print $3}')
    keys=$(echo "$combo" | awk '{print $4}')
    trial=$(echo "$combo" | awk '{print $5}')
    
    echo "=========================================================="
    echo "Running State Size Sweep: $engine | $workload | $failure | Keys: $keys | Trial $trial"
    echo "=========================================================="
    
    export RUN_LABEL="statesweep_keys${keys}_failure_${failure}_trial${trial}"
    if [ -d "experiments/results/${engine}_${workload}_latency_${RUN_LABEL}" ]; then
        echo "Skipping already completed run: ${engine}_${workload}_latency_${RUN_LABEL}"
        continue
    fi
    
    # Tear down completely to ensure genuinely independent trials
    for compose_file in $(find experiments -name docker-compose.yml); do
        docker compose -f "$compose_file" down -v --remove-orphans >/dev/null 2>&1 || true
    done

    export TRIAL="$trial"
    export KEYS="$keys"
    export RUN_LABEL="statesweep_keys${keys}_failure_${failure}_trial${trial}"
    ./scripts/run-failure-test.sh "$engine" "$workload" "$failure" "$RATE_PER_SEC" "$EVENTS" || echo "Run failed but continuing sweep..."
    
    echo "Waiting 10 seconds before next test..."
    sleep 10
done

echo "State size sweep completed!"
