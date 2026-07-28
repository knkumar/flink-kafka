#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/lib/workload-ids.sh"

ENGINES=("kafka-streams" "flink")
WORKLOADS=("w1" "w3" "w4")
PARTITIONS_LIST=(1 5 10)
RATES=(100 1000 5000 10000 50000)

echo "Starting Saturation Sweeps..."

combinations=()
for engine in "${ENGINES[@]}"; do
    for workload in "${WORKLOADS[@]}"; do
        for p in "${PARTITIONS_LIST[@]}"; do
            for rate in "${RATES[@]}"; do
                combinations+=("$engine $workload $p $rate")
            done
        done
    done
done

readarray -t shuffled < <(printf "%s\n" "${combinations[@]}" | shuf)

for combo in "${shuffled[@]}"; do
    engine=$(echo "$combo" | awk '{print $1}')
    workload=$(echo "$combo" | awk '{print $2}')
    p=$(echo "$combo" | awk '{print $3}')
    rate=$(echo "$combo" | awk '{print $4}')
    
    echo "=========================================================="
    echo "Running Saturation/Partition Sweep: $engine | $workload | Part: $p | Rate: $rate"
    echo "=========================================================="
    
    export RUN_LABEL="saturation_part${p}_rate${rate}"
    if [ -d "experiments/results/${engine}_${workload}_latency_${RUN_LABEL}" ]; then
        echo "Skipping already completed run: ${engine}_${workload}_latency_${RUN_LABEL}"
        continue
    fi
    
    for compose_file in $(find experiments -name docker-compose.yml); do
        docker compose -f "$compose_file" down -v --remove-orphans >/dev/null 2>&1 || true
    done

    export KAFKA_PARTITIONS="$p"
    export RATE_PER_SEC="$rate"
    # Cap total events so tests don't run forever at high rates
    export EVENTS=$(( rate * 30 )) 
    if [ "$EVENTS" -lt 2000 ]; then
        EVENTS=2000
    fi
    
    export RUN_LABEL="saturation_part${p}_rate${rate}"
    
    export WORKLOAD_ID="${workload}_latency"
    export WORKLOAD="$(workload_name_for_id "$workload")"

    if [ "$engine" == "kafka-streams" ]; then
        ./scripts/run-kafka-streams-w1-latency.sh || echo "Run failed but continuing sweep..."
    else
        ./scripts/run-flink-w1-latency.sh || echo "Run failed but continuing sweep..."
    fi
    
    sleep 5
done

echo "Saturation sweeps completed!"
