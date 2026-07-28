#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/lib/workload-ids.sh"

ENGINES=("kafka-streams" "flink")
WORKLOADS=("w1" "w3")
PARTITIONS_LIST=(5)
RATES=(5000)
SKEWS=("false" "true")
PARALLELISMS=(1 3)

echo "Starting Skew & Parallelism Sweeps..."

combinations=()
for engine in "${ENGINES[@]}"; do
    for workload in "${WORKLOADS[@]}"; do
        for p in "${PARTITIONS_LIST[@]}"; do
            for rate in "${RATES[@]}"; do
                for skew in "${SKEWS[@]}"; do
                    for par in "${PARALLELISMS[@]}"; do
                        combinations+=("$engine $workload $p $rate $skew $par")
                    done
                done
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
    skew=$(echo "$combo" | awk '{print $5}')
    par=$(echo "$combo" | awk '{print $6}')
    
    echo "=========================================================="
    echo "Running Sweep: $engine | $workload | Part: $p | Rate: $rate | Skew: $skew | Parallelism: $par"
    echo "=========================================================="
    
    export RUN_LABEL="skew${skew}_par${par}_part${p}"
    if [ -d "experiments/results/${engine}_${workload}_latency_${RUN_LABEL}" ]; then
        echo "Skipping already completed run: ${engine}_${workload}_latency_${RUN_LABEL}"
        continue
    fi
    
    for compose_file in $(find experiments -name docker-compose.yml); do
        docker compose -f "$compose_file" down -v --remove-orphans >/dev/null 2>&1 || true
    done

    export KAFKA_PARTITIONS="$p"
    export RATE_PER_SEC="$rate"
    export SKEW="$skew"
    export PARALLELISM="$par"
    export EVENTS=10000 
    
    export RUN_LABEL="skew${skew}_par${par}_part${p}"
    
    export WORKLOAD_ID="${workload}_latency"
    export WORKLOAD="$(workload_name_for_id "$workload")"

    if [ "$engine" == "kafka-streams" ]; then
        ./scripts/run-kafka-streams-w1-latency.sh || echo "Run failed but continuing sweep..."
    else
        ./scripts/run-flink-w1-latency.sh || echo "Run failed but continuing sweep..."
    fi
    
    sleep 5
done

echo "Skew & Parallelism sweeps completed!"
