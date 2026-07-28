#!/usr/bin/env bash
set -euo pipefail

ENGINES=("flink" "kafka-streams")
WORKLOADS=("w1" "w2" "w3" "w4")
TRIALS=(1 2 3 4 5)
DURATION=60

mkdir -p experiments/results

function run_trial() {
    local engine=$1
    local w=$2
    local t=$3
    local out_dir="experiments/results/${engine}_${w}_resource_trial${t}"
    mkdir -p "$out_dir"
    
    local run_script="scripts/run-${engine}-w1.sh"
    if [[ "$engine" == "kafka-streams" ]]; then
        run_script="scripts/run-kafka-streams-w1.sh"
    fi
    # Use the appropriate workload if they exist, else we just pass WORKLOAD=
    export WORKLOAD_ID=$w
    export RUN_LABEL="resource_trial${t}"
    
    if [[ "$w" == "idle" ]]; then
        export EVENTS=0
        export WORKLOAD_ID="w1"
    else
        export EVENTS=100000
    fi
    
    echo "Running $engine $w trial $t"
    
    # We start the monitor in background. But we need containers.
    # The run scripts bring up containers. So we can't start monitor until they are up.
    # The run scripts might be blocking. So we run them in background, wait a bit, then start monitor.
    
    bash "$run_script" > "${out_dir}/run.log" 2>&1 &
    local run_pid=$!
    
    sleep 15
    
    # Get containers
    local containers=$(docker ps --format '{{.Names}}' | grep "stream-state-bench-${engine}" || true)
    if [[ -n "$containers" ]]; then
        python3 src/stream_state_bench/resource_monitor.py \
            --containers $containers \
            --output-csv "${out_dir}/resource_monitor.csv" \
            --interval-sec 5 \
            --duration-sec $DURATION
    fi
    
    wait $run_pid || true
}

for e in "${ENGINES[@]}"; do
    for w in "${WORKLOADS[@]}"; do
        for t in "${TRIALS[@]}"; do
            run_trial "$e" "$w" "$t"
        done
    done
    # idle baseline
    run_trial "$e" "idle" "1"
done

# Aggregate results
python3 src/stream_state_bench/aggregate_resource_metrics.py
