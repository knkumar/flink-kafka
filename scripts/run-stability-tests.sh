#!/usr/bin/env bash
set -euo pipefail

RATE_PER_SEC="${RATE_PER_SEC:-100}"
DURATION_SEC="${DURATION_SEC:-120}"
EVENTS=$(( RATE_PER_SEC * DURATION_SEC ))

run_stability() {
    local engine="$1"
    local workload="$2"
    
    local trial_suffix=""
    if [[ -n "${TRIAL:-}" ]]; then
        trial_suffix="_trial${TRIAL}"
    fi
    local run_label="stability_${RATE_PER_SEC}${trial_suffix}"
    local script="scripts/run-${engine}-w1-latency.sh"
    
    local start_ms=0
    local keys=100
    case "$workload" in
        w1) workload_name="identity" ;;
        w2) workload_name="filter_map" ;;
        w3) workload_name="tumbling_count" ;;
        w4) workload_name="sliding_sum"; start_ms=600000 ;;
        w5) workload_name="stream_stream_join"; start_ms=1000; keys=100000 ;;
        *) echo "Unknown workload: $workload"; exit 1 ;;
    esac
    
    local group_id
    local compose_dir
    local result_dir
    
    if [[ "$engine" == "kafka-streams" ]]; then
        group_id="stream-state-bench-${workload}-latency-${workload_name}-${run_label}"
        compose_dir="experiments/kafka_streams_w1"
        result_dir="experiments/results/kafka_streams_${workload}_latency_${run_label}"
    else
        group_id="stream-state-bench-flink-${workload}-latency-${run_label}"
        compose_dir="experiments/flink_w1"
        result_dir="experiments/results/${engine//-/_}_${workload}_latency_${run_label}"
    fi
    
    echo "Starting stability run for $engine $workload ($workload_name) at $RATE_PER_SEC/sec for $DURATION_SEC sec ($EVENTS events)..."
    
    # Run in background
    EVENTS="$EVENTS" RATE_PER_SEC="$RATE_PER_SEC" START_MS="$start_ms" KEYS="$keys" RUN_LABEL="$run_label" WORKLOAD="$workload_name" WORKLOAD_ID="${workload}_latency" "$script" &
    local pid=$!
    
    # Wait for topics to be created and application to start
    echo "Waiting for test to initialize..."
    sleep 20
    
    # Start lag monitoring
    mkdir -p "$result_dir"
    echo "Starting lag monitor in background..."
    python3 src/stream_state_bench/monitor_lag.py \
        --compose-file "${compose_dir}/docker-compose.yml" \
        --group-id "$group_id" \
        --output-csv "${result_dir}/lag.csv" \
        --interval-sec 5 &
    local lag_pid=$!
    
    # Wait for test to finish
    wait $pid || { echo "Test failed"; kill $lag_pid; exit 1; }
    
    # Kill lag monitor
    kill $lag_pid || true
    
    echo "Stability run finished for $engine $workload."
}

TRIALS="${TRIALS:-5}"

if [[ $# -ge 2 ]]; then
    for trial in $(seq 1 "$TRIALS"); do
        echo "--- Starting stability trial ${trial}/${TRIALS} for $1 $2 ---"
        TRIAL="$trial" run_stability "$1" "$2"
    done
    echo "Completed $TRIALS stability trials for $1 $2."
else
    echo "Usage: $0 <engine> <workload>"
    echo "Example: $0 flink w1"
    exit 1
fi
