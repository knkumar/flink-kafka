#!/usr/bin/env bash
set -euo pipefail

WORKLOAD="${WORKLOAD:-identity}"
WORKLOAD_ID="${WORKLOAD_ID:-w1_latency}"
START_MS="${START_MS:-0}"
RATES="${RATES:-10 40}"
REPEAT_RATE="${REPEAT_RATE:-20}"
REPEAT_LABEL="${REPEAT_LABEL:-repeat1}"

run_pair() {
  local rate="$1"
  local label="$2"
  WORKLOAD="$WORKLOAD" WORKLOAD_ID="$WORKLOAD_ID" START_MS="$START_MS" RATE_PER_SEC="$rate" RUN_LABEL="$label" ./scripts/run-kafka-streams-w1-latency.sh
  WORKLOAD="$WORKLOAD" WORKLOAD_ID="$WORKLOAD_ID" START_MS="$START_MS" RATE_PER_SEC="$rate" RUN_LABEL="$label" ./scripts/run-flink-w1-latency.sh
}

for rate in $RATES; do
  label="rate${rate//./p}"
  run_pair "$rate" "$label"
done

run_pair "$REPEAT_RATE" "$REPEAT_LABEL"

PYTHONPATH=src python3 -m stream_state_bench.summarize_latency_results
