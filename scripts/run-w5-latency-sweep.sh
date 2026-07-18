#!/usr/bin/env bash
set -euo pipefail

RATES="${RATES:-10 40}"
REPEAT_RATE="${REPEAT_RATE:-20}"
REPEAT_LABEL="${REPEAT_LABEL:-repeat1}"

run_pair() {
  local rate="$1"
  local label="$2"
  WORKLOAD=stream_stream_join WORKLOAD_ID=w5_latency START_MS=1000 RATE_PER_SEC="$rate" RUN_LABEL="$label" ./scripts/run-kafka-streams-w1-latency.sh
  WORKLOAD=stream_stream_join WORKLOAD_ID=w5_latency START_MS=1000 RATE_PER_SEC="$rate" RUN_LABEL="$label" ./scripts/run-flink-w1-latency.sh
}

for rate in $RATES; do
  label="rate${rate//./p}"
  run_pair "$rate" "$label"
done

run_pair "$REPEAT_RATE" "$REPEAT_LABEL"

PYTHONPATH=src python3 -m stream_state_bench.summarize_latency_results
