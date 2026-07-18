#!/usr/bin/env bash
set -euo pipefail

RUN_LABEL="${RUN_LABEL:-repeat}"
TRIALS="${TRIALS:-5}"

run_kafka_streams() {
  local workload="$1"
  local workload_id="$2"
  local start_ms="${3:-0}"
  WORKLOAD="$workload" WORKLOAD_ID="$workload_id" START_MS="$start_ms" RUN_LABEL="${RUN_LABEL}_trial${trial}" ./scripts/run-kafka-streams-w1.sh
}

run_flink() {
  local workload="$1"
  local workload_id="$2"
  local start_ms="${3:-0}"
  WORKLOAD="$workload" WORKLOAD_ID="$workload_id" START_MS="$start_ms" RUN_LABEL="${RUN_LABEL}_trial${trial}" ./scripts/run-flink-w1.sh
}

for trial in $(seq 1 "$TRIALS"); do
  echo "--- Starting correctness trial ${trial}/${TRIALS} ---"
  
  run_kafka_streams identity w1
  run_kafka_streams filter_map w2
  run_kafka_streams tumbling_count w3
  run_kafka_streams sliding_sum w4 600000
  run_kafka_streams stream_stream_join w5 1000
  
  run_flink identity w1
  run_flink filter_map w2
  run_flink tumbling_count w3
  run_flink sliding_sum w4 600000
  run_flink stream_stream_join w5 1000
done
echo "Completed $TRIALS correctness trials."
