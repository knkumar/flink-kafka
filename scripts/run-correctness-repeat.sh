#!/usr/bin/env bash
set -euo pipefail

RUN_LABEL="${RUN_LABEL:-repeat}"
TRIALS="${TRIALS:-5}"

combinations=()
for trial in $(seq 1 "$TRIALS"); do
  combinations+=("kafka_streams identity w1 0 $trial")
  combinations+=("kafka_streams filter_map w2 0 $trial")
  combinations+=("kafka_streams tumbling_count w3 0 $trial")
  combinations+=("kafka_streams sliding_sum w4 600000 $trial")
  combinations+=("kafka_streams stream_stream_join w5 1000 $trial")
  
  combinations+=("flink identity w1 0 $trial")
  combinations+=("flink filter_map w2 0 $trial")
  combinations+=("flink tumbling_count w3 0 $trial")
  combinations+=("flink sliding_sum w4 600000 $trial")
  combinations+=("flink stream_stream_join w5 1000 $trial")
done

readarray -t shuffled < <(printf "%s\n" "${combinations[@]}" | shuf)

for combo in "${shuffled[@]}"; do
  engine=$(echo "$combo" | awk "{print \$1}")
  workload=$(echo "$combo" | awk "{print \$2}")
  workload_id=$(echo "$combo" | awk "{print \$3}")
  start_ms=$(echo "$combo" | awk "{print \$4}")
  trial=$(echo "$combo" | awk "{print \$5}")
  
  RESULT_DIR="experiments/results/${engine}_${workload_id}_${RUN_LABEL}_trial${trial}"
  if [[ -f "$RESULT_DIR/verification.json" ]]; then
      echo "Skipping correctness trial ${trial}/${TRIALS} for $engine $workload (already completed)"
      continue
  fi
  
  echo "--- Starting correctness trial ${trial}/${TRIALS} for $engine $workload ---"
  
  # Tear down completely to ensure genuinely independent trials
  for compose_file in $(find experiments -name docker-compose.yml); do
      docker compose -f "$compose_file" down -v --remove-orphans >/dev/null 2>&1 || true
  done
  
  if [ "$engine" == "kafka_streams" ]; then
    WORKLOAD="$workload" WORKLOAD_ID="$workload_id" START_MS="$start_ms" RUN_LABEL="${RUN_LABEL}_trial${trial}" ./scripts/run-kafka-streams-w1.sh
  else
    WORKLOAD="$workload" WORKLOAD_ID="$workload_id" START_MS="$start_ms" RUN_LABEL="${RUN_LABEL}_trial${trial}" ./scripts/run-flink-w1.sh
  fi
done

echo "Completed $TRIALS correctness trials."
