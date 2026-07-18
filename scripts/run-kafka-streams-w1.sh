#!/usr/bin/env bash
set -euo pipefail

EVENTS="${EVENTS:-1000}"
KEYS="${KEYS:-100}"
SEED="${SEED:-7}"
START_MS="${START_MS:-0}"
WORKLOAD="${WORKLOAD:-identity}"
WORKLOAD_ID="${WORKLOAD_ID:-w1}"
RUN_LABEL="${RUN_LABEL:-}"
COMPOSE_FILE="experiments/kafka_streams_w1/docker-compose.yml"
RESULT_DIR="experiments/results/kafka_streams_${WORKLOAD_ID}"
if [[ -n "$RUN_LABEL" ]]; then
  RESULT_DIR="${RESULT_DIR}_${RUN_LABEL}"
fi
INPUT_TSV="${RESULT_DIR}/input.tsv"
LEFT_INPUT_TSV="${RESULT_DIR}/left_input.tsv"
RIGHT_INPUT_TSV="${RESULT_DIR}/right_input.tsv"
PRODUCER_TSV="${RESULT_DIR}/producer_input.tsv"
EXPECTED_JSONL="${RESULT_DIR}/expected.jsonl"
ACTUAL_JSONL="${RESULT_DIR}/actual.jsonl"
VERIFY_JSON="${RESULT_DIR}/verification.json"
LOG_FILE="${RESULT_DIR}/docker-compose.log"
METADATA_JSON="${RESULT_DIR}/run_metadata.json"
INPUT_TOPIC="bench-${WORKLOAD_ID}-input"
LEFT_INPUT_TOPIC="bench-${WORKLOAD_ID}-left-input"
RIGHT_INPUT_TOPIC="bench-${WORKLOAD_ID}-right-input"
OUTPUT_TOPIC="bench-${WORKLOAD_ID}-output"
APPLICATION_ID="stream-state-bench-${WORKLOAD_ID}-${WORKLOAD}"

export WORKLOAD INPUT_TOPIC LEFT_INPUT_TOPIC RIGHT_INPUT_TOPIC OUTPUT_TOPIC APPLICATION_ID

mkdir -p "$RESULT_DIR"

cleanup() {
  docker compose -f "$COMPOSE_FILE" logs --no-color > "$LOG_FILE" 2>/dev/null || true
  docker compose -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

PYTHONPATH=src python3 -m stream_state_bench.generate_input \
  --workload "$WORKLOAD" \
  --events "$EVENTS" \
  --keys "$KEYS" \
  --seed "$SEED" \
  --start-ms "$START_MS" \
  --input-tsv "$INPUT_TSV" \
  --left-input-tsv "$LEFT_INPUT_TSV" \
  --right-input-tsv "$RIGHT_INPUT_TSV" \
  --expected-jsonl "$EXPECTED_JSONL"
EXPECTED_COUNT="$(wc -l < "$EXPECTED_JSONL" | tr -d ' ')"
PRODUCER_INPUT="$INPUT_TSV"
PRODUCER_INPUT_RECORDS="$EVENTS"
LEFT_PRODUCER_INPUT_RECORDS=0
RIGHT_PRODUCER_INPUT_RECORDS=0

if [[ "$WORKLOAD" == "tumbling_count" || "$WORKLOAD" == "sliding_sum" ]]; then
  cp "$INPUT_TSV" "$PRODUCER_TSV"
  python3 - <<PY
from pathlib import Path

events = int("$EVENTS")
start_ms = int("$START_MS")
step_ms = 100
workload = "$WORKLOAD"
last_event_time = start_ms + (events - 1) * step_ms
if workload == "sliding_sum":
    slide_ms = 60_000
    window_ms = 600_000
    latest_start = (last_event_time // slide_ms) * slide_ms
    tick_time = latest_start + window_ms + slide_ms
else:
    window_ms = 60_000
    tick_time = ((last_event_time // window_ms) + 2) * window_ms
with Path("$PRODUCER_TSV").open("a", encoding="utf-8") as handle:
    handle.write(f"__tick__\\t0\\t0\\t{tick_time}\\n")
PY
  PRODUCER_INPUT="$PRODUCER_TSV"
  PRODUCER_INPUT_RECORDS="$((EVENTS + 1))"
elif [[ "$WORKLOAD" == "stream_stream_join" ]]; then
  PRODUCER_INPUT_RECORDS="$((EVENTS * 2))"
  LEFT_PRODUCER_INPUT_RECORDS="$EVENTS"
  RIGHT_PRODUCER_INPUT_RECORDS="$EVENTS"
fi

docker compose -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
docker compose -f "$COMPOSE_FILE" up -d --build kafka

for _ in $(seq 1 60); do
  if docker compose -f "$COMPOSE_FILE" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

docker compose -f "$COMPOSE_FILE" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic "$INPUT_TOPIC" \
  --partitions 1 \
  --replication-factor 1 \
  --config message.timestamp.type=LogAppendTime

if [[ "$WORKLOAD" == "stream_stream_join" ]]; then
  docker compose -f "$COMPOSE_FILE" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic "$LEFT_INPUT_TOPIC" \
    --partitions 1 \
    --replication-factor 1 \
    --config message.timestamp.type=LogAppendTime

  docker compose -f "$COMPOSE_FILE" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic "$RIGHT_INPUT_TOPIC" \
    --partitions 1 \
    --replication-factor 1 \
    --config message.timestamp.type=LogAppendTime
fi

docker compose -f "$COMPOSE_FILE" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic "$OUTPUT_TOPIC" \
  --partitions 1 \
  --replication-factor 1

docker compose -f "$COMPOSE_FILE" up -d --build kafka-streams-identity
sleep 5

if [[ "$WORKLOAD" == "stream_stream_join" ]]; then
  docker compose -f "$COMPOSE_FILE" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 \
    --topic "$LEFT_INPUT_TOPIC" < "$LEFT_INPUT_TSV"

  docker compose -f "$COMPOSE_FILE" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 \
    --topic "$RIGHT_INPUT_TOPIC" < "$RIGHT_INPUT_TSV"
else
  docker compose -f "$COMPOSE_FILE" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 \
    --topic "$INPUT_TOPIC" < "$PRODUCER_INPUT"
fi

docker compose -f "$COMPOSE_FILE" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic "$OUTPUT_TOPIC" \
  --from-beginning \
  --max-messages "$EXPECTED_COUNT" \
  --timeout-ms 60000 \
  --isolation-level read_committed > "$ACTUAL_JSONL"

PYTHONPATH=src python3 -m stream_state_bench.verify_external_output \
  --workload "$WORKLOAD" \
  --actual-jsonl "$ACTUAL_JSONL" \
  --events "$EVENTS" \
  --keys "$KEYS" \
  --seed "$SEED" \
  --start-ms "$START_MS" \
  --out "$VERIFY_JSON"

python3 - <<PY
import json
import subprocess
from pathlib import Path

def run(cmd):
    return subprocess.check_output(cmd, text=True).strip()

metadata = {
    "events": $EVENTS,
    "keys": $KEYS,
    "seed": $SEED,
    "start_ms": $START_MS,
    "workload": "$WORKLOAD",
    "run_label": "$RUN_LABEL",
    "expected_output_records": $EXPECTED_COUNT,
    "producer_input_records": $PRODUCER_INPUT_RECORDS,
    "left_producer_input_records": $LEFT_PRODUCER_INPUT_RECORDS,
    "right_producer_input_records": $RIGHT_PRODUCER_INPUT_RECORDS,
    "kafka_image": "apache/kafka:4.3.1",
    "kafka_image_id": run(["docker", "image", "inspect", "apache/kafka:4.3.1", "--format", "{{.Id}}"]),
    "streams_image_id": run(["docker", "compose", "-f", "$COMPOSE_FILE", "images", "-q", "kafka-streams-identity"]),
    "input_topic": "$INPUT_TOPIC",
    "left_input_topic": "$LEFT_INPUT_TOPIC",
    "right_input_topic": "$RIGHT_INPUT_TOPIC",
    "output_topic": "$OUTPUT_TOPIC",
    "processing_guarantee": "exactly_once_v2",
    "consumer_isolation": "read_committed",
}
Path("$METADATA_JSON").write_text(json.dumps(metadata, indent=2) + "\\n", encoding="utf-8")
print(json.dumps(metadata, indent=2))
PY
