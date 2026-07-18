#!/usr/bin/env bash
set -euo pipefail

EVENTS="${EVENTS:-100}"
KEYS="${KEYS:-100}"
SEED="${SEED:-7}"
START_MS="${START_MS:-0}"
RATE_PER_SEC="${RATE_PER_SEC:-20}"
RUN_LABEL="${RUN_LABEL:-}"
WORKLOAD="${WORKLOAD:-identity}"
WORKLOAD_ID="${WORKLOAD_ID:-w1_latency}"
TOPIC_ID="${WORKLOAD_ID//_/-}"
COMPOSE_FILE="experiments/flink_w1/docker-compose.yml"
RESULT_DIR="experiments/results/flink_${WORKLOAD_ID}"
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
LATENCY_JSON="${RESULT_DIR}/latency_summary.json"
LATENCY_CSV="${RESULT_DIR}/latency_samples.csv"
LOG_FILE="${RESULT_DIR}/docker-compose.log"
METADATA_JSON="${RESULT_DIR}/run_metadata.json"
INPUT_TOPIC="bench-${TOPIC_ID}-input"
LEFT_INPUT_TOPIC="bench-${TOPIC_ID}-left-input"
RIGHT_INPUT_TOPIC="bench-${TOPIC_ID}-right-input"
OUTPUT_TOPIC="bench-${TOPIC_ID}-flink-output"
GROUP_ID="stream-state-bench-flink-${TOPIC_ID}"
TRANSACTIONAL_ID_PREFIX="stream-state-bench-flink-${TOPIC_ID}"
if [[ -n "$RUN_LABEL" ]]; then
  GROUP_ID="${GROUP_ID}-${RUN_LABEL}"
  TRANSACTIONAL_ID_PREFIX="${TRANSACTIONAL_ID_PREFIX}-${RUN_LABEL}"
fi
SOURCE_BOUNDED="false"

export WORKLOAD INPUT_TOPIC LEFT_INPUT_TOPIC RIGHT_INPUT_TOPIC OUTPUT_TOPIC GROUP_ID TRANSACTIONAL_ID_PREFIX SOURCE_BOUNDED

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
PROBE_INPUT_TSV="$INPUT_TSV"

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
  PROBE_INPUT_TSV="$PRODUCER_TSV"
fi

docker compose -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true

# experiments/flink_w1/checkpoints is a host bind mount (not a Docker-managed
# volume), so `down -v` above does not clear it, and it survives across
# unrelated invocations of this script. FlinkIdentityJob.java auto-resumes
# from whichever job directory under it has the highest checkpoint number,
# with no check that the checkpoint's operator topology matches the current
# workload. That caused the W3/W4 30-minute stability runs and both W3
# checkpoint-interval tuning runs to crash at startup trying to restore a
# leftover checkpoint from an unrelated earlier job (see docs/project_log.md,
# 2026-07-17). Clear it here so every fresh invocation starts clean; this
# does not affect the intentional mid-run resume used by
# scripts/run-failure-test.sh, which restarts the container within the same
# invocation rather than calling this script again.
mkdir -p experiments/flink_w1/checkpoints
docker run --rm -v "$(pwd)/experiments/flink_w1/checkpoints:/checkpoints" ubuntu:latest \
  bash -c 'rm -rf /checkpoints/*' || true

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

docker compose -f "$COMPOSE_FILE" up -d --build flink-identity
sleep 8

PROBE_ARGS=(
  --compose-file "$COMPOSE_FILE" \
  --input-tsv "$PROBE_INPUT_TSV" \
  --actual-jsonl "$ACTUAL_JSONL" \
  --latency-json "$LATENCY_JSON" \
  --latency-csv "$LATENCY_CSV" \
  --expected-jsonl "$EXPECTED_JSONL" \
  --input-topic "$INPUT_TOPIC" \
  --output-topic "$OUTPUT_TOPIC" \
  --expected-count "$EXPECTED_COUNT" \
  --rate-per-sec "$RATE_PER_SEC" \
  --timeout-sec "$(( (EVENTS / RATE_PER_SEC) + 120 ))" \
  --consumer-isolation read_committed \
  --docker-network flink_w1_default
)
if [[ "$WORKLOAD" == "stream_stream_join" ]]; then
  PROBE_ARGS+=(
    --left-input-tsv "$LEFT_INPUT_TSV"
    --right-input-tsv "$RIGHT_INPUT_TSV"
    --left-input-topic "$LEFT_INPUT_TOPIC"
    --right-input-topic "$RIGHT_INPUT_TOPIC"
  )
fi

PYTHONPATH=src python3 -m stream_state_bench.kafka_latency_probe "${PROBE_ARGS[@]}"

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

latency = json.loads(Path("$LATENCY_JSON").read_text(encoding="utf-8"))["summary"]
metadata = {
    "events": $EVENTS,
    "keys": $KEYS,
    "seed": $SEED,
    "start_ms": $START_MS,
    "workload": "$WORKLOAD",
    "run_label": "$RUN_LABEL",
    "rate_per_sec": float("$RATE_PER_SEC"),
    "expected_output_records": $EXPECTED_COUNT,
    "produced_records": latency["produced_records"],
    "producer_topics": latency["producer_topics"],
    "kafka_image": "apache/kafka:4.3.1",
    "kafka_image_id": run(["docker", "image", "inspect", "apache/kafka:4.3.1", "--format", "{{.Id}}"]),
    "flink_app_image_id": run(["docker", "compose", "-f", "$COMPOSE_FILE", "images", "-q", "flink-identity"]),
    "flink_version": "2.2.0",
    "flink_kafka_connector_version": "5.0.0-2.2",
    "input_topic": "$INPUT_TOPIC",
    "left_input_topic": "$LEFT_INPUT_TOPIC",
    "right_input_topic": "$RIGHT_INPUT_TOPIC",
    "output_topic": "$OUTPUT_TOPIC",
    "group_id": "$GROUP_ID",
    "transactional_id_prefix": "$TRANSACTIONAL_ID_PREFIX",
    "source_bounded": False,
    "delivery_guarantee": "exactly_once",
    "consumer_isolation": "read_committed",
    "latency_measurement": latency["measurement"],
    "latency_p50_ms": latency["p50_ms"],
    "latency_p95_ms": latency["p95_ms"],
    "latency_p99_ms": latency["p99_ms"],
}
Path("$METADATA_JSON").write_text(json.dumps(metadata, indent=2) + "\\n", encoding="utf-8")
print(json.dumps(metadata, indent=2))
PY
