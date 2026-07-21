#!/bin/bash
set -ex
export RATE_PER_SEC=10
export DURATION_SEC=10
export WORKLOAD="stream_stream_join"
export EVENTS=100
export KEYS=10

export COMPOSE_FILE="experiments/flink_w1/docker-compose.yml"
docker compose -f "$COMPOSE_FILE" down -v --remove-orphans || true
rm -rf experiments/flink_w1/checkpoints/*

export LEFT_INPUT_TOPIC="test-left"
export RIGHT_INPUT_TOPIC="test-right"
export OUTPUT_TOPIC="test-output"
export TRANSACTIONAL_ID_PREFIX="test-join"
export GROUP_ID="test-join"
export CHECKPOINT_INTERVAL_MS=1000

PYTHONPATH=src python3 -m stream_state_bench.generate_input --workload "$WORKLOAD" --events "$EVENTS" --keys "$KEYS" --input-tsv experiments/results/test_join/producer.tsv --left-input-tsv experiments/results/test_join/left.tsv --right-input-tsv experiments/results/test_join/right.tsv --expected-jsonl experiments/results/test_join/expected.jsonl

docker run --rm -v "$(pwd)/experiments/flink_w1/checkpoints:/checkpoints" ubuntu:latest bash -c 'rm -rf /checkpoints/*' || true

docker compose -f "$COMPOSE_FILE" up -d kafka-1 kafka-2 kafka-3
sleep 10

docker compose -f "$COMPOSE_FILE" exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --create --if-not-exists --topic "$LEFT_INPUT_TOPIC" --partitions 1 --replication-factor 3 --config min.insync.replicas=2 --config message.timestamp.type=LogAppendTime
docker compose -f "$COMPOSE_FILE" exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --create --if-not-exists --topic "$RIGHT_INPUT_TOPIC" --partitions 1 --replication-factor 3 --config min.insync.replicas=2 --config message.timestamp.type=LogAppendTime
docker compose -f "$COMPOSE_FILE" exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 --create --if-not-exists --topic "$OUTPUT_TOPIC" --partitions 1 --replication-factor 3 --config min.insync.replicas=2

docker compose -f "$COMPOSE_FILE" up -d minio
docker compose -f "$COMPOSE_FILE" up -d minio-setup
sleep 5
docker compose -f "$COMPOSE_FILE" up -d flink-identity

PYTHONPATH=src python3 -m stream_state_bench.kafka_latency_probe --rate-per-sec $RATE_PER_SEC --timeout-sec 30 --expected-jsonl experiments/results/test_join/expected.jsonl --actual-jsonl experiments/results/test_join/actual.jsonl --summary-json experiments/results/test_join/summary.json --left-input-tsv experiments/results/test_join/left.tsv --left-input-topic "$LEFT_INPUT_TOPIC" --right-input-tsv experiments/results/test_join/right.tsv --right-input-topic "$RIGHT_INPUT_TOPIC" --output-topic "$OUTPUT_TOPIC"

echo "ACTUAL:"
cat experiments/results/test_join/actual.jsonl
echo "SUMMARY:"
cat experiments/results/test_join/summary.json

docker compose -f "$COMPOSE_FILE" logs flink-identity > experiments/results/test_join/flink.log
