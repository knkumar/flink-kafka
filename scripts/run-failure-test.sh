#!/usr/bin/env bash
set -euo pipefail

ENGINE="${1:-kafka-streams}"
WORKLOAD="${2:-w1}"
FAILURE_MODE="${3:-jvm_kill}"
RATE_PER_SEC="${4:-20}"
EVENTS="${5:-2000}"

# 100 seconds total, inject at 50 seconds
INJECT_DELAY=$(( (EVENTS / RATE_PER_SEC) / 2 ))

local_workload_name="identity"
case "$WORKLOAD" in
    w1) local_workload_name="identity" ;;
    w2) local_workload_name="filter_map" ;;
    w3) local_workload_name="tumbling_count" ;;
    w4) local_workload_name="sliding_sum" ;;
    w5) local_workload_name="stream_stream_join" ;;
esac

echo "Starting failure test ($FAILURE_MODE) for $ENGINE $WORKLOAD at $RATE_PER_SEC/sec for $EVENTS events..."

start_ms=0
keys="${KEYS:-100}"
case "$WORKLOAD" in
    w4) start_ms=600000 ;;
    w5) start_ms=1000; keys="${KEYS:-100000}" ;;
esac

# Start the latency runner in the background
EVENTS="$EVENTS" RATE_PER_SEC="$RATE_PER_SEC" START_MS="$start_ms" KEYS="$keys" RUN_LABEL="failure_${FAILURE_MODE}_trial${TRIAL:-1}" WORKLOAD="$local_workload_name" WORKLOAD_ID="${WORKLOAD}_latency" ./scripts/run-${ENGINE}-w1-latency.sh &
PID=$!

echo "Waiting $INJECT_DELAY seconds before injecting failure..."
sleep "$INJECT_DELAY"

echo "Injecting $FAILURE_MODE into $ENGINE $WORKLOAD..."
if [[ "$ENGINE" == "kafka-streams" ]]; then
    COMPOSE_FILE="experiments/kafka_streams_w1/docker-compose.yml"
    WORKER_CONTAINER="kafka-streams-identity"
else
    COMPOSE_FILE="experiments/flink_w1/docker-compose.yml"
    WORKER_CONTAINER="flink-identity"
fi

export WORKLOAD="$local_workload_name"
WORKLOAD_ID="${WORKLOAD}_latency"
TOPIC_ID="${WORKLOAD_ID//_/-}"
export INPUT_TOPIC="bench-${TOPIC_ID}-input"
export LEFT_INPUT_TOPIC="bench-${TOPIC_ID}-left-input"
export RIGHT_INPUT_TOPIC="bench-${TOPIC_ID}-right-input"

# These must match the derived identity/topic names the backgrounded runner
# (run-${ENGINE}-w1-latency.sh) computed for this same WORKLOAD_ID/RUN_LABEL,
# so a container recreated below (node_loss) rejoins the same consumer
# group / transactional id / topic instead of a fresh default one. A
# mismatch here previously made a Kafka Streams node_loss run silently
# rejoin the wrong consumer group and drop 1294 of 2000 expected output
# records (see docs/project_log.md, 2026-07-17).
if [[ "$ENGINE" == "kafka-streams" ]]; then
    export OUTPUT_TOPIC="bench-${TOPIC_ID}-output"
    export APPLICATION_ID="stream-state-bench-${TOPIC_ID}-${WORKLOAD}-failure_${FAILURE_MODE}_trial${TRIAL:-1}"
else
    export OUTPUT_TOPIC="bench-${TOPIC_ID}-flink-output"
    export GROUP_ID="stream-state-bench-flink-${TOPIC_ID}-failure_${FAILURE_MODE}_trial${TRIAL:-1}"
    export TRANSACTIONAL_ID_PREFIX="stream-state-bench-flink-${TOPIC_ID}-failure_${FAILURE_MODE}_trial${TRIAL:-1}"
    export SOURCE_BOUNDED="false"
fi

if [[ "$FAILURE_MODE" == "jvm_kill" ]]; then
    docker compose -f "$COMPOSE_FILE" kill "$WORKER_CONTAINER"
    echo "Killed worker. Waiting 5s..."
    sleep 5
    docker compose -f "$COMPOSE_FILE" start "$WORKER_CONTAINER"
    echo "Restarted worker."
elif [[ "$FAILURE_MODE" == "broker_kill" ]]; then
    docker compose -f "$COMPOSE_FILE" stop kafka-1
    echo "Stopped broker. Waiting 5s..."
    sleep 5
    docker compose -f "$COMPOSE_FILE" start kafka-1
    echo "Restarted broker."
elif [[ "$FAILURE_MODE" == "node_loss" ]]; then
    docker compose -f "$COMPOSE_FILE" rm -fsv "$WORKER_CONTAINER"
    echo "Destroyed worker node. Waiting 5s..."
    sleep 5
    docker compose -f "$COMPOSE_FILE" up -d --no-deps "$WORKER_CONTAINER"
    echo "Restarted worker node."
elif [[ "$FAILURE_MODE" == "kraft_failover" ]]; then
    docker compose -f "$COMPOSE_FILE" restart kafka-1
    echo "Restarted Kafka (KRaft failover). Waiting 5s..."
    sleep 5
elif [[ "$FAILURE_MODE" == "s3_throttling" ]]; then
    if [[ "$ENGINE" == "flink" ]]; then
        docker compose -f "$COMPOSE_FILE" pause "$WORKER_CONTAINER"
        echo "Paused Flink worker (Object-Store Throttling). Waiting 10s..."
        sleep 10
        docker compose -f "$COMPOSE_FILE" unpause "$WORKER_CONTAINER"
        echo "Unpaused Flink worker."
    else
        echo "Skipping s3_throttling for $ENGINE (Flink-specific)."
    fi
elif [[ "$FAILURE_MODE" == "changelog_restore" ]]; then
    if [[ "$ENGINE" == "kafka-streams" ]]; then
        docker compose -f "$COMPOSE_FILE" exec -T "$WORKER_CONTAINER" rm -rf /tmp/kafka-streams
        echo "Deleted Kafka Streams state (Changelog Restore). Waiting 5s..."
        sleep 5
    else
        echo "Skipping changelog_restore for $ENGINE (Kafka Streams-specific)."
    fi
else
    echo "Unknown failure mode: $FAILURE_MODE"
    exit 1
fi

echo "Waiting for test to complete..."
wait $PID
echo "Failure test completed successfully!"
