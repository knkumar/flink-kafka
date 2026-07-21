#!/usr/bin/env bash
set -euo pipefail

echo "This script reproduces the EDBT failure recovery and tuning study tables natively."

echo "0. Cleaning stale state (topics, checkpoints, volumes, offsets, application identities)..."
docker compose -f experiments/flink_w1/docker-compose.yml down -v --remove-orphans || true
docker compose -f experiments/kafka_streams_w1/docker-compose.yml down -v --remove-orphans || true
docker run --rm -v "$(pwd)/experiments/flink_w1/checkpoints:/checkpoints" ubuntu:latest bash -c 'rm -rf /checkpoints/*' || true

echo "1. Run all failure tests..."
./scripts/run-all-failure-tests.sh

echo "2. Run tuning tests..."
./scripts/run-w3-tuning.sh

echo "3. Generate statistical summaries and patch the paper..."
PYTHONPATH=src python3 src/stream_state_bench/patch_paper.py

echo "Done! Final paper is located at paper/final_paper.md."
