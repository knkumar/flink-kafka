#!/usr/bin/env bash
set -euo pipefail

echo "Running 5 independent trials for correctness..."
./scripts/run-correctness-repeat.sh

echo "Running full failure matrix..."
./scripts/run-all-failure-tests.sh

echo "Summarizing results..."
make engine-summary
make latency-summary

echo "All missing experiments completed!"
