#!/usr/bin/env bash
set -euo pipefail

# The tuning test is already running in the background. We will just wait for it.
echo "Waiting for any running tests..."
wait

echo "Running failure tests (which will skip already-passed tests)..."
./scripts/run-all-failure-tests.sh

echo "Regenerating summaries..."
PYTHONPATH=src python3 src/stream_state_bench/summarize_engine_results.py
PYTHONPATH=src python3 src/stream_state_bench/summarize_failure_results.py
