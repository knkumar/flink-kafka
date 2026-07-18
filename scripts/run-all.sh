#!/usr/bin/env bash
set -euo pipefail

echo "Starting all runs..."

echo "1/7 Running Expert Tuning..."
./scripts/run-expert-tuning.sh

echo "2/7 Running Failure Tests..."
./scripts/run-all-failure-tests.sh

echo "3/7 Running Stability Tests..."
./scripts/run-all-30m-sweeps.sh

echo "4/7 Running Correctness Tests..."
make repeat-correctness
make engine-summary

echo "5/7 Running Partition Sweeps..."
./scripts/run-partition-sweep.sh

echo "6/7 Running Memory Sweeps..."
./scripts/run-memory-sweep.sh

echo "7/7 Running Storage Sweeps..."
./scripts/run-storage-sweep.sh

echo "All complete!"
