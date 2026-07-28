#!/usr/bin/env bash
set -euo pipefail

echo "Starting all runs..."

echo "1/11 Running Expert Tuning..."
./scripts/run-expert-tuning.sh

echo "2/11 Running Failure Tests..."
./scripts/run-all-failure-tests.sh

echo "3/11 Running State Size Sweep..."
./scripts/run-state-size-sweep.sh

echo "4/11 Running Saturation and Partition Sweep..."
./scripts/run-saturation-sweep.sh

echo "5/11 Running Skew and Parallelism Sweep..."
./scripts/run-skew-parallelism-sweep.sh

echo "6/11 Running Stability Tests..."
./scripts/run-all-30m-sweeps.sh

echo "7/11 Running Correctness Tests..."
make repeat-correctness
make engine-summary

echo "8/11 Running Partition Sweeps..."
./scripts/run-partition-sweep.sh

echo "9/11 Running Memory Sweeps..."
./scripts/run-memory-sweep.sh

echo "10/11 Running Storage Sweeps..."
./scripts/run-storage-sweep.sh

echo "11/11 Running Kafka Ablation Tests..."
./scripts/run-ablation-kafka-cache.sh
./scripts/run-ablation-kafka-commit.sh

echo "Summarizing Latency Results..."
make latency-summary

echo "Extracting Recovery Timelines..."
./scripts/extract-recovery-timeline.py

echo "All complete!"
