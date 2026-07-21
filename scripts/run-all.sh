#!/usr/bin/env bash
set -euo pipefail

echo "Starting all runs..."

echo "1/7 Running Expert Tuning..."
./scripts/run-expert-tuning.sh

echo "2/7 Running Failure Tests..."
./scripts/run-all-failure-tests.sh

echo "2.5/7 Running State Size Sweep..."
./scripts/run-state-size-sweep.sh

echo "2.75/7 Running Saturation and Partition Sweep..."
./scripts/run-saturation-sweep.sh

echo "2.8/7 Running Skew and Parallelism Sweep..."
./scripts/run-skew-parallelism-sweep.sh

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

echo "8/7 Running Kafka Ablation Tests..."
./scripts/run-ablation-kafka-cache.sh
./scripts/run-ablation-kafka-commit.sh

echo "Summarizing Latency Results..."
make latency-summary

echo "Extracting Recovery Timelines..."
./scripts/extract-recovery-timeline.py

echo "All complete!"
