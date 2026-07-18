#!/usr/bin/env bash
# Grid search over commit/checkpoint intervals and other relevant configs
# to find the optimal tuning for stream state benchmarking.

set -euo pipefail

# Parameter search space
COMMIT_INTERVALS=(10 50 100 500 1000)
CHECKPOINT_INTERVALS=(500 1000 5000 10000)
STATE_BACKENDS=("hashmap" "rocksdb")
PARALLELISMS=(1 2 4)

echo "Starting expert tuning parameter search..."

# Loop over parameter search space
for backend in "${STATE_BACKENDS[@]}"; do
    for p in "${PARALLELISMS[@]}"; do
        for c_int in "${COMMIT_INTERVALS[@]}"; do
            for cp_int in "${CHECKPOINT_INTERVALS[@]}"; do
                echo "Testing tuning: Backend=$backend, Parallelism=$p, CommitInterval=${c_int}ms, CheckpointInterval=${cp_int}ms"
                
                # In a real environment, you would invoke the Flink job execution script here, e.g.:
                # ./run-benchmarks.sh --state-backend $backend --parallelism $p --commit-interval $c_int --checkpoint-interval $cp_int
                
                # Simulated delay for demonstration
                sleep 0.1
            done
        done
    done
done

echo "Expert tuning parameter search complete. Optimal tuning can be determined by analyzing the resulting resource_monitor.csv logs."
