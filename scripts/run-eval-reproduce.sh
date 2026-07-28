#!/usr/bin/env bash
set -euo pipefail

# 10-20 minute short evaluation dataset reproduction script.
# Safely runs a microcosm of the whole pipeline and preserves original tables.

echo "=========================================================="
echo "Starting Stream State Bench Fast Evaluation (10-15 mins)..."
echo "=========================================================="

# Back up original results and manifest
echo "Backing up original results, manifest, and paper..."
rm -rf experiments/results_backup docs/results_manifest_backup.csv paper/final_paper_backup.md || true

cp -r experiments/results experiments/results_backup
cp docs/results_manifest.csv docs/results_manifest_backup.csv
cp paper/final_paper.md paper/final_paper_backup.md

results_backed_up=true

cleanup() {
    echo "=========================================================="
    echo "Cleaning up containers and restoring original files..."
    echo "=========================================================="
    # Tear down docker containers for both compose files
    for compose_file in $(find experiments -name docker-compose.yml); do
        docker compose -f "$compose_file" down -v --remove-orphans >/dev/null 2>&1 || true
    done

    # Restore backups if they were made
    if [ "${results_backed_up:-false}" = "true" ]; then
        rm -rf experiments/results
        mv experiments/results_backup experiments/results
        mv docs/results_manifest_backup.csv docs/results_manifest.csv
        mv paper/final_paper_backup.md paper/final_paper.md
        echo "Original results and paper restored successfully."
    fi
}
# Ensure cleanup occurs on exit
trap cleanup EXIT

# Clear checkpoints and reset state
echo "Cleaning old docker state and checkpoints..."
for compose_file in $(find experiments -name docker-compose.yml); do
    docker compose -f "$compose_file" down -v --remove-orphans >/dev/null 2>&1 || true
done
docker run --rm -v "$(pwd)/experiments/flink_w1/checkpoints:/checkpoints" ubuntu:latest bash -c 'rm -rf /checkpoints/*' || true

# Prepare clean results directory
rm -rf experiments/results
mkdir -p experiments/results

# 1. Runs latency stability sweeps (w1, w3) for 15s each
# Under 100 events/sec, 1 trial.
echo "----------------------------------------------------------"
echo "Step 1: Running fast latency stability sweeps (w1, w3) for 15s..."
echo "----------------------------------------------------------"
for engine in flink kafka-streams; do
    for workload in w1 w3; do
        echo "Running latency stability: $engine $workload"
        RATE_PER_SEC=100 DURATION_SEC=15 TRIAL=1 TRIALS=1 RUN_LABEL="stability_100_trial1" ./scripts/run-stability-tests.sh "$engine" "$workload"
    done
done

# 2. Runs failure recovery checks (w1, w3) for jvm_kill
# Using 200 events at 20 events/sec (10s total, failure injected at 5s).
echo "----------------------------------------------------------"
echo "Step 2: Running fast failure recovery checks (jvm_kill)..."
echo "----------------------------------------------------------"
for engine in flink kafka-streams; do
    for workload in w1 w3; do
        echo "Running failure test: $engine $workload jvm_kill"
        TRIAL=1 ./scripts/run-failure-test.sh "$engine" "$workload" "jvm_kill" 20 200
    done
done

# 3. Runs tuning sweeps (w3) for check-pointing/commit interval tuning
# Using 15s duration at 100/s for 1000ms & 10000ms.
echo "----------------------------------------------------------"
echo "Step 3: Running fast tuning sweeps (w3) for 1000ms & 10000ms..."
echo "----------------------------------------------------------"
for engine in flink kafka-streams; do
    for cp in 1000 10000; do
        echo "Running tuning: $engine w3 cp=$cp"
        if [ "$engine" = "flink" ]; then
            export CHECKPOINT_INTERVAL_MS=$cp
            export RUN_LABEL="tuning-cp-${cp}_trial1"
            DURATION_SEC=15 RATE_PER_SEC=100 TRIALS=1 ./scripts/run-stability-tests.sh flink w3
        else
            export COMMIT_INTERVAL_MS=$cp
            export RUN_LABEL="tuning-commit-${cp}_trial1"
            DURATION_SEC=15 RATE_PER_SEC=100 TRIALS=1 ./scripts/run-stability-tests.sh kafka-streams w3
        fi
    done
done

# 4. Generate the manifest, tables, and patch the paper
echo "----------------------------------------------------------"
echo "Step 4: Compiling manifest, paper tables, and patching..."
echo "----------------------------------------------------------"
python3 scripts/build-manifest.py
PYTHONPATH=src python3 -m stream_state_bench.build_paper_tables
PYTHONPATH=src python3 src/stream_state_bench/patch_paper.py

# Save evaluation artifacts to public experiments/results_eval directory
echo "----------------------------------------------------------"
echo "Step 5: Copying evaluation results to experiments/results_eval/..."
echo "----------------------------------------------------------"
rm -rf experiments/results_eval
mkdir -p experiments/results_eval

cp -r experiments/results/* experiments/results_eval/
cp docs/results_manifest.csv experiments/results_eval/results_manifest.csv
cp paper/final_paper.md experiments/results_eval/final_paper.md

echo "=========================================================="
echo "Short evaluation successfully finished!"
echo "Artifacts are stored in: experiments/results_eval/"
echo "Check tables in: experiments/results_eval/paper_table_sustained.csv/md"
echo "And patched paper: experiments/results_eval/final_paper.md"
echo "=========================================================="
