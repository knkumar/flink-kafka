#!/usr/bin/env bash
set -euo pipefail

PARTITIONS="${PARTITIONS:-25 50 100 200}"

echo "Starting Partition sweeps..."

for p in $PARTITIONS; do
  echo "=========================================================="
  echo "Running sweep for PARTITIONS=$p"
  echo "=========================================================="
  PARTITIONS="$p" RUN_LABEL="part${p}" ./scripts/run-all-30m-sweeps.sh
  sleep 10
done

echo "Partition sweeps completed!"
