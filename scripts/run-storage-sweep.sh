#!/usr/bin/env bash
set -euo pipefail

STORAGE_REGIMES="${STORAGE_REGIMES:-local_nvme cloud_block_storage}"

echo "Starting Storage Regime sweeps..."

for storage in $STORAGE_REGIMES; do
  echo "=========================================================="
  echo "Running sweep for STORAGE_REGIME=$storage"
  echo "=========================================================="
  STORAGE_REGIME="$storage" RUN_LABEL="storage_${storage}" ./scripts/run-all-30m-sweeps.sh
  sleep 10
done

echo "Storage Regime sweeps completed!"
