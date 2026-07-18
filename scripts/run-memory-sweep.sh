#!/usr/bin/env bash
set -euo pipefail

MEMORY_REGIMES="${MEMORY_REGIMES:-fit pressure starved}"

echo "Starting Memory Regime sweeps..."

for mem in $MEMORY_REGIMES; do
  echo "=========================================================="
  echo "Running sweep for MEMORY_REGIME=$mem"
  echo "=========================================================="
  MEMORY_REGIME="$mem" RUN_LABEL="mem_${mem}" ./scripts/run-all-30m-sweeps.sh
  sleep 10
done

echo "Memory Regime sweeps completed!"
