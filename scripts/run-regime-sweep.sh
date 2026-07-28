#!/usr/bin/env bash
# Run experiments/results-labeled 30-minute sweeps across a list of values for
# a single environment variable (e.g. PARTITIONS, MEMORY_REGIME, STORAGE_REGIME).
#
# Usage: scripts/run-regime-sweep.sh VAR_NAME "value1 value2 ..." label_prefix
set -euo pipefail

var_name="$1"
values="$2"
label_prefix="$3"

echo "Starting ${var_name} sweeps..."

for value in $values; do
  echo "=========================================================="
  echo "Running sweep for ${var_name}=${value}"
  echo "=========================================================="
  env "${var_name}=${value}" RUN_LABEL="${label_prefix}${value}" ./scripts/run-all-30m-sweeps.sh
  sleep 10
done

echo "${var_name} sweeps completed!"
