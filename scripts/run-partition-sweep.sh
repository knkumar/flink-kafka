#!/usr/bin/env bash
set -euo pipefail
exec ./scripts/run-regime-sweep.sh PARTITIONS "${PARTITIONS:-25 50 100 200}" part
