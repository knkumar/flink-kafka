#!/usr/bin/env bash
set -euo pipefail
exec ./scripts/run-regime-sweep.sh STORAGE_REGIME "${STORAGE_REGIMES:-local_nvme cloud_block_storage}" storage_
