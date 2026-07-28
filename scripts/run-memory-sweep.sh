#!/usr/bin/env bash
set -euo pipefail
exec ./scripts/run-regime-sweep.sh MEMORY_REGIME "${MEMORY_REGIMES:-fit pressure starved}" mem_
