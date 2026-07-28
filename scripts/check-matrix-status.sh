#!/usr/bin/env bash
# Report artifact-backed status and an ETA for the sustained-load matrix.
#
# Usage:
#   scripts/check-matrix-status.sh
#   scripts/check-matrix-status.sh --watch [seconds]
set -euo pipefail

cd "$(dirname "$0")/.."

watch=0
interval=60

if [[ "${1:-}" == "--watch" ]]; then
    watch=1
    interval="${2:-60}"
    [[ "$interval" =~ ^[1-9][0-9]*$ ]] || {
        echo "watch interval must be a positive number of seconds" >&2
        exit 2
    }
elif [[ $# -ne 0 ]]; then
    echo "usage: $0 [--watch [seconds]]" >&2
    exit 2
fi

report() {
    local now pid pid_elapsed active_cell completed remaining median_seconds
    now=$(date +%s)
    pid=$(pgrep -f '(^|/)run-sustained-matrix\.sh' | head -n 1 || true)
    pid_elapsed=0
    active_cell=""

    if [[ -n "$pid" ]]; then
        pid_elapsed=$(ps -o etimes= -p "$pid" | tr -d ' ' || echo 0)
        active_cell=$(ps -eww -o args= | python3 -c '
import re
import sys
for line in sys.stdin:
    if "kafka_latency_probe" not in line:
        continue
    match = re.search(r"(kafka_streams|flink)_(w[1-4])_latency_stability_100_trial([1-5])", line)
    if match:
        engine = "kafka-streams" if match.group(1) == "kafka_streams" else match.group(1)
        print(f"{engine} {match.group(2)} trial={match.group(3)}")
        break
')
    fi

    read -r completed median_seconds < <(python3 - <<'PY'
import json
import statistics
from pathlib import Path

root = Path("experiments/results")
durations = []
complete = 0
for engine in ("kafka_streams", "flink"):
    for workload in ("w1", "w2", "w3", "w4"):
        for trial in range(1, 6):
            suffix = "stability_100" if trial == 1 else f"stability_100_trial{trial}"
            directory = root / f"{engine}_{workload}_latency_{suffix}"
            verification = directory / "verification.json"
            summary = directory / "latency_summary.json"
            try:
                passed = json.loads(verification.read_text()).get("verification", {}).get("passed") is True
            except (FileNotFoundError, json.JSONDecodeError):
                passed = False
            if passed and summary.is_file():
                complete += 1
                input_path = directory / ("producer_input.tsv" if (directory / "producer_input.tsv").is_file() else "input.tsv")
                if input_path.is_file():
                    duration = int(verification.stat().st_mtime - input_path.stat().st_mtime)
                    # A sustained cell has 1,800 seconds of paced input plus
                    # setup/teardown. Ignore historical timestamp outliers.
                    if 1500 <= duration <= 2700:
                        durations.append(duration)

# 30 minutes of pacing plus a small setup/teardown allowance is the safe fallback.
median = int(statistics.median(durations)) if durations else 1920
print(complete, median)
PY
)

    remaining=$((40 - completed))
    (( remaining < 0 )) && remaining=0

    local eta_seconds eta_epoch
    eta_seconds=$((remaining * median_seconds))
    if [[ -n "$pid" && -n "$active_cell" ]]; then
        # The active cell is included in remaining; credit elapsed runner time,
        # capped at one typical cell to avoid a negative estimate.
        local credit=$pid_elapsed
        (( credit > median_seconds )) && credit=$median_seconds
        eta_seconds=$((eta_seconds - credit))
        (( eta_seconds < 0 )) && eta_seconds=0
    fi
    eta_epoch=$((now + eta_seconds))

    printf '=== Sustained matrix status ===\n'
    if [[ -n "$pid" ]]; then
        printf 'Process:    RUNNING (pid %s, elapsed %s)\n' "$pid" "$(ps -o etime= -p "$pid" | xargs)"
    else
        printf 'Process:    NOT RUNNING\n'
    fi
    printf 'Verified:   %s/40 cells\n' "$completed"
    printf 'Remaining:  %s cells\n' "$remaining"
    [[ -n "$active_cell" ]] && printf 'Active:     %s\n' "$active_cell"
    printf 'ETA basis:  %dm per cell (median verified runtime; includes setup)\n' "$((median_seconds / 60))"
    printf 'ETA:        %dh%02dm remaining; estimated completion %s\n' \
        "$((eta_seconds / 3600))" "$(((eta_seconds % 3600) / 60))" \
        "$(date -d "@$eta_epoch" '+%Y-%m-%d %H:%M %Z')"
}

if (( watch )); then
    while true; do
        report
        sleep "$interval"
        printf '\n'
    done
else
    report
fi
