#!/usr/bin/env bash
set -euo pipefail

ENGINES=("kafka-streams" "flink")
WORKLOADS=("w1" "w2" "w3" "w4")
TRIALS=(1 2 3 4 5)

# Generate combinations
tmp_file=$(mktemp)
for e in "${ENGINES[@]}"; do
    for w in "${WORKLOADS[@]}"; do
        for t in "${TRIALS[@]}"; do
            echo "$e $w $t" >> "$tmp_file"
        done
    done
done

# Shuffle and save to run_order.txt
shuf "$tmp_file" > run_order.txt
rm "$tmp_file"

echo "Randomized run order saved to run_order.txt"
cat run_order.txt

# Run the matrix
while read -r engine workload trial; do
    echo "=========================================================="
    echo "Running matrix cell: engine=$engine workload=$workload trial=$trial"
    echo "=========================================================="
    RUN_LABEL="stability_100_trial${trial}" RATE_PER_SEC=100 DURATION_SEC=1800 \
        scripts/run-stability-tests.sh "$engine" "$workload"
done < run_order.txt
