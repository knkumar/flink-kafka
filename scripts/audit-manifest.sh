#!/usr/bin/env bash

MANIFEST="${1:-docs/results_manifest.csv}"

if [ ! -f "$MANIFEST" ]; then
    echo "Warning: Manifest file $MANIFEST does not exist."
    exit 0
fi

total=0
passed=0
failed=0
failures=()

while IFS=, read -r paper_element claim result_dir command raw_files n_trials limits; do
    # Skip empty lines
    if [ -z "$result_dir" ] || [ -z "$claim" ]; then
        continue
    fi
    # If the line ends with \r, remove it
    claim=$(echo "$claim" | tr -d '\r')
    result_dir=$(echo "$result_dir" | tr -d '\r')

    ((total++))

    dir_failed=0
    fail_reason=""

    if [[ "$result_dir" == *"_invalid_"* ]] || [[ "$result_dir" == *"_incomplete_"* ]]; then
        dir_failed=1
        fail_reason="Invalid or incomplete directory name"
    elif [ ! -d "$result_dir" ]; then
        dir_failed=1
        fail_reason="Directory does not exist"
    else
        if [ ! -f "$result_dir/verification.json" ]; then
            dir_failed=1
            fail_reason="verification.json is missing"
        else
            is_passed=$(python3 -c "import json,sys; data=json.load(sys.stdin); print(str(data.get('verification', {}).get('passed', False)).lower())" < "$result_dir/verification.json" 2>/dev/null || echo "error")
            if [ "$is_passed" != "true" ]; then
                dir_failed=1
                fail_reason="verification.passed is not true"
            fi
        fi

        if [ "$dir_failed" -eq 0 ]; then
            if [[ "$claim" == "sustained_latency" ]] || [[ "$claim" == "tuning_effect" ]] || [[ "$claim" == "rate_sensitivity" ]]; then
                if [ ! -f "$result_dir/latency_samples.csv" ] && [ ! -f "$result_dir/latency_summary.json" ]; then
                    dir_failed=1
                    fail_reason="Missing latency data for latency claim"
                fi
            fi
        fi
    fi

    if [ "$dir_failed" -eq 1 ]; then
        ((failed++))
        failures+=("$result_dir ($claim): $fail_reason")
    else
        ((passed++))
    fi

done < <(tail -n +2 "$MANIFEST" 2>/dev/null || true)

echo "Audit Summary"
echo "============="
echo "Total checked: $total"
echo "Passed: $passed"
echo "Failed: $failed"

if [ "$failed" -gt 0 ]; then
    echo ""
    echo "Failure Details:"
    for f in "${failures[@]}"; do
        echo " - $f"
    done
    exit 1
fi

exit 0
