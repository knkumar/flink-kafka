#!/usr/bin/env python3
"""Build docs/results_manifest.csv from experiments/results/.

Every number in the paper must trace to a result-directory path and an
exact command.  Re-run this script any time result directories change.
"""

import csv
import json
import re
import sys
from pathlib import Path
from collections import Counter


RESULTS_DIR = Path("experiments/results")
OUTPUT_CSV = Path("docs/results_manifest.csv")

# Directories that are containers / aggregates, not individual results
SKIP_DIRS = {"_archived", "resource_metrics", "test_join"}

# Patterns to exclude (invalid/incomplete results already renamed)
EXCLUDE_PATTERNS = re.compile(r"_invalid_|_incomplete_")


def categorize(dir_name: str) -> dict | None:
    """Return manifest metadata for a result directory, or None to skip."""

    # Skip invalid/incomplete directories
    if EXCLUDE_PATTERNS.search(dir_name):
        return None

    # 1. Sustained latency: {engine}_{workload}_latency_stability_100[_trial{N}]
    if "latency_stability_100" in dir_name:
        return {
            "paper_element": "Table 1: Sustained latency",
            "claim": "sustained_latency",
            "command": "scripts/run-sustained-matrix.sh",
            "limits": "Old CSV format — no t_e_ms/wm_ms/semantic_wait_ms. Re-run needed for timing attribution.",
        }

    # 2. Fault/recovery trials: {engine}_{wl}_latency_failure_{type}[_trial{N}]
    if "latency_failure_" in dir_name:
        return {
            "paper_element": "Table 3: Fault completion",
            "claim": "completion_under_faults",
            "command": "scripts/run-all-failure-tests.sh",
            "limits": "Completion/correctness only, not recovery-duration claims.",
        }

    # 3. Tuning study: {engine}_{wl}_latency_tuning*
    if "latency_tuning" in dir_name:
        return {
            "paper_element": "Table 4: Tuning effect",
            "claim": "tuning_effect",
            "command": "scripts/run-w3-tuning.sh",
            "limits": "",
        }

    # 4. Rate sweeps: {engine}_{wl}_latency_rate{N}
    if re.search(r"latency_rate\d+", dir_name):
        return {
            "paper_element": "Section 5.2: Rate sensitivity",
            "claim": "rate_sensitivity",
            "command": "scripts/run-w*-latency-sweep.sh",
            "limits": "",
        }

    # 5. Resource metrics: {engine}_{wl}_latency_resource_metrics
    if "latency_resource_metrics" in dir_name:
        return {
            "paper_element": "Table 5: Resource comparison",
            "claim": "resource_measurement",
            "command": "scripts/run-resource-matrix.sh",
            "limits": "Separate from sustained runs — not co-located with timing data.",
        }

    # 6. Saturation runs
    if "latency_saturation_" in dir_name:
        return {
            "paper_element": "Section 5.3: Saturation",
            "claim": "saturation_test",
            "command": "scripts/run-saturation-sweep.sh",
            "limits": "",
        }

    # 7. W5 latency standalone (not stability, not repeat)
    if re.search(r"w5_latency_stability", dir_name):
        return {
            "paper_element": "Section 5.4: W5 case study",
            "claim": "w5_latency",
            "command": "",
            "limits": "W5 kept outside headline claims unless full protocol applied.",
        }

    # 8. Correctness repeats: {engine}_{workload}[_repeat*] (no "latency" in name)
    if re.match(
        r"^(flink|kafka_streams)_(w[1-5])(_repeat.*)?$", dir_name
    ):
        return {
            "paper_element": "Table S1: Correctness verification",
            "claim": "functional_correctness",
            "command": "scripts/run-correctness-repeat.sh",
            "limits": "",
        }

    # 9. Base latency runs (single-shot, not stability/tuning/rate/failure)
    #    e.g. flink_w1_latency, kafka_streams_w3_latency_repeat1
    if re.match(
        r"^(flink|kafka_streams)_(w[1-5])_latency(_repeat\d*)?$", dir_name
    ):
        return {
            "paper_element": "Section 5.1: Baseline latency",
            "claim": "baseline_latency",
            "command": "make {engine}-{workload}-latency",
            "limits": "Single-shot baseline; not repeated-trial evidence.",
        }

    # Fallback
    return {
        "paper_element": "Unknown",
        "claim": "uncategorized",
        "command": "",
        "limits": "",
    }


def validate_dir(path: Path) -> dict:
    """Check verification and latency data inside a result dir."""
    info = {
        "has_verification": False,
        "verification_passed": False,
        "has_latency": False,
        "raw_files": [],
    }
    if not path.is_dir():
        return info
    for f in sorted(path.iterdir()):
        if f.is_file():
            info["raw_files"].append(f.name)
    info["has_verification"] = "verification.json" in info["raw_files"]
    if info["has_verification"]:
        try:
            v = json.loads((path / "verification.json").read_text(encoding="utf-8"))
            info["verification_passed"] = v.get("verification", {}).get("passed", False)
        except Exception:
            pass
    info["has_latency"] = (
        "latency_samples.csv" in info["raw_files"]
        or "latency_summary.json" in info["raw_files"]
    )
    return info


def main() -> int:
    if not RESULTS_DIR.is_dir():
        print(f"ERROR: {RESULTS_DIR} not found", file=sys.stderr)
        return 1

    rows: list[dict] = []
    uncategorized: list[str] = []
    skipped_invalid = 0

    for entry in sorted(RESULTS_DIR.iterdir()):
        if not entry.is_dir():
            continue
        if entry.name in SKIP_DIRS:
            continue

        cat = categorize(entry.name)
        if cat is None:
            skipped_invalid += 1
            continue

        if cat["claim"] == "uncategorized":
            uncategorized.append(entry.name)

        info = validate_dir(entry)

        # Extract engine/workload for sorting
        m = re.match(r"^(flink|kafka_streams)_(w[1-5])", entry.name)
        engine = m.group(1) if m else "unknown"
        workload = m.group(2) if m else "unknown"

        rows.append({
            "paper_element": cat["paper_element"],
            "claim": cat["claim"],
            "result_dir": str(entry),  # full repo-relative path
            "command": cat["command"],
            "raw_files": ";".join(info["raw_files"]),
            "n_trials": 1,
            "limits": cat["limits"],
            # internal sort keys
            "_engine": engine,
            "_workload": workload,
            "_verified": info["verification_passed"],
        })

    rows.sort(key=lambda r: (r["paper_element"], r["_engine"], r["_workload"], r["result_dir"]))

    # Write CSV
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["paper_element", "claim", "result_dir", "command", "raw_files", "n_trials", "limits"]
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    # Summary
    counts = Counter(r["claim"] for r in rows)
    verified = sum(1 for r in rows if r["_verified"])
    print(f"Total rows: {len(rows)}")
    print(f"Verified (passed): {verified}")
    print(f"Skipped invalid/incomplete: {skipped_invalid}")
    print("\nBreakdown by claim:")
    for claim, count in sorted(counts.items()):
        print(f"  {claim}: {count}")

    if uncategorized:
        print(f"\nUncategorized ({len(uncategorized)}):")
        for u in uncategorized:
            print(f"  {u}")

    print(f"\nManifest written to {OUTPUT_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
