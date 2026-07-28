import json
from pathlib import Path
from collections import defaultdict
import csv
import statistics

def load_failure_results():
    results_dir = Path("experiments/results")
    rows = []
    for md_path in results_dir.glob("*/run_metadata.json"):
        run_dir = md_path.parent
        run_label = ""
        with md_path.open() as f:
            metadata = json.load(f)
            run_label = metadata.get("run_label", "")
        
        if not run_label.startswith("failure_"):
            continue
            
        parts = run_label.split("_")
        # e.g. failure_jvm_kill_trial1
        failure_mode = "_".join(parts[1:-1])
        trial = parts[-1]
        
        engine = "flink" if "flink" in run_dir.name else "kafka_streams"
        workload = metadata.get("workload", "unknown")
        
        summary_path = run_dir / "latency_summary.json"
        if not summary_path.exists():
            continue
            
        with summary_path.open() as f:
            summary = json.load(f)
            
        verification_path = run_dir / "verification.json"
        passed = False
        if verification_path.exists():
            with verification_path.open() as f:
                ver = json.load(f)
                passed = ver.get("verification", {}).get("passed", False)
                
        rows.append({
            "engine": engine,
            "workload": workload,
            "failure_mode": failure_mode,
            "trial": trial,
            "passed": passed,
            "p99_total_ms": float(summary.get("summary", {}).get("p99_ms", 0)),
            "p99_t1_t0_ms": float(summary.get("summary", {}).get("p99_write_to_input_append_latency_ms", 0) or 0),
            "p99_t2_t1_ms": float(summary.get("summary", {}).get("p99_input_append_to_result_emission_latency_ms", 0) or 0),
            "p99_t3_t2_ms": float(summary.get("summary", {}).get("p99_l_visibility_ms", 0) or 0)
        })
    return rows

def aggregate(rows):
    grouped = defaultdict(list)
    for row in rows:
        key = (row["engine"], row["workload"], row["failure_mode"])
        grouped[key].append(row)
        
    aggregates = []
    for key, group in grouped.items():
        engine, workload, failure_mode = key
        agg = {
            "engine": engine,
            "workload": workload,
            "failure_mode": failure_mode,
            "trials": len(group),
            "passed_trials": sum(1 for r in group if r["passed"]),
            "median_p99_total_ms": statistics.median([r["p99_total_ms"] for r in group]),
            "median_p99_t1_t0_ms": statistics.median([r["p99_t1_t0_ms"] for r in group]),
            "median_p99_t2_t1_ms": statistics.median([r["p99_t2_t1_ms"] for r in group]),
            "median_p99_t3_t2_ms": statistics.median([r["p99_t3_t2_ms"] for r in group]),
        }
        aggregates.append(agg)
    return sorted(aggregates, key=lambda x: (x["workload"], x["engine"], x["failure_mode"]))

def main():
    rows = load_failure_results()
    aggs = aggregate(rows)
    
    with open("experiments/results/failure_summary.csv", "w", newline="") as f:
        if aggs:
            writer = csv.DictWriter(f, fieldnames=list(aggs[0].keys()))
            writer.writeheader()
            writer.writerows(aggs)
            
    # Markdown
    lines = [
        "| Engine | Workload | Failure mode | Trials | Passed | p99 total | p99 t1-t0 | p99 t2-t1 | p99 t3-t2 |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |"
    ]
    for a in aggs:
        lines.append(f"| {a['engine']} | {a['workload']} | {a['failure_mode']} | {a['trials']} | {a['passed_trials']} | {a['median_p99_total_ms']:.1f} ms | {a['median_p99_t1_t0_ms']:.1f} ms | {a['median_p99_t2_t1_ms']:.1f} ms | {a['median_p99_t3_t2_ms']:.1f} ms |")
        
    with open("experiments/results/failure_summary.md", "w") as f:
        f.write("\n".join(lines) + "\n")
        
    print("Done")

if __name__ == "__main__":
    main()
