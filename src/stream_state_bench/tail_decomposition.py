import argparse
import csv
import json
from pathlib import Path
import pandas as pd
import numpy as np

from stream_state_bench.summarize_engine_results import engine_from_dir, workload_id_from_dir

def load_rows(results_dir: Path) -> list[dict[str, object]]:
    rows = []
    for latency_path in sorted(results_dir.glob("*/latency_summary.json")):
        run_dir = latency_path.parent
        metadata_path = run_dir / "run_metadata.json"
        verification_path = run_dir / "verification.json"
        if not metadata_path.exists() or not verification_path.exists():
            continue
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        verification = json.loads(verification_path.read_text(encoding="utf-8"))["verification"]
        summary = json.loads(latency_path.read_text(encoding="utf-8"))["summary"]
        
        rows.append(
            {
                "engine": engine_from_dir(run_dir.name),
                "workload_id": workload_id_from_dir(run_dir.name),
                "workload": metadata.get("workload", ""),
                "rate_per_sec": summary["rate_per_sec"],
                "passed": verification["passed"],
                "result_dir": str(run_dir),
            }
        )
    return rows

def aggregate_rows_with_ci(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped = {}
    for row in rows:
        key = (row["engine"], row["workload_id"], row["workload"], row["rate_per_sec"])
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(row)
        
    # First, calculate per-run tail values
    run_values = {}
    for key, group in grouped.items():
        run_values[key] = []
        for row in group:
            samples_csv = Path(row["result_dir"]) / "latency_samples.csv"
            if not samples_csv.exists():
                continue
            df = pd.read_csv(samples_csv)
            if 't0_ms' in df.columns:
                df = df.sort_values('t0_ms')
                warmup_idx = int(len(df) * 0.05)
                df = df.iloc[warmup_idx:]
            if 'latency_ms' in df.columns and len(df) > 0:
                cutoff = np.percentile(df['latency_ms'].dropna(), 99.9)
                df = df[df['latency_ms'] <= cutoff]
                top_1_percent_cutoff = np.percentile(df['latency_ms'].dropna(), 99)
                tail_df = df[df['latency_ms'] >= top_1_percent_cutoff]
                if len(tail_df) > 0:
                    run_vals = {
                        "tail_latency_ms": tail_df['latency_ms'].mean(),
                        "tail_semantic_wait_ms": tail_df['semantic_wait'].mean() if 'semantic_wait' in tail_df.columns else 0.0,
                        "tail_engine_compute_ms": tail_df['engine_compute'].mean() if 'engine_compute' in tail_df.columns else 0.0,
                        "tail_visibility_ms": tail_df['visibility'].mean() if 'visibility' in tail_df.columns else 0.0,
                    }
                    run_values[key].append(run_vals)
                    
    aggregates = []
    
    # Calculate baseline
    baselines = {}
    for key, runs in run_values.items():
        engine, workload_id, workload, rate = key
        if engine == "flink":
            baselines[(workload_id, rate)] = runs
            
    for key, runs in run_values.items():
        engine, workload_id, workload, rate = key
        if not runs:
            continue
            
        agg = {
            "engine": engine,
            "workload_id": workload_id,
            "workload": workload,
            "rate_per_sec": rate,
            "tail_latency_ms": round(np.mean([r["tail_latency_ms"] for r in runs]), 3),
            "tail_semantic_wait_ms": round(np.mean([r["tail_semantic_wait_ms"] for r in runs]), 3),
            "tail_engine_compute_ms": round(np.mean([r["tail_engine_compute_ms"] for r in runs]), 3),
            "tail_visibility_ms": round(np.mean([r["tail_visibility_ms"] for r in runs]), 3),
        }
        
        # Block bootstrap engine differences
        baseline_key = (workload_id, rate)
        if engine != "flink" and baseline_key in baselines and len(runs) > 1 and len(baselines[baseline_key]) > 1:
            base_runs = baselines[baseline_key]
            n_bootstraps = 1000
            diffs = []
            for _ in range(n_bootstraps):
                boot_runs = np.random.choice(runs, len(runs), replace=True)
                boot_base = np.random.choice(base_runs, len(base_runs), replace=True)
                boot_diff = np.mean([r["tail_latency_ms"] for r in boot_runs]) - np.mean([r["tail_latency_ms"] for r in boot_base])
                diffs.append(boot_diff)
            
            diffs = np.array(diffs)
            agg["effect_size_ms"] = round(np.mean(diffs), 3)
            agg["ci_low"] = round(np.percentile(diffs, 2.5), 3)
            agg["ci_high"] = round(np.percentile(diffs, 97.5), 3)
        else:
            agg["effect_size_ms"] = 0.0
            agg["ci_low"] = 0.0
            agg["ci_high"] = 0.0
            
        aggregates.append(agg)
        
    return aggregates

def main() -> int:
    parser = argparse.ArgumentParser(description="Tail latency component decomposition.")
    parser.add_argument("--results-dir", type=Path, default=Path("experiments/results"))
    parser.add_argument("--csv", type=Path, default=Path("experiments/results/tail_decomposition.csv"))
    args = parser.parse_args()

    rows = load_rows(args.results_dir)
    aggregates = aggregate_rows_with_ci(rows)
    
    args.csv.parent.mkdir(parents=True, exist_ok=True)
    if aggregates:
        with args.csv.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(aggregates[0].keys()))
            writer.writeheader()
            writer.writerows(aggregates)
            
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
