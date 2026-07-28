import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path
import pandas as pd
import numpy as np
from scipy import stats

from stream_state_bench.summarize_engine_results import engine_from_dir, workload_id_from_dir

def load_rows(results_dir: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
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
                "run_label": metadata.get("run_label", ""),
                "rate_per_sec": summary["rate_per_sec"],
                "produced_records": summary.get("produced_records", ""),
                "expected_output_records": summary.get(
                    "expected_output_records",
                    metadata.get("expected_output_records", summary["matched_records"]),
                ),
                "matched_records": summary["matched_records"],
                "consumed_records": summary["consumed_records"],
                "passed": verification["passed"],
                "p50_ms": summary["p50_ms"],
                "p95_ms": summary["p95_ms"],
                "p99_ms": summary["p99_ms"],
                "max_ms": summary["max_ms"],
                "p99_write_to_input_append_latency_ms": summary.get("p99_write_to_input_append_latency_ms", ""),
                "p99_input_append_to_result_emission_latency_ms": summary.get("p99_input_append_to_result_emission_latency_ms", ""),
                "p99_l_visibility_ms": summary.get("p99_l_visibility_ms", ""),
                "p99_l_closure_ms": summary.get("p99_l_closure_ms", ""),
                "result_dir": str(run_dir),
            }
        )
    return rows


def aggregate_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str, str, float], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        key = (
            str(row["engine"]),
            str(row["workload_id"]),
            str(row["workload"]),
            float(row["rate_per_sec"]),
        )
        grouped[key].append(row)

    aggregates: list[dict[str, object]] = []
    
    # Pre-compute to calculate effect sizes between engines later
    median_cache = {}
    
    for (engine, workload_id, workload, rate), group in sorted(grouped.items()):
        all_dfs = []
        for row in group:
            samples_csv = Path(row["result_dir"]) / "latency_samples.csv"
            if samples_csv.exists():
                df = pd.read_csv(samples_csv)
                # Warm-up policy: exclude first 5% of samples (by t0_ms)
                if 't0_ms' in df.columns:
                    df = df.sort_values('t0_ms')
                    warmup_idx = int(len(df) * 0.05)
                    df = df.iloc[warmup_idx:]
                
                # Outlier policy: exclude top 0.1% of latency_ms values
                if 'latency_ms' in df.columns and len(df) > 0:
                    cutoff = np.percentile(df['latency_ms'].dropna(), 99.9)
                    df = df[df['latency_ms'] <= cutoff]
                
                all_dfs.append(df)
        
        agg = {
            "engine": engine,
            "workload_id": workload_id,
            "workload": workload,
            "rate_per_sec": rate,
            "runs": len(group),
            "all_passed": all(bool(row["passed"]) for row in group),
        }
        
        if all_dfs:
            combined = pd.concat(all_dfs, ignore_index=True)
            data = combined['latency_ms'].dropna().values
            
            if len(data) > 0:
                p50 = np.percentile(data, 50)
                p95 = np.percentile(data, 95)
                p99 = np.percentile(data, 99)
                
                # Bootstrap CIs (n_resamples=1000)
                # Cap the data array to 20,000 samples to keep it fast
                if len(data) > 20000:
                    bdata = np.random.choice(data, 20000, replace=False)
                else:
                    bdata = data

                def get_ci(stat_func):
                    if len(bdata) < 2: return 0.0, 0.0
                    try:
                        res = stats.bootstrap((bdata,), stat_func, confidence_level=0.95, n_resamples=100, method='percentile')
                        return float(res.confidence_interval.low), float(res.confidence_interval.high)
                    except Exception as e:
                        print(f"Error bootstrapping: {e}")
                        return 0.0, 0.0
                
                ci_p50 = get_ci(np.median)
                ci_p95 = get_ci(lambda x: np.percentile(x, 95))
                ci_p99 = get_ci(lambda x: np.percentile(x, 99))
                
                agg.update({
                    "median_p50_ms": round(p50, 3),
                    "p50_ci_low": round(ci_p50[0], 3),
                    "p50_ci_high": round(ci_p50[1], 3),
                    
                    "median_p95_ms": round(p95, 3),
                    "p95_ci_low": round(ci_p95[0], 3),
                    "p95_ci_high": round(ci_p95[1], 3),
                    
                    "median_p99_ms": round(p99, 3),
                    "p99_ci_low": round(ci_p99[0], 3),
                    "p99_ci_high": round(ci_p99[1], 3),
                })
                median_cache[(engine, workload_id, rate)] = p50
            else:
                agg.update({
                    "median_p50_ms": 0.0, "p50_ci_low": 0.0, "p50_ci_high": 0.0,
                    "median_p95_ms": 0.0, "p95_ci_low": 0.0, "p95_ci_high": 0.0,
                    "median_p99_ms": 0.0, "p99_ci_low": 0.0, "p99_ci_high": 0.0,
                })
        else:
            # Fallback to older method if latency_samples.csv not found
            p99_values = [float(row["p99_ms"]) for row in group]
            p95_values = [float(row["p95_ms"]) for row in group]
            p50_values = [float(row["p50_ms"]) for row in group]
            
            p50_med = statistics.median(p50_values) if p50_values else 0.0
            agg.update({
                "median_p50_ms": round(p50_med, 3),
                "p50_ci_low": 0.0, "p50_ci_high": 0.0,
                "median_p95_ms": round(statistics.median(p95_values), 3) if p95_values else 0.0,
                "p95_ci_low": 0.0, "p95_ci_high": 0.0,
                "median_p99_ms": round(statistics.median(p99_values), 3) if p99_values else 0.0,
                "p99_ci_low": 0.0, "p99_ci_high": 0.0,
            })
            median_cache[(engine, workload_id, rate)] = p50_med
        
        # Add decomposed latencies if they exist
        for comp in ["write_to_input_append_latency", "input_append_to_result_emission_latency", "l_visibility", "l_closure"]:
            vals = [float(row[f"p99_{comp}_ms"]) for row in group if row.get(f"p99_{comp}_ms")]
            if vals:
                agg[f"mean_p99_{comp}_ms"] = round(sum(vals) / len(vals), 3)
            else:
                agg[f"mean_p99_{comp}_ms"] = ""
                
        aggregates.append(agg)
        
    # Calculate effect size (difference in medians compared to Flink as baseline)
    for agg in aggregates:
        base_engine = "flink"
        key = (base_engine, agg["workload_id"], agg["rate_per_sec"])
        if agg["engine"] != base_engine and key in median_cache:
            diff = agg["median_p50_ms"] - median_cache[key]
            agg["effect_size_ms"] = round(diff, 3)
            # Cohen's d is typically not used for heavily skewed latency, so we report absolute median difference
        else:
            agg["effect_size_ms"] = 0.0

    return aggregates


def write_csv(rows: list[dict[str, object]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(rows: list[dict[str, object]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "| Engine | Workload ID | Workload | Run label | Rate | Produced | Expected | Matched | Passed | p50 ms | p95 ms | p99 ms | p99 write_to_input_append_latency | p99 input_append_to_result_emission_latency | p99 l_visibility | p99 l_closure |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            "| {engine} | {workload_id} | {workload} | {run_label} | {rate_per_sec} | {produced_records} | {expected_output_records} | {matched_records} | {passed} | {p50_ms} | {p95_ms} | {p99_ms} | {p99_write_to_input_append_latency_ms} | {p99_input_append_to_result_emission_latency_ms} | {p99_l_visibility_ms} | {p99_l_closure_ms} |".format(
                **row
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_aggregate_markdown(rows: list[dict[str, object]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "| Engine | Workload ID | Workload | Rate | Runs | All passed | p50 ms (95% CI) | Effect Size (ms) | p95 ms (95% CI) | p99 ms (95% CI) | Mean p99 write_to_input_append_latency | Mean p99 input_append_to_result_emission_latency | Mean p99 l_visibility | Mean p99 l_closure |",
        "| --- | --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        fmt_args = dict(row)
        for k in ['mean_p99_write_to_input_append_latency_ms', 'mean_p99_input_append_to_result_emission_latency_ms', 'mean_p99_l_visibility_ms', 'mean_p99_l_closure_ms']:
            if k not in fmt_args:
                fmt_args[k] = ''
        
        fmt_args["p50_str"] = f"{fmt_args['median_p50_ms']} ({fmt_args['p50_ci_low']}-{fmt_args['p50_ci_high']})"
        fmt_args["p95_str"] = f"{fmt_args['median_p95_ms']} ({fmt_args['p95_ci_low']}-{fmt_args['p95_ci_high']})"
        fmt_args["p99_str"] = f"{fmt_args['median_p99_ms']} ({fmt_args['p99_ci_low']}-{fmt_args['p99_ci_high']})"
        
        lines.append(
            "| {engine} | {workload_id} | {workload} | {rate_per_sec} | {runs} | {all_passed} | {p50_str} | {effect_size_ms} | {p95_str} | {p99_str} | {mean_p99_write_to_input_append_latency_ms} | {mean_p99_input_append_to_result_emission_latency_ms} | {mean_p99_l_visibility_ms} | {mean_p99_l_closure_ms} |".format(
                **fmt_args
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize host-side latency result files.")
    parser.add_argument("--results-dir", type=Path, default=Path("experiments/results"))
    parser.add_argument("--csv", type=Path, default=Path("experiments/results/latency_summary.csv"))
    parser.add_argument("--markdown", type=Path, default=Path("experiments/results/latency_summary.md"))
    parser.add_argument("--aggregate-csv", type=Path, default=Path("experiments/results/latency_aggregate_summary.csv"))
    parser.add_argument("--aggregate-markdown", type=Path, default=Path("experiments/results/latency_aggregate_summary.md"))
    args = parser.parse_args()

    rows = load_rows(args.results_dir)
    aggregates = aggregate_rows(rows)
    write_csv(rows, args.csv)
    write_markdown(rows, args.markdown)
    write_csv(aggregates, args.aggregate_csv)
    write_aggregate_markdown(aggregates, args.aggregate_markdown)
    print(f"Wrote {args.csv}, {args.markdown}, {args.aggregate_csv}, and {args.aggregate_markdown}")
    return 0 if rows and all(bool(row["passed"]) for row in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
