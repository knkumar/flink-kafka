from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

from .summarize_engine_results import engine_from_dir, workload_id_from_dir


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
                "p99_t1_t0_ms": summary.get("p99_t1_t0_ms", ""),
                "p99_t2_t1_ms": summary.get("p99_t2_t1_ms", ""),
                "p99_t3_t2_ms": summary.get("p99_t3_t2_ms", ""),
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
    for (engine, workload_id, workload, rate), group in sorted(grouped.items()):
        p99_values = [float(row["p99_ms"]) for row in group]
        p95_values = [float(row["p95_ms"]) for row in group]
        aggregates.append(
            {
                "engine": engine,
                "workload_id": workload_id,
                "workload": workload,
                "rate_per_sec": rate,
                "runs": len(group),
                "all_passed": all(bool(row["passed"]) for row in group),
                "mean_p95_ms": round(sum(p95_values) / len(p95_values), 3),
                "min_p95_ms": min(p95_values),
                "max_p95_ms": max(p95_values),
                "mean_p99_ms": round(sum(p99_values) / len(p99_values), 3),
                "min_p99_ms": min(p99_values),
                "max_p99_ms": max(p99_values),
            }
        )
        
        # Add decomposed latencies if they exist
        for comp in ["t1_t0", "t2_t1", "t3_t2"]:
            vals = [float(row[f"p99_{comp}_ms"]) for row in group if row.get(f"p99_{comp}_ms")]
            if vals:
                aggregates[-1][f"mean_p99_{comp}_ms"] = round(sum(vals) / len(vals), 3)
            else:
                aggregates[-1][f"mean_p99_{comp}_ms"] = ""
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
        "| Engine | Workload ID | Workload | Run label | Rate | Produced | Expected | Matched | Passed | p50 ms | p95 ms | p99 ms | p99 t1-t0 | p99 t2-t1 | p99 t3-t2 |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            "| {engine} | {workload_id} | {workload} | {run_label} | {rate_per_sec} | {produced_records} | {expected_output_records} | {matched_records} | {passed} | {p50_ms} | {p95_ms} | {p99_ms} | {p99_t1_t0_ms} | {p99_t2_t1_ms} | {p99_t3_t2_ms} |".format(
                **row
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_aggregate_markdown(rows: list[dict[str, object]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "| Engine | Workload ID | Workload | Rate | Runs | All passed | Mean p95 ms | Min p95 ms | Max p95 ms | Mean p99 ms | Min p99 ms | Max p99 ms | Mean p99 t1-t0 | Mean p99 t2-t1 | Mean p99 t3-t2 |",
        "| --- | --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        fmt_args = dict(row)
        for k in ['mean_p99_t1_t0_ms', 'mean_p99_t2_t1_ms', 'mean_p99_t3_t2_ms']:
            if k not in fmt_args:
                fmt_args[k] = ''
        lines.append(
            "| {engine} | {workload_id} | {workload} | {rate_per_sec} | {runs} | {all_passed} | {mean_p95_ms} | {min_p95_ms} | {max_p95_ms} | {mean_p99_ms} | {min_p99_ms} | {max_p99_ms} | {mean_p99_t1_t0_ms} | {mean_p99_t2_t1_ms} | {mean_p99_t3_t2_ms} |".format(
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
