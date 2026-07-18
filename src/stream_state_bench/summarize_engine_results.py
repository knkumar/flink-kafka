from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def engine_from_dir(name: str) -> str:
    if name.startswith("kafka_streams_"):
        return "kafka_streams"
    if name.startswith("flink_"):
        return "flink"
    return "unknown"


def workload_id_from_dir(name: str) -> str:
    parts = name.split("_")
    for part in parts:
        if part.startswith("w") and part[1:].isdigit():
            return part
    return ""


def load_rows(results_dir: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for verification_path in sorted(results_dir.glob("*/verification.json")):
        run_dir = verification_path.parent
        if not (run_dir.name.startswith("kafka_streams_w") or run_dir.name.startswith("flink_w")):
            continue
        verification = json.loads(verification_path.read_text(encoding="utf-8"))
        metadata_path = run_dir / "run_metadata.json"
        metadata = json.loads(metadata_path.read_text(encoding="utf-8")) if metadata_path.exists() else {}
        report = verification["verification"]
        rows.append(
            {
                "engine": engine_from_dir(run_dir.name),
                "workload_id": workload_id_from_dir(run_dir.name),
                "workload": verification["workload"],
                "run_label": metadata.get("run_label", ""),
                "events": verification["events"],
                "keys": verification["keys"],
                "seed": verification["seed"],
                "start_ms": verification.get("start_ms", 0),
                "expected_count": report["expected_count"],
                "actual_count": report["actual_count"],
                "missing_count": report["missing_count"],
                "unexpected_count": report["unexpected_count"],
                "duplicate_count": report["duplicate_count"],
                "passed": report["passed"],
            }
        )
    return rows


def write_csv(rows: list[dict[str, object]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "engine",
        "workload_id",
        "workload",
        "run_label",
        "events",
        "keys",
        "seed",
        "start_ms",
        "expected_count",
        "actual_count",
        "missing_count",
        "unexpected_count",
        "duplicate_count",
        "passed",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(rows: list[dict[str, object]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "| Engine | Workload ID | Workload | Run label | Expected | Actual | Missing | Unexpected | Duplicates | Passed |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in rows:
        lines.append(
            "| {engine} | {workload_id} | {workload} | {run_label} | {expected_count} | {actual_count} | {missing_count} | {unexpected_count} | {duplicate_count} | {passed} |".format(
                **row
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize external engine correctness result files.")
    parser.add_argument("--results-dir", type=Path, default=Path("experiments/results"))
    parser.add_argument("--csv", type=Path, default=Path("experiments/results/engine_correctness_summary.csv"))
    parser.add_argument("--markdown", type=Path, default=Path("experiments/results/engine_correctness_summary.md"))
    args = parser.parse_args()

    rows = load_rows(args.results_dir)
    write_csv(rows, args.csv)
    write_markdown(rows, args.markdown)
    print(f"Wrote {args.csv} and {args.markdown}")
    return 0 if rows and all(bool(row["passed"]) for row in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
