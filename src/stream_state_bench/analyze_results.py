from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize local semantic harness results.")
    parser.add_argument("input", type=Path, nargs="?", default=Path("experiments/results/local_semantic_results.json"))
    parser.add_argument("--csv", type=Path, default=Path("experiments/results/local_semantic_summary.csv"))
    parser.add_argument("--md", type=Path, default=Path("experiments/results/local_semantic_summary.md"))
    args = parser.parse_args()

    data = json.loads(args.input.read_text(encoding="utf-8"))
    rows = []
    for result in data["results"]:
        verification = result["verification"]
        rows.append(
            {
                "workload": result["workload"],
                "events": result["events"],
                "keys": result["keys"],
                "seed": result["seed"],
                "output_records": result["output_records"],
                "passed": verification["passed"],
                "missing_count": verification["missing_count"],
                "unexpected_count": verification["unexpected_count"],
                "duplicate_count": verification["duplicate_count"],
            }
        )

    args.csv.parent.mkdir(parents=True, exist_ok=True)
    with args.csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    lines = [
        "# Local semantic harness summary",
        "",
        "This summary covers only the in-process semantic harness. It does not contain Flink or Kafka Streams measurements.",
        "",
        "| Workload | Events | Keys | Seed | Output records | Verification | Missing | Unexpected | Duplicates |",
        "| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            f"| {row['workload']} | {row['events']} | {row['keys']} | {row['seed']} | "
            f"{row['output_records']} | {row['passed']} | {row['missing_count']} | "
            f"{row['unexpected_count']} | {row['duplicate_count']} |"
        )
    args.md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {args.csv} and {args.md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
