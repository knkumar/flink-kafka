from __future__ import annotations

import argparse
import json
from pathlib import Path

from .reference import WORKLOADS, reference_outputs
from .verify import verify_records
from .workloads import OutputRecord


def load_jsonl_records(path: Path) -> list[OutputRecord]:
    records: list[OutputRecord] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            payload = json.loads(stripped)
            try:
                records.append(
                    OutputRecord(
                        output_id=payload["output_id"],
                        key=payload["key"],
                        window_start_ms=payload.get("window_start_ms"),
                        window_end_ms=payload.get("window_end_ms"),
                        value=payload["value"],
                        source_event_ids=tuple(payload["source_event_ids"]),
                    )
                )
            except KeyError as exc:
                raise ValueError(f"{path}:{line_number} missing field {exc.args[0]}") from exc
    return records


def verify_external_output(
    *,
    workload: str,
    actual_jsonl: Path,
    events: int,
    keys: int,
    seed: int,
    start_ms: int = 0,
) -> dict[str, object]:
    expected = reference_outputs(workload, events=events, keys=keys, seed=seed, start_ms=start_ms)
    actual = load_jsonl_records(actual_jsonl)
    report = verify_records(expected, actual)
    return {
        "workload": workload,
        "actual_jsonl": str(actual_jsonl),
        "events": events,
        "keys": keys,
        "seed": seed,
        "start_ms": start_ms,
        "verification": report.to_dict(),
    }



def parse_scale(value: str) -> int:
    v = value.lower()
    if v.endswith("k"):
        return int(float(v[:-1]) * 1000)
    elif v.endswith("m"):
        return int(float(v[:-1]) * 1000000)
    elif v.endswith("b"):
        return int(float(v[:-1]) * 1000000000)
    return int(v)

def main() -> int:
    parser = argparse.ArgumentParser(description="Verify external engine output JSONL against reference semantics.")
    parser.add_argument("--workload", choices=WORKLOADS, required=True)
    parser.add_argument("--actual-jsonl", type=Path, required=True)
    parser.add_argument("--events", type=parse_scale, default=1000)
    parser.add_argument("--keys", type=parse_scale, default=100)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--start-ms", type=int, default=0)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()

    result = verify_external_output(
        workload=args.workload,
        actual_jsonl=args.actual_jsonl,
        events=args.events,
        keys=args.keys,
        seed=args.seed,
        start_ms=args.start_ms,
    )
    text = json.dumps(result, indent=2) + "\n"
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text, encoding="utf-8")
    print(text, end="")
    return 0 if result["verification"]["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
