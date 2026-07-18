from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from .reference import WORKLOADS, reference_outputs
from .verify import verify_records


def run_once(name: str, *, events: int, keys: int, seed: int) -> dict[str, object]:
    start_ns = time.perf_counter_ns()
    expected = reference_outputs(name, events=events, keys=keys, seed=seed)
    process_end_ns = time.perf_counter_ns()

    # The local harness uses the same deterministic semantic implementation as the reference.
    # A real engine adapter must replace this call with records read from the output topic.
    actual = reference_outputs(name, events=events, keys=keys, seed=seed)
    visibility_end_ns = time.perf_counter_ns()

    verification = verify_records(expected, actual)
    return {
        "workload": name,
        "engine": "local_semantic_harness",
        "events": events,
        "keys": keys,
        "seed": seed,
        "output_records": len(actual),
        "reference_runtime_ms": round((process_end_ns - start_ns) / 1_000_000, 3),
        "verification_runtime_ms": round((visibility_end_ns - process_end_ns) / 1_000_000, 3),
        "verification": verification.to_dict(),
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
    parser = argparse.ArgumentParser(description="Run the local semantic benchmark harness.")
    parser.add_argument("--workload", choices=WORKLOADS + ("all",), default="all")
    parser.add_argument("--events", type=parse_scale, default=1000)
    parser.add_argument("--keys", type=parse_scale, default=100)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--out", type=Path, default=Path("experiments/results/local_semantic_results.json"))
    args = parser.parse_args()

    workloads = WORKLOADS if args.workload == "all" else (args.workload,)
    results = [run_once(name, events=args.events, keys=args.keys, seed=args.seed) for name in workloads]

    payload = {
        "schema_version": 1,
        "note": "Local semantic harness only; no Flink or Kafka Streams runtime was executed.",
        "results": results,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2))
    return 0 if all(result["verification"]["passed"] for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
