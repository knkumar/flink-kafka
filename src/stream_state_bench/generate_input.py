from __future__ import annotations

import argparse
import json
from pathlib import Path

from .events import Event, paired_join_events, uniform_events
from .reference import WORKLOADS, reference_outputs


def event_line(event: Event) -> str:
    return f"{event.event_id}\t{event.key}\t{event.payload}\t{event.event_time_ms}\n"


def write_inputs(
    *,
    workload: str,
    events: int,
    keys: int,
    seed: int,
    input_tsv: Path,
    expected_jsonl: Path,
    start_ms: int = 0,
    left_input_tsv: Path | None = None,
    right_input_tsv: Path | None = None,
) -> None:
    input_tsv.parent.mkdir(parents=True, exist_ok=True)
    expected_jsonl.parent.mkdir(parents=True, exist_ok=True)

    if workload == "stream_stream_join":
        left, right = paired_join_events(events, key_count=keys, seed=seed, start_ms=start_ms)
        input_tsv.write_text("".join(event_line(event) for event in [*left, *right]), encoding="utf-8")
        if left_input_tsv is not None:
            left_input_tsv.parent.mkdir(parents=True, exist_ok=True)
            left_input_tsv.write_text("".join(event_line(event) for event in left), encoding="utf-8")
        if right_input_tsv is not None:
            right_input_tsv.parent.mkdir(parents=True, exist_ok=True)
            right_input_tsv.write_text("".join(event_line(event) for event in right), encoding="utf-8")
    else:
        generated = uniform_events(events, key_count=keys, seed=seed, start_ms=start_ms)
        input_tsv.write_text("".join(event_line(event) for event in generated), encoding="utf-8")

    expected = reference_outputs(workload, events=events, keys=keys, seed=seed, start_ms=start_ms)
    expected_jsonl.write_text(
        "".join(
            json.dumps(
                {
                    "output_id": record.output_id,
                    "key": record.key,
                    "window_start_ms": record.window_start_ms,
                    "window_end_ms": record.window_end_ms,
                    "value": record.value,
                    "source_event_ids": list(record.source_event_ids),
                },
                separators=(",", ":"),
            )
            + "\n"
            for record in expected
        ),
        encoding="utf-8",
    )


def write_identity_inputs(*, events: int, keys: int, seed: int, input_tsv: Path, expected_jsonl: Path) -> None:
    write_inputs(workload="identity", events=events, keys=keys, seed=seed, input_tsv=input_tsv, expected_jsonl=expected_jsonl)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate deterministic benchmark input files.")
    parser.add_argument("--workload", choices=WORKLOADS, default="identity")
    parser.add_argument("--events", type=int, default=1_000)
    parser.add_argument("--keys", type=int, default=100)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--start-ms", type=int, default=0)
    parser.add_argument("--input-tsv", type=Path, default=Path("experiments/results/w1_input.tsv"))
    parser.add_argument("--left-input-tsv", type=Path)
    parser.add_argument("--right-input-tsv", type=Path)
    parser.add_argument("--expected-jsonl", type=Path, default=Path("experiments/results/w1_expected.jsonl"))
    args = parser.parse_args()

    write_inputs(
        workload=args.workload,
        events=args.events,
        keys=args.keys,
        seed=args.seed,
        start_ms=args.start_ms,
        input_tsv=args.input_tsv,
        left_input_tsv=args.left_input_tsv,
        right_input_tsv=args.right_input_tsv,
        expected_jsonl=args.expected_jsonl,
    )
    print(f"Wrote {args.input_tsv} and {args.expected_jsonl}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
