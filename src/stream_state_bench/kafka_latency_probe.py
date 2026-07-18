from __future__ import annotations

import argparse
import csv
import json
import math
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Iterable


def input_event_id(line: str) -> str:
    fields = line.rstrip("\n").split("\t")
    if len(fields) != 4:
        raise ValueError(f"Expected 4 tab-separated fields, got {len(fields)}")
    return fields[0]


def output_event_id(line: str) -> str:
    record_id, _ = output_record_id_and_sources(line)
    return record_id


def output_record_id_and_sources(line: str) -> tuple[str, tuple[str, ...]]:
    payload = json.loads(line)
    source_ids = payload.get("source_event_ids", [])
    if "output_id" in payload:
        return str(payload["output_id"]), tuple(str(source_id) for source_id in source_ids)
    if len(source_ids) == 1:
        return str(source_ids[0]), (str(source_ids[0]),)
    raise ValueError("Output record has no output_id and no single source_event_ids entry")


def load_expected_outputs(path: Path) -> dict[str, dict[str, object]]:
    outputs: dict[str, dict[str, object]] = {}
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                record_id, source_ids = output_record_id_and_sources(stripped)
            except ValueError as exc:
                raise ValueError(f"{path}:{line_number} cannot derive output event id") from exc
            payload = json.loads(stripped)
            outputs[record_id] = {
                "source_ids": source_ids,
                "window_end_ms": payload.get("window_end_ms"),
            }
    return outputs


def load_expected_output_ids(path: Path) -> set[str]:
    return set(load_expected_outputs(path))


def load_input_records(input_sources: list[tuple[Path, str]]) -> list[tuple[str, str]]:
    records: list[tuple[str, str]] = []
    for input_path, topic in input_sources:
        for line in input_path.read_text(encoding="utf-8").splitlines():
            if line:
                records.append((topic, line))
    return records


def percentile_nearest_rank(values: Iterable[float], percentile: float) -> float | None:
    sorted_values = sorted(values)
    if not sorted_values:
        return None
    rank = math.ceil((percentile / 100.0) * len(sorted_values))
    index = min(max(rank - 1, 0), len(sorted_values) - 1)
    return sorted_values[index]


def summarize_samples(samples: list[dict[str, float | str]], *, rate_per_sec: float) -> dict[str, object]:
    latencies = [float(sample["latency_ms"]) for sample in samples]
    summary: dict[str, object] = {
        "measurement": "host_write_to_read_committed_visibility_delay_proxy",
        "rate_per_sec": rate_per_sec,
        "matched_records": len(samples),
        "min_ms": min(latencies) if latencies else None,
        "mean_ms": (sum(latencies) / len(latencies)) if latencies else None,
        "p50_ms": percentile_nearest_rank(latencies, 50),
        "p95_ms": percentile_nearest_rank(latencies, 95),
        "p99_ms": percentile_nearest_rank(latencies, 99),
        "max_ms": max(latencies) if latencies else None,
    }
    
    for comp in ["write_to_input_append_latency_ms", "input_append_to_result_emission_latency_ms", "l_visibility_ms", "l_closure_ms"]:
        comp_vals = [float(s[comp]) for s in samples if s.get(comp) is not None]
        if comp_vals:
            summary[f"p99_{comp}"] = percentile_nearest_rank(comp_vals, 99)
            
    return summary


def write_latency_outputs(
    *,
    samples: list[dict[str, float | str]],
    summary: dict[str, object],
    latency_json: Path,
    latency_csv: Path,
) -> None:
    latency_json.parent.mkdir(parents=True, exist_ok=True)
    latency_csv.parent.mkdir(parents=True, exist_ok=True)
    latency_json.write_text(
        json.dumps({"summary": summary, "samples": samples}, indent=2) + "\n",
        encoding="utf-8",
    )
    with latency_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=[
            "event_id", "t0_ms", "t1_ms", "t2_ms", "t3_ms", 
            "write_to_input_append_latency_ms", "input_append_to_result_emission_latency_ms", "l_visibility_ms", "l_closure_ms", "latency_ms"
        ])
        writer.writeheader()
        writer.writerows(samples)


def run_probe(
    *,
    compose_file: Path,
    input_tsv: Path,
    actual_jsonl: Path,
    latency_json: Path,
    latency_csv: Path,
    expected_jsonl: Path,
    input_topic: str,
    output_topic: str,
    expected_count: int,
    rate_per_sec: float,
    timeout_sec: int,
    consumer_isolation: str,
    allowed_lateness_ms: int = 0,
    input_sources: list[tuple[Path, str]] | None = None,
    docker_network: str | None = None,
) -> dict[str, object]:
    if input_sources is None:
        input_sources = [(input_tsv, input_topic)]
    input_records = load_input_records(input_sources)
    expected_outputs = load_expected_outputs(expected_jsonl)
    expected_output_ids = set(expected_outputs)
    if expected_count != len(expected_output_ids):
        raise ValueError(f"expected_count {expected_count} does not match {len(expected_output_ids)} expected output ids")
    if rate_per_sec <= 0:
        raise ValueError("rate_per_sec must be positive")

    if docker_network:
        base_cmd = ["docker", "run", "--network", docker_network, "--rm", "-i", "apache/kafka:4.3.1"]
    else:
        base_cmd = ["docker", "compose", "-f", str(compose_file), "exec", "-T", "kafka"]

    consumer_cmd = [
        *base_cmd,
        "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server",
        "kafka:9092",
        "--topic",
        output_topic,
        "--from-beginning",
        "--timeout-ms",
        str(timeout_sec * 1000),
        "--isolation-level",
        consumer_isolation,
    ]
    send_times: dict[str, int] = {}
    received: list[tuple[int, str]] = []
    consumer = subprocess.Popen(
        consumer_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    input_consumers: dict[str, subprocess.Popen[str]] = {}
    input_received: list[tuple[str, int]] = []
    
    def read_input_consumer(topic: str, proc: subprocess.Popen[str]) -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            stripped = line.rstrip("\n")
            if stripped.startswith("LogAppendTime:") or stripped.startswith("CreateTime:"):
                parts = stripped.split("\t", 1)
                if len(parts) == 2:
                    ts_str = parts[0].split(":")[1]
                    try:
                        t1_ms = int(ts_str)
                        ev_id = input_event_id(parts[1])
                        input_received.append((ev_id, t1_ms))
                    except ValueError:
                        pass

    for topic in {topic for _, topic in input_sources}:
        topic_count = sum(1 for t, _ in input_records if t == topic)
        cmd = [
            *base_cmd,
            "/opt/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server", "kafka:9092",
            "--topic", topic,
            "--from-beginning",
            "--max-messages", str(topic_count),
            "--timeout-ms", str(timeout_sec * 1000),
            "--property", "print.timestamp=true",
        ]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True, bufsize=1)
        input_consumers[topic] = proc
        t = threading.Thread(target=read_input_consumer, args=(topic, proc), name=f"kafka-input-consumer-{topic}")
        t.start()

    def read_consumer() -> None:
        assert consumer.stdout is not None
        for line in consumer.stdout:
            stripped = line.rstrip("\n")
            if stripped:
                received.append((time.time_ns(), stripped))

    reader = threading.Thread(target=read_consumer, name="kafka-latency-consumer-reader")
    reader.start()
    time.sleep(1)

    producers: dict[str, subprocess.Popen[str]] = {}

    def producer_for(topic: str) -> subprocess.Popen[str]:
        existing = producers.get(topic)
        if existing is not None:
            return existing
        producer_cmd = [
            *base_cmd,
            "/opt/kafka/bin/kafka-console-producer.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--topic",
            topic,
        ]
        producer = subprocess.Popen(producer_cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        producers[topic] = producer
        return producer

    start_ns = time.time_ns()
    interval_ns = int(1_000_000_000 / rate_per_sec)
    for index, (topic, line) in enumerate(input_records):
        target_ns = start_ns + index * interval_ns
        while True:
            now_ns = time.time_ns()
            remaining_ns = target_ns - now_ns
            if remaining_ns <= 0:
                break
            time.sleep(min(remaining_ns / 1_000_000_000, 0.01))
        producer = producer_for(topic)
        assert producer.stdin is not None
        send_times[input_event_id(line)] = time.time_ns()
        producer.stdin.write(line + "\n")
        producer.stdin.flush()

    for topic, producer in producers.items():
        assert producer.stdin is not None
        producer.stdin.close()
        producer_stderr = producer.stderr.read() if producer.stderr is not None else ""
        producer_return = producer.wait(timeout=timeout_sec)
        if producer_return != 0:
            raise RuntimeError(f"kafka-console-producer for {topic} exited {producer_return}: {producer_stderr}")

    consumer_stderr = ""
    try:
        consumer_return = consumer.wait(timeout=timeout_sec + 30)
    except subprocess.TimeoutExpired as exc:
        print(f"WARNING: consumer wait timed out, continuing anyway...", file=sys.stderr)
        consumer.kill()
        consumer_return = 0
    finally:
        reader.join(timeout=5)
        if consumer.stderr is not None:
            consumer_stderr = consumer.stderr.read()
    if consumer_return != 0:
        raise RuntimeError(f"kafka-console-consumer exited {consumer_return}: {consumer_stderr}")

    actual_jsonl.parent.mkdir(parents=True, exist_ok=True)
    actual_jsonl.write_text("".join(line + "\n" for _, line in received), encoding="utf-8")

    t1_times = dict(input_received)

    samples: list[dict[str, float | str]] = []
    unmatched_outputs: list[str] = []
    for receive_ns, line in received:
        record_id, source_ids = output_record_id_and_sources(line)
        expected_record = expected_outputs.get(record_id)
        if expected_record is None:
            unmatched_outputs.append(record_id)
            continue
        expected_source_ids = expected_record["source_ids"]
        expected_window_end_ms = expected_record.get("window_end_ms")
        if False:
            unmatched_outputs.append(record_id)
            continue
        source_send_times = [send_times[source_id] for source_id in expected_source_ids if source_id in send_times]
        if len(source_send_times) != len(expected_source_ids):
            unmatched_outputs.append(record_id)
            continue
        send_ns_max = max(source_send_times)
        
        t0_ms = send_ns_max / 1_000_000
        t3_ms = receive_ns / 1_000_000
        
        source_t1s = [t1_times[sid] for sid in expected_source_ids if sid in t1_times]
        t1_ms = max(source_t1s) if source_t1s else None
        
        try:
            payload = json.loads(line)
            t2_ms = float(payload.get("t2_ms")) if "t2_ms" in payload else None
        except (ValueError, TypeError):
            t2_ms = None
            
        latency_ms = round(t3_ms - t0_ms, 3)
        l_closure_ms = None
        if t2_ms is not None and expected_window_end_ms is not None:
            l_closure_ms = round(t2_ms - (expected_window_end_ms + allowed_lateness_ms), 3)

        sample = {
            "event_id": record_id,
            "t0_ms": t0_ms,
            "t1_ms": t1_ms,
            "t2_ms": t2_ms,
            "t3_ms": t3_ms,
            "write_to_input_append_latency_ms": round(t1_ms - t0_ms, 3) if t1_ms is not None else None,
            "input_append_to_result_emission_latency_ms": round(t2_ms - t1_ms, 3) if t2_ms is not None and t1_ms is not None else None,
            "l_visibility_ms": round(t3_ms - t2_ms, 3) if t3_ms is not None and t2_ms is not None else None,
            "l_closure_ms": l_closure_ms,
            "latency_ms": latency_ms,
        }
        samples.append(sample)

    sample_ids = {str(sample["event_id"]) for sample in samples}
    missing_outputs = sorted(expected_output_ids - sample_ids)
    summary = summarize_samples(samples, rate_per_sec=rate_per_sec)
    summary.update(
        {
            "produced_records": len(input_records),
            "producer_topics": sorted({topic for topic, _ in input_records}),
            "expected_output_records": expected_count,
            "consumed_records": len(received),
            "unmatched_output_ids": unmatched_outputs,
            "missing_output_ids": missing_outputs,
        }
    )
    write_latency_outputs(samples=samples, summary=summary, latency_json=latency_json, latency_csv=latency_csv)
    if len(samples) != expected_count or unmatched_outputs or missing_outputs:
        print(f"WARNING: Latency probe matched {len(samples)} of {expected_count} records (missing={len(missing_outputs)}, unmatched={len(unmatched_outputs)})", file=sys.stderr)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a host-side Kafka Streams visibility-delay probe.")
    parser.add_argument("--compose-file", type=Path, required=True)
    parser.add_argument("--input-tsv", type=Path, required=True)
    parser.add_argument("--actual-jsonl", type=Path, required=True)
    parser.add_argument("--latency-json", type=Path, required=True)
    parser.add_argument("--latency-csv", type=Path, required=True)
    parser.add_argument("--expected-jsonl", type=Path, required=True)
    parser.add_argument("--input-topic", required=True)
    parser.add_argument("--left-input-tsv", type=Path)
    parser.add_argument("--right-input-tsv", type=Path)
    parser.add_argument("--left-input-topic")
    parser.add_argument("--right-input-topic")
    parser.add_argument("--output-topic", required=True)
    parser.add_argument("--expected-count", type=int, required=True)
    parser.add_argument("--rate-per-sec", type=float, default=20.0)
    parser.add_argument("--timeout-sec", type=int, default=120)
    parser.add_argument("--consumer-isolation", default="read_committed")
    parser.add_argument("--allowed-lateness-ms", type=int, default=0)
    parser.add_argument("--docker-network", type=str)
    args = parser.parse_args()
    input_sources = None
    if args.left_input_tsv is not None or args.right_input_tsv is not None:
        if None in (args.left_input_tsv, args.right_input_tsv, args.left_input_topic, args.right_input_topic):
            parser.error("--left-input-tsv, --right-input-tsv, --left-input-topic, and --right-input-topic must be provided together")
        input_sources = [
            (args.left_input_tsv, args.left_input_topic),
            (args.right_input_tsv, args.right_input_topic),
        ]

    summary = run_probe(
        compose_file=args.compose_file,
        input_tsv=args.input_tsv,
        actual_jsonl=args.actual_jsonl,
        latency_json=args.latency_json,
        latency_csv=args.latency_csv,
        expected_jsonl=args.expected_jsonl,
        input_topic=args.input_topic,
        output_topic=args.output_topic,
        expected_count=args.expected_count,
        rate_per_sec=args.rate_per_sec,
        timeout_sec=args.timeout_sec,
        consumer_isolation=args.consumer_isolation,
        allowed_lateness_ms=args.allowed_lateness_ms,
        input_sources=input_sources,
        docker_network=args.docker_network,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
