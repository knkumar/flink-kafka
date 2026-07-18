"""Poll `docker stats` for a fixed set of containers and append rows to a CSV.

This is a best-effort resource-utilization sampler: it uses the Docker CLI's
own cgroup-derived stats, not a dedicated profiler, and it shares the host
with unrelated containers. It is not a substitute for isolated hardware
profiling; see docs/reproducibility.md for the caveat this script exists to
support.
"""
from __future__ import annotations

import argparse
import csv
import subprocess
import time
from pathlib import Path

def get_changelog_repartition_bytes(kafka_container: str) -> int:
    """Measure the total size of changelog and repartition topics in Kafka."""
    try:
        cmd = [
            "docker", "exec", kafka_container, "sh", "-c",
            "du -sb /tmp/kraft-combined-logs/*changelog* /tmp/kraft-combined-logs/*repartition* /var/lib/kafka/data/*changelog* /var/lib/kafka/data/*repartition* 2>/dev/null | awk '{sum += $1} END {print sum}'"
        ]
        import subprocess
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        res = out.stdout.strip()
        if res:
            return int(res)
    except Exception:
        pass
    return 0

def poll_once(containers: list[str]) -> list[dict]:
    fmt = "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}"
    out = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", fmt, *containers],
        capture_output=True,
        text=True,
        timeout=30,
    )
    rows = []
    ts = time.time()
    kafka_container = next((c for c in containers if "kafka" in c and "streams" not in c), None)
    kafka_bytes = 0
    if kafka_container:
        kafka_bytes = get_changelog_repartition_bytes(kafka_container)

    for line in out.stdout.strip().splitlines():
        parts = line.split("\t")
        if len(parts) != 5:
            continue
        name, cpu_perc, mem_usage, net_io, block_io = parts
        rows.append(
            {
                "timestamp": ts,
                "container": name,
                "cpu_perc": cpu_perc,
                "mem_usage": mem_usage,
                "net_io": net_io,
                "block_io": block_io,
                "changelog_repartition_bytes": kafka_bytes if name == kafka_container else 0,
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--containers", nargs="+", required=True)
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--interval-sec", type=float, default=15.0)
    parser.add_argument("--duration-sec", type=float, default=0.0, help="0 means run until killed")
    args = parser.parse_args()

    out_path = Path(args.output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "timestamp", "container", "cpu_perc", "mem_usage", "net_io", "block_io",
        "changelog_repartition_bytes"
    ]
    write_header = not out_path.exists()
    start = time.time()
    with out_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        handle.flush()
        while True:
            try:
                rows = poll_once(args.containers)
                for row in rows:
                    writer.writerow(row)
                handle.flush()
            except Exception as exc:  # noqa: BLE001 - best effort sampler
                print(f"resource_monitor: poll error: {exc}")
            if args.duration_sec and (time.time() - start) >= args.duration_sec:
                break
            time.sleep(args.interval_sec)


if __name__ == "__main__":
    main()
