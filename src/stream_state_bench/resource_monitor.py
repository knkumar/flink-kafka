import argparse
import csv
import subprocess
import time
import json
from pathlib import Path

def get_changelog_repartition_bytes(kafka_container: str) -> int:
    try:
        cmd = [
            "docker", "exec", kafka_container, "sh", "-c",
            "du -sb /tmp/kraft-combined-logs/*changelog* /tmp/kraft-combined-logs/*repartition* /var/lib/kafka/data/*changelog* /var/lib/kafka/data/*repartition* 2>/dev/null | awk '{sum += $1} END {print sum}'"
        ]
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        res = out.stdout.strip()
        if res:
            return int(res)
    except Exception:
        pass
    return 0

def get_flink_checkpoint_stats(container: str) -> dict:
    try:
        out = subprocess.run(["docker", "exec", container, "curl", "-s", "http://localhost:8081/jobs"], capture_output=True, text=True, timeout=5)
        jobs = json.loads(out.stdout)
        if not jobs.get("jobs"): return {}
        job_id = jobs["jobs"][0]["id"]
        out = subprocess.run(["docker", "exec", container, "curl", "-s", f"http://localhost:8081/jobs/{job_id}/checkpoints"], capture_output=True, text=True, timeout=5)
        cp_info = json.loads(out.stdout)
        latest = cp_info.get("latest", {}).get("completed")
        if latest:
            return {
                "flink_cp_size": latest.get("state_size", 0),
                "flink_cp_duration": latest.get("end_to_end_duration", 0),
                "flink_cp_read": latest.get("metrics", {}).get("read_bytes", 0), # Flink state size / checkpoint bytes?
                "flink_cp_write": latest.get("metrics", {}).get("write_bytes", 0),
            }
    except Exception:
        pass
    return {}

def get_rocksdb_size(container: str) -> int:
    try:
        if "streams" in container:
            cmd = ["docker", "exec", container, "sh", "-c", "du -sb /tmp/kafka-streams 2>/dev/null | awk '{sum += $1} END {print sum}'"]
        elif "flink" in container:
            cmd = ["docker", "exec", container, "sh", "-c", "find /tmp /var/tmp -name '*rocksdb*' -type d -exec du -sb {} + 2>/dev/null | awk '{sum += $1} END {print sum}'"]
        else:
            return 0
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
    kafka_container = next((c for c in containers if "kafka" in c and "streams" not in c and c.endswith("1")), None)
    kafka_bytes = get_changelog_repartition_bytes(kafka_container) if kafka_container else 0

    flink_container = next((c for c in containers if "flink-identity" in c), None)
    flink_cp = get_flink_checkpoint_stats(flink_container) if flink_container else {}

    for line in out.stdout.strip().splitlines():
        parts = line.split("\t")
        if len(parts) != 5:
            continue
        name, cpu_perc, mem_usage, net_io, block_io = parts
        
        # Determine rocksdb size
        rocksdb_sz = get_rocksdb_size(name)
        
        rows.append(
            {
                "timestamp": ts,
                "container": name,
                "cpu_perc": cpu_perc,
                "mem_usage": mem_usage,
                "net_io": net_io,
                "block_io": block_io,
                "changelog_repartition_bytes": kafka_bytes if name == kafka_container else 0,
                "rocksdb_state_bytes": rocksdb_sz,
                "flink_cp_size": flink_cp.get("flink_cp_size", 0) if name == flink_container else 0,
                "flink_cp_duration": flink_cp.get("flink_cp_duration", 0) if name == flink_container else 0,
                "flink_cp_read": flink_cp.get("flink_cp_read", 0) if name == flink_container else 0,
                "flink_cp_write": flink_cp.get("flink_cp_write", 0) if name == flink_container else 0,
            }
        )
    return rows

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--containers", nargs="+", required=True)
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--interval-sec", type=float, default=15.0)
    parser.add_argument("--duration-sec", type=float, default=0.0)
    args = parser.parse_args()

    out_path = Path(args.output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "timestamp", "container", "cpu_perc", "mem_usage", "net_io", "block_io",
        "changelog_repartition_bytes", "rocksdb_state_bytes",
        "flink_cp_size", "flink_cp_duration", "flink_cp_read", "flink_cp_write"
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
            except Exception as exc:
                print(f"resource_monitor: poll error: {exc}")
            if args.duration_sec and (time.time() - start) >= args.duration_sec:
                break
            time.sleep(args.interval_sec)

if __name__ == "__main__":
    main()
