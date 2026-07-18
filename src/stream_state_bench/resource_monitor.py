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

# Constants for energy estimation
PUE = 1.5  # Power Usage Effectiveness
CPU_WATTS_MAX = 100.0  # Max CPU power in watts (assume 1 core = 100W for simplicity)
CPU_WATTS_IDLE = 10.0
MEM_WATTS_PER_GB = 0.5  # Watts per GB of memory
CARBON_INTENSITY = 400.0  # gCO2eq/kWh

def estimate_energy_costs(cpu_perc_str: str, mem_usage_str: str) -> tuple[float, float]:
    """Estimate power in Watts and carbon intensity in gCO2eq/h."""
    try:
        cpu_perc = float(cpu_perc_str.replace('%', ''))
        # Parse mem usage roughly (e.g. "1.2GiB / 16GiB" or "50MiB / 16GiB")
        mem_gb = 0.0
        if 'GiB' in mem_usage_str:
            mem_gb = float(mem_usage_str.split('/')[0].replace('GiB', '').strip())
        elif 'MiB' in mem_usage_str:
            mem_gb = float(mem_usage_str.split('/')[0].replace('MiB', '').strip()) / 1024
        elif 'B' in mem_usage_str and 'iB' not in mem_usage_str:
            mem_gb = float(mem_usage_str.split('/')[0].replace('B', '').strip()) / (1024 ** 3)
        
        cpu_power = CPU_WATTS_IDLE + (CPU_WATTS_MAX - CPU_WATTS_IDLE) * (cpu_perc / 100.0)
        mem_power = mem_gb * MEM_WATTS_PER_GB
        total_power = cpu_power + mem_power
        total_power_with_pue = total_power * PUE
        
        carbon_g_per_hour = (total_power_with_pue / 1000.0) * CARBON_INTENSITY
        return round(total_power_with_pue, 2), round(carbon_g_per_hour, 2)
    except Exception:
        return 0.0, 0.0

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
    for line in out.stdout.strip().splitlines():
        parts = line.split("\t")
        if len(parts) != 5:
            continue
        name, cpu_perc, mem_usage, net_io, block_io = parts
        power_w, carbon_gph = estimate_energy_costs(cpu_perc, mem_usage)
        rows.append(
            {
                "timestamp": ts,
                "container": name,
                "cpu_perc": cpu_perc,
                "mem_usage": mem_usage,
                "net_io": net_io,
                "block_io": block_io,
                "power_watts": power_w,
                "carbon_gCO2eq_per_hour": carbon_gph,
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
        "power_watts", "carbon_gCO2eq_per_hour"
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
