import csv
import json
import os
import glob
from collections import defaultdict
import numpy as np

def parse_size(size_str):
    if not isinstance(size_str, str): return float(size_str)
    size_str = size_str.upper().strip()
    if size_str.endswith('B') and not size_str.endswith('GB') and not size_str.endswith('MB') and not size_str.endswith('KB'):
        try:
            return float(size_str.replace('B', ''))
        except:
            pass
    units = {"KB": 1e3, "MB": 1e6, "GB": 1e9, "KIB": 1024, "MIB": 1024**2, "GIB": 1024**3}
    for u, m in units.items():
        if size_str.endswith(u):
            try:
                return float(size_str.replace(u, '')) * m
            except:
                pass
    try:
        return float(size_str)
    except:
        return 0.0

def aggregate_runs():
    results = defaultdict(list)
    base_dir = "experiments/results"
    for trial_dir in glob.glob(os.path.join(base_dir, "*_w*_resource_trial*")) + glob.glob(os.path.join(base_dir, "*_idle_resource_trial*")):
        csv_file = os.path.join(trial_dir, "resource_monitor.csv")
        metadata_file = os.path.join(trial_dir, "run_metadata.json")
        if not os.path.exists(csv_file):
            continue
            
        parts = os.path.basename(trial_dir).split("_")
        engine = parts[0]
        if parts[1] == "streams":
            engine = "kafka-streams"
            w = parts[2]
        else:
            w = parts[1]
            
        trial = parts[-1]
        
        # We need input/output count to normalize
        input_count = 1
        output_count = 1
        duration = 60
        if os.path.exists(metadata_file):
            try:
                with open(metadata_file) as f:
                    meta = json.load(f)
                    input_count = meta.get("total_inputs", 1)
                    output_count = meta.get("total_outputs", 1)
                    duration = meta.get("duration_sec", 60)
            except:
                pass
                
        metrics = defaultdict(list)
        with open(csv_file) as f:
            reader = csv.DictReader(f)
            for row in reader:
                container = row["container"]
                # parse mem usage e.g., 200MiB / 4GiB -> 200MiB
                mem_str = row["mem_usage"].split(" / ")[0]
                mem = parse_size(mem_str)
                cpu = float(row["cpu_perc"].replace("%", ""))
                net_io_str = row["net_io"].split(" / ")[0]
                net = parse_size(net_io_str)
                block_io_str = row["block_io"].split(" / ")[0]
                block = parse_size(block_io_str)
                rocksdb = float(row.get("rocksdb_state_bytes", 0))
                changelog = float(row.get("changelog_repartition_bytes", 0))
                flink_cp = float(row.get("flink_cp_size", 0))
                
                metrics[(container, "cpu")].append(cpu)
                metrics[(container, "mem")].append(mem)
                metrics[(container, "net")].append(net)
                metrics[(container, "block")].append(block)
                metrics[(container, "rocksdb")].append(rocksdb)
                metrics[(container, "changelog")].append(changelog)
                metrics[(container, "flink_cp")].append(flink_cp)

        # aggregate per container
        container_agg = {}
        for (container, metric), vals in metrics.items():
            if not vals: continue
            if container not in container_agg:
                container_agg[container] = {}
            container_agg[container][metric] = np.mean(vals)
            if metric == "mem":
                container_agg[container]["mem_peak"] = np.max(vals)
                container_agg[container]["mem_p95"] = np.percentile(vals, 95)
                
        results[(engine, w)].append({
            "trial": trial,
            "containers": container_agg,
            "input_count": input_count,
            "output_count": output_count,
            "duration": duration
        })
        
    out_csv = os.path.join(base_dir, "resource_summary.csv")
    with open(out_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["engine", "workload", "container", "metric", "mean", "std", "norm_per_input", "norm_per_output", "norm_per_sec"])
        for (engine, w), trials in results.items():
            # For each container and metric, compute mean and std across trials
            all_containers = set()
            for t in trials:
                all_containers.update(t["containers"].keys())
                
            for c in all_containers:
                all_metrics = set()
                for t in trials:
                    if c in t["containers"]:
                        all_metrics.update(t["containers"][c].keys())
                for m in all_metrics:
                    vals = []
                    norms_in = []
                    norms_out = []
                    norms_sec = []
                    for t in trials:
                        if c in t["containers"] and m in t["containers"][c]:
                            v = t["containers"][c][m]
                            vals.append(v)
                            norms_in.append(v / max(1, t["input_count"]))
                            norms_out.append(v / max(1, t["output_count"]))
                            norms_sec.append(v / max(1, t["duration"]))
                    if vals:
                        writer.writerow([engine, w, c, m, np.mean(vals), np.std(vals), np.mean(norms_in), np.mean(norms_out), np.mean(norms_sec)])

if __name__ == "__main__":
    aggregate_runs()
