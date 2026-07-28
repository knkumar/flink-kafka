import json
import glob
import statistics

# 1. Gather failure data
results = {}
for path in glob.glob("experiments/results/*_failure_*/run_metadata.json"):
    with open(path) as f:
        meta = json.load(f)
    
    v_path = path.replace("run_metadata.json", "verification.json")
    try:
        with open(v_path) as f:
            v_data = json.load(f)
            passed = v_data.get("verification", {}).get("passed", False)
    except:
        passed = False

    engine = "flink" if "flink" in path else "kafka_streams"
    workload = meta["workload"]
    failure = meta["run_label"].replace("failure_", "")
    failure_type = "_".join(failure.split("_")[:-1])
    
    key = (engine, failure_type, workload)
    if key not in results:
        results[key] = []
    results[key].append((meta["latency_p99_ms"], passed))

failure_table = ["| Engine | Failure Mode | W1 Hop | W3 Hop | W4 Hop |", "| --- | --- | ---: | ---: | ---: |"]
for engine in ["flink", "kafka_streams"]:
    for failure_type in ["jvm_kill", "broker_kill", "node_loss"]:
        row = f"| {engine} | {failure_type} |"
        for w in ["identity", "tumbling_count", "sliding_sum"]:
            key = (engine, failure_type, w)
            if key not in results:
                row += " N/A |"
                continue
            
            passes = [r for r in results[key] if r[1]]
            if len(passes) >= 3:
                med = statistics.median([r[0] for r in passes])
                row += f" {med/1000.0:.2f}s ({len(passes)}/{len(results[key])}) |"
            else:
                row += f" DNF ({len(passes)}/{len(results[key])}) |"
        failure_table.append(row)


# 2. Gather tuning data
results = {}
for path in glob.glob("experiments/results/*_w3_latency_tuning-*/run_metadata.json"):
    with open(path) as f:
        meta = json.load(f)
    
    engine = "flink" if "flink" in path else "kafka_streams"
    run_label = meta["run_label"]
    if "tuning-cp-" in run_label:
        interval = run_label.split("tuning-cp-")[1].split("_")[0]
    elif "tuning-commit-" in run_label:
        interval = run_label.split("tuning-commit-")[1].split("_")[0]
    else:
        continue
        
    latency_path = path.replace("run_metadata.json", "latency_summary.json")
    try:
        with open(latency_path) as f:
            lat = json.load(f)["summary"]
    except:
        continue
        
    key = (engine, interval)
    if key not in results:
        results[key] = {"t1-t0": [], "t2-t1": [], "t3-t2": [], "total": []}
    
    results[key]["t1-t0"].append(lat.get("p99_write_to_input_append_latency_ms", 0))
    results[key]["t2-t1"].append(lat.get("p99_input_append_to_result_emission_latency_ms", 0))
    results[key]["t3-t2"].append(lat.get("p99_l_visibility_ms", 0))
    results[key]["total"].append(lat.get("p99_ms", 0))

tuning_table = ["| Engine | Interval | p99 total | p99 t1-t0 | p99 t2-t1 (processing) | p99 t3-t2 (commit) |", "| --- | ---: | ---: | ---: | ---: | ---: |"]
for engine in ["flink", "kafka_streams"]:
    for interval in ["1000", "10000"]:
        key = (engine, interval)
        if key in results:
            t1 = statistics.median(results[key]["t1-t0"])
            t2 = statistics.median(results[key]["t2-t1"])
            t3 = statistics.median(results[key]["t3-t2"])
            tot = statistics.median(results[key]["total"])
            tuning_table.append(f"| {engine} | {interval} ms | {tot:.1f} ms | {t1:.1f} ms | {t2:.1f} ms | {t3:.1f} ms |")


# 3. Patch final_paper.md
with open("paper/final_paper.md") as f:
    lines = f.read().split("\n")

# Patch failure table
start_idx = None
end_idx = None
for i, line in enumerate(lines):
    if line.startswith("| Engine | Failure Mode | W1 Hop | W3 Hop | W4 Hop |"):
        start_idx = i
    if start_idx is not None and i > start_idx and not line.startswith("|"):
        end_idx = i
        break
if start_idx is not None and end_idx is not None:
    lines = lines[:start_idx] + failure_table + lines[end_idx:]

# Patch tuning table (find the one with Commit/CP Interval)
start_idx = None
end_idx = None
for i, line in enumerate(lines):
    if line.startswith("| Engine | Interval | p99 total | p99 t1-t0 | p99 t2-t1 (processing) | p99 t3-t2 (commit) |"):
        start_idx = i
    if start_idx is not None and i > start_idx and not line.startswith("|"):
        end_idx = i
        break
if start_idx is not None and end_idx is not None:
    lines = lines[:start_idx] + tuning_table + lines[end_idx:]

with open("paper/final_paper.md", "w") as f:
    f.write("\n".join(lines))
