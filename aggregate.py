import json
import glob

results = {}
for path in glob.glob("experiments/results/*_failure_*/run_metadata.json"):
    with open(path) as f:
        meta = json.load(f)
    
    # Also load verification.json
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
    # failure looks like "jvm_kill_trial1"
    failure_type = "_".join(failure.split("_")[:-1])
    trial = failure.split("_")[-1]

    key = (engine, failure_type, workload)
    if key not in results:
        results[key] = []
    results[key].append((meta["latency_p99_ms"], passed))

# Calculate median p99 for passed runs, or report DNF
import statistics
print("| Engine | Failure Mode | W1 Hop | W3 Hop | W4 Hop |")
print("| --- | --- | ---: | ---: | ---: |")
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
        print(row)
