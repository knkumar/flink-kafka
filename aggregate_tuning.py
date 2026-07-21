import json
import glob
import statistics

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
        results[key] = {"t1-t0": [], "t2-t1": [], "t3-t2": []}
    
    results[key]["t1-t0"].append(lat.get("p99_write_to_input_append_latency_ms", 0))
    results[key]["t2-t1"].append(lat.get("p99_input_append_to_result_emission_latency_ms", 0))
    results[key]["t3-t2"].append(lat.get("p99_l_visibility_ms", 0))

print("| Engine | Commit/CP Interval | p99 t1-t0 (ms) | p99 t2-t1 (ms) | p99 t3-t2 (ms) |")
print("| --- | --- | ---: | ---: | ---: |")
for engine in ["flink", "kafka_streams"]:
    for interval in ["1000", "10000"]:
        key = (engine, interval)
        if key in results:
            t1 = statistics.median(results[key]["t1-t0"])
            t2 = statistics.median(results[key]["t2-t1"])
            t3 = statistics.median(results[key]["t3-t2"])
            print(f"| {engine} | {interval}ms | {t1:.1f} | {t2:.1f} | {t3:.1f} |")
