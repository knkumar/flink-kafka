import json
import glob
from pathlib import Path
import csv

results = []
for p in glob.glob("experiments/results/*_failure_*/latency_summary.json"):
    path = Path(p)
    with open(path) as f:
        data = json.load(f)
        parent = path.parent.name
        data["run_dir"] = parent
        results.append(data)

if results:
    with open("experiments/results/failure_latency_aggregated.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)
    print(f"Aggregated {len(results)} results")
else:
    print("No results found")
