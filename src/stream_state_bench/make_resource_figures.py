import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def main():
    csv_file = "experiments/results/resource_summary.csv"
    if not os.path.exists(csv_file):
        print(f"File {csv_file} not found, skipping figure generation.")
        return
        
    df = pd.read_csv(csv_file)
    
    out_dir = "experiments/results/figures"
    os.makedirs(out_dir, exist_ok=True)
    
    # Categorize containers
    def categorize(c):
        if "kafka" in c and "streams" not in c:
            return "broker"
        elif "minio" in c:
            return "storage"
        elif "flink" in c or "streams" in c:
            return "worker"
        return "other"
        
    df["role"] = df["container"].apply(categorize)
    
    # Group by engine, workload, role, metric
    # Sum across containers of the same role for mean
    grouped = df.groupby(["engine", "workload", "role", "metric"])["mean"].sum().reset_index()
    
    metrics = ["cpu", "mem", "rocksdb", "changelog", "flink_cp", "net", "block"]
    
    for w in df["workload"].unique():
        for m in metrics:
            data = grouped[(grouped["workload"] == w) & (grouped["metric"] == m)]
            if data.empty or data["mean"].sum() == 0:
                continue
                
            plt.figure(figsize=(8, 6))
            sns.barplot(data=data, x="engine", y="mean", hue="role")
            plt.title(f"Resource Cost: {m} for {w}")
            plt.ylabel(m)
            plt.tight_layout()
            plt.savefig(os.path.join(out_dir, f"resource_{m}_{w}.pdf"))
            plt.close()

if __name__ == "__main__":
    main()
