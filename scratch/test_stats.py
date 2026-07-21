import pandas as pd
import numpy as np
from scipy import stats
import glob
import json
from pathlib import Path

def compute_metrics():
    # find all latency_samples.csv
    files = glob.glob("experiments/results/*/latency_samples.csv")
    if not files:
        print("No samples found.")
        return
        
    f = files[0]
    print(f"Loading {f}")
    df = pd.read_csv(f)
    print(df.head())
    
    # drop warm-up (first 5% of samples by t0_ms)
    df = df.sort_values('t0_ms')
    warmup_idx = int(len(df) * 0.05)
    df = df.iloc[warmup_idx:]
    
    data = df['latency_ms'].dropna().values
    
    print("Computing percentiles...")
    p50 = np.percentile(data, 50)
    p95 = np.percentile(data, 95)
    p99 = np.percentile(data, 99)
    print(f"p50: {p50}, p95: {p95}, p99: {p99}")
    
    print("Computing bootstrap CIs (this might be slow)...")
    res_p50 = stats.bootstrap((data,), np.median, confidence_level=0.95, n_resamples=100)
    res_p95 = stats.bootstrap((data,), lambda x: np.percentile(x, 95), confidence_level=0.95, n_resamples=100)
    res_p99 = stats.bootstrap((data,), lambda x: np.percentile(x, 99), confidence_level=0.95, n_resamples=100)
    
    print(f"p50 CI: {res_p50.confidence_interval}")
    print(f"p95 CI: {res_p95.confidence_interval}")
    print(f"p99 CI: {res_p99.confidence_interval}")

if __name__ == '__main__':
    compute_metrics()
