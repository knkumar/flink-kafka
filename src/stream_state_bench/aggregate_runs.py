import os
import re
import numpy as np
import pandas as pd
from collections import defaultdict
import glob

from .cli_utils import get_base_experiment_and_trial

def bootstrap_median_ci(data, n_bootstraps=1000, ci=95):
    data = np.array(data)
    if len(data) == 0:
        return np.nan, np.nan, np.nan
    bootstrapped_medians = []
    for _ in range(n_bootstraps):
        sample = np.random.choice(data, size=len(data), replace=True)
        bootstrapped_medians.append(np.median(sample))
    
    lower_percentile = (100 - ci) / 2.0
    upper_percentile = 100 - lower_percentile
    
    ci_lower = np.percentile(bootstrapped_medians, lower_percentile)
    ci_upper = np.percentile(bootstrapped_medians, upper_percentile)
    return np.median(data), ci_lower, ci_upper

def main():
    results_dir = os.path.join("experiments", "results")
    if not os.path.exists(results_dir):
        print(f"Directory {results_dir} not found.")
        return

    # Map base_experiment -> dict of { trial: latency_df }
    experiments = defaultdict(dict)

    for entry in os.listdir(results_dir):
        full_path = os.path.join(results_dir, entry)
        if os.path.isdir(full_path):
            # Skip _archived or anything not looking like a run
            if entry.startswith("_"):
                continue
            
            latency_file = os.path.join(full_path, "latency_samples.csv")
            if os.path.exists(latency_file):
                base_exp, trial = get_base_experiment_and_trial(entry)
                experiments[base_exp][trial] = full_path

    # Output list
    summary_rows = []

    for base_exp, trials in experiments.items():
        if not trials:
            continue
        
        # component -> trial -> values of p50, p95, p99
        # we also need the "total" (latency_ms)
        percentiles_per_trial = defaultdict(lambda: defaultdict(dict)) # comp -> trial -> metric
        n_samples_per_trial = {}

        for trial, path in trials.items():
            latency_file = os.path.join(path, "latency_samples.csv")
            try:
                df = pd.read_csv(latency_file)
            except Exception as e:
                print(f"Error reading {latency_file}: {e}")
                continue

            n_samples_per_trial[trial] = len(df)
            
            # identify components: columns excluding event_id, tX_ms, and latency_ms
            exclude_cols = {'event_id', 'latency_ms'}
            components = [c for c in df.columns if c not in exclude_cols and not re.match(r'^t\d+_ms$', c)]
            
            # calculate p50, p95, p99 (using linear interpolation by default in numpy.percentile / pd.quantile)
            # The instructions explicitly say to state the quantile estimator (linear interpolation, default) and N behind it.
            
            cols_to_calc = ['latency_ms'] + components
            for col in cols_to_calc:
                if col not in df.columns:
                    continue
                # drop NaNs if any
                s = df[col].dropna()
                if len(s) == 0:
                    continue
                p50 = np.percentile(s, 50, method='linear')
                p95 = np.percentile(s, 95, method='linear')
                p99 = np.percentile(s, 99, method='linear')
                
                percentiles_per_trial[col][trial] = {
                    'p50': p50,
                    'p95': p95,
                    'p99': p99
                }
        
        # Now aggregate across trials (the five runs)
        for col in percentiles_per_trial.keys():
            p50_list = []
            p95_list = []
            p99_list = []
            for trial in percentiles_per_trial[col].keys():
                p50_list.append(percentiles_per_trial[col][trial]['p50'])
                p95_list.append(percentiles_per_trial[col][trial]['p95'])
                p99_list.append(percentiles_per_trial[col][trial]['p99'])

            # bootstrap for median of runs for p50, p95, p99
            med_p50, p50_lower, p50_upper = bootstrap_median_ci(p50_list)
            med_p95, p95_lower, p95_upper = bootstrap_median_ci(p95_list)
            med_p99, p99_lower, p99_upper = bootstrap_median_ci(p99_list)
            
            # Run-to-run dispersion (IQR)
            iqr_p50 = np.percentile(p50_list, 75) - np.percentile(p50_list, 25)
            iqr_p95 = np.percentile(p95_list, 75) - np.percentile(p95_list, 25)
            iqr_p99 = np.percentile(p99_list, 75) - np.percentile(p99_list, 25)
            
            avg_n_samples = np.mean([n_samples_per_trial[t] for t in percentiles_per_trial[col].keys()])
            num_runs = len(percentiles_per_trial[col])

            summary_rows.append({
                'experiment': base_exp,
                'metric': col,
                'runs_count': num_runs,
                'avg_n_samples_per_run': avg_n_samples,
                'quantile_estimator': 'linear_interpolation',
                'p50_median_of_runs': med_p50,
                'p50_95ci_lower': p50_lower,
                'p50_95ci_upper': p50_upper,
                'p50_iqr': iqr_p50,
                'p95_median_of_runs': med_p95,
                'p95_95ci_lower': p95_lower,
                'p95_95ci_upper': p95_upper,
                'p95_iqr': iqr_p95,
                'p99_median_of_runs': med_p99,
                'p99_95ci_lower': p99_lower,
                'p99_95ci_upper': p99_upper,
                'p99_iqr': iqr_p99,
            })
            
    summary_df = pd.DataFrame(summary_rows)
    output_file = os.path.join(results_dir, "sustained_summary.csv")
    summary_df.to_csv(output_file, index=False)
    print(f"Aggregated summary written to {output_file}")

if __name__ == "__main__":
    main()
