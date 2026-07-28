import os
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm
from collections import defaultdict
import glob

def get_base_experiment_and_trial(dirname):
    match = re.search(r'^(.*)_trial(\d+)$', dirname)
    if match:
        return match.group(1), int(match.group(2))
    return dirname, 1

def plot_ecdf(ax, data, label, color):
    x = np.sort(data)
    y = np.arange(1, len(x) + 1) / len(x)
    ax.plot(x, y, label=label, color=color, alpha=0.7)

def main():
    results_dir = os.path.join("experiments", "results")
    figures_dir = os.path.join(results_dir, "figures")
    os.makedirs(figures_dir, exist_ok=True)
    
    # map base_exp -> { trial -> full_path }
    experiments = defaultdict(dict)

    if not os.path.exists(results_dir):
        print(f"Directory {results_dir} not found.")
        return

    for entry in os.listdir(results_dir):
        full_path = os.path.join(results_dir, entry)
        if os.path.isdir(full_path):
            if entry.startswith("_") or entry == "figures":
                continue
            base_exp, trial = get_base_experiment_and_trial(entry)
            experiments[base_exp][trial] = full_path

    colors = plt.cm.get_cmap('tab10', 10)

    # We will generate ECDF and Lag figures per base experiment
    for base_exp, trials in experiments.items():
        if not trials:
            continue
        
        # 1. Plot ECDF
        fig_ecdf, ax_ecdf = plt.subplots(figsize=(8, 5))
        has_latency_data = False
        
        # 2. Plot Lag
        fig_lag, ax_lag = plt.subplots(figsize=(8, 5))
        has_lag_data = False
        
        color_idx = 0
        
        for trial, path in sorted(trials.items()):
            color = colors(color_idx % 10)
            color_idx += 1
            
            # ECDF Plotting
            latency_file = os.path.join(path, "latency_samples.csv")
            if os.path.exists(latency_file):
                try:
                    df = pd.read_csv(latency_file)
                    if 'latency_ms' in df.columns:
                        data = df['latency_ms'].dropna().values
                        plot_ecdf(ax_ecdf, data, label=f"Run {trial}", color=color)
                        has_latency_data = True
                except Exception as e:
                    print(f"Error reading {latency_file}: {e}")
            
            # Lag Plotting & Stability
            lag_file = os.path.join(path, "lag.csv")
            if os.path.exists(lag_file):
                try:
                    df_lag = pd.read_csv(lag_file)
                    if 'timestamp_ms' in df_lag.columns and 'lag' in df_lag.columns:
                        df_lag = df_lag.dropna(subset=['timestamp_ms', 'lag']).sort_values('timestamp_ms')
                        if len(df_lag) > 1:
                            # convert ms to minutes relative to start
                            t0 = df_lag['timestamp_ms'].min()
                            t_min = (df_lag['timestamp_ms'] - t0) / 60000.0
                            lag_vals = df_lag['lag'].values
                            
                            ax_lag.plot(t_min, lag_vals, label=f"Run {trial} lag", color=color, alpha=0.7)
                            
                            # Fit OLS: lag vs wall-clock time (in minutes)
                            X = sm.add_constant(t_min.values)
                            model = sm.OLS(lag_vals, X)
                            results = model.fit()
                            
                            slope = results.params[1] if len(results.params) > 1 else 0
                            ci = results.conf_int(alpha=0.05)
                            slope_ci_upper = ci[1][1] if len(ci) > 1 else 0
                            
                            # Stability Criterion: slope CI upper bound < 5 events/min => stable
                            stable = slope_ci_upper < 5.0
                            status = "Stable" if stable else "Unstable"
                            
                            # Plot regression line
                            ax_lag.plot(t_min, results.predict(X), '--', color=color, 
                                        label=f"Fit {trial} (slope: {slope:.2f}, CI upper: {slope_ci_upper:.2f}) - {status}")
                            
                            has_lag_data = True
                except Exception as e:
                    print(f"Error reading {lag_file}: {e}")

        if has_latency_data:
            ax_ecdf.set_title(f"ECDF of Latency - {base_exp}")
            ax_ecdf.set_xlabel("Latency (ms)")
            ax_ecdf.set_ylabel("ECDF")
            ax_ecdf.legend()
            ax_ecdf.grid(True, linestyle='--', alpha=0.6)
            ecdf_pdf_path = os.path.join(figures_dir, f"ecdf_{base_exp}.pdf")
            fig_ecdf.savefig(ecdf_pdf_path, bbox_inches='tight')
            print(f"Saved {ecdf_pdf_path}")
        plt.close(fig_ecdf)
        
        if has_lag_data:
            ax_lag.set_title(f"Lag over Time - {base_exp}")
            ax_lag.set_xlabel("Time (minutes)")
            ax_lag.set_ylabel("Lag (events)")
            ax_lag.legend()
            ax_lag.grid(True, linestyle='--', alpha=0.6)
            lag_pdf_path = os.path.join(figures_dir, f"lag_{base_exp}.pdf")
            fig_lag.savefig(lag_pdf_path, bbox_inches='tight')
            print(f"Saved {lag_pdf_path}")
        plt.close(fig_lag)

if __name__ == "__main__":
    main()
