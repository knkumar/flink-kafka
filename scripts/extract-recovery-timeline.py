#!/usr/bin/env python3
import os
import sys
import re
import csv
import glob
import json
from datetime import datetime

log_pattern = re.compile(r"^(?:[\w-]+(?:_[\w-]+)?\s+\|\s+)?(?:\[?(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2},\d{3})\]?\s+)?(?:(\w+)\s+)?(.*)$")

def parse_trial(dirname):
    log_file = os.path.join(dirname, "docker-compose.log")
    lag_file = os.path.join(dirname, "lag.csv")
    meta_file = os.path.join(dirname, "run_metadata.json")
    
    milestones = {
        'trial_dir': os.path.basename(dirname),
        't_inject': None,
        't_restart': None,
        't_rebalance': None,
        't_restore_start': None,
        't_restore_complete': None,
        't_first_process': None,
        't_first_commit': None,
        't_backlog_zero': None,
        'censored': False,
        'timeout_sec': None,
        'censor_cause': None
    }
    
    # Read timeout from metadata if available (to set exact timeout_sec)
    if os.path.exists(meta_file):
        try:
            with open(meta_file, 'r') as f:
                meta = json.load(f)
                milestones['timeout_sec'] = meta.get('timeout_sec', None)
        except:
            pass

    # fallback read from run script or runner logs? The prompt says "read the timeout from the run script, not guessed"
    # Wait, the run script `scripts/run-flink-w1-latency.sh` passes `--timeout-sec "$(( (EVENTS / RATE_PER_SEC) + 60 ))"`
    # Events=2000, RATE=20 -> 100 + 60 = 160s.
    if milestones['timeout_sec'] is None:
        milestones['timeout_sec'] = 160

    if not os.path.exists(log_file):
        return milestones

    try:
        with open(log_file, "r") as f:
            for line in f:
                m = log_pattern.search(line.strip())
                if not m:
                    continue
                ts_str, level, msg = m.groups()
                if not ts_str:
                    continue
                
                msg_lower = msg.lower()
                
                # Injection marker
                if "kill" in msg_lower or "exited with code" in msg_lower or "stopping" in msg_lower:
                    if not milestones['t_inject']: milestones['t_inject'] = ts_str
                # Restart marker
                if "starting container" in msg_lower or "started container" in msg_lower or "registered signal handlers" in msg_lower:
                    if not milestones['t_restart']: milestones['t_restart'] = ts_str
                # Rebalance / assignment
                if "rebalance" in msg_lower or "assign" in msg_lower or "state transition from" in msg_lower:
                    if not milestones['t_rebalance']: milestones['t_rebalance'] = ts_str
                # Restore start
                if "restoring state store" in msg_lower or "restoring partition" in msg_lower or "starting to restore from state handle" in msg_lower:
                    if not milestones['t_restore_start']: milestones['t_restore_start'] = ts_str
                # Restore complete
                if "finished restoring" in msg_lower or "restoration took" in msg_lower:
                    if not milestones['t_restore_complete']: milestones['t_restore_complete'] = ts_str
                # Process / commit (rough proxies)
                if "processing" in msg_lower and "first" in msg_lower:
                    if not milestones['t_first_process']: milestones['t_first_process'] = ts_str
                if "commit" in msg_lower and ("offset" in msg_lower or "first" in msg_lower):
                    if not milestones['t_first_commit']: milestones['t_first_commit'] = ts_str
                
                # Check for censor causes in Kafka Streams
                if "kafka_streams" in dirname:
                    if "error" in msg_lower or "exception" in msg_lower or "timeout" in msg_lower:
                        if not milestones['censor_cause']:
                            milestones['censor_cause'] = "Error in logs"
                            
    except Exception:
        pass
        
    if os.path.exists(lag_file):
        try:
            with open(lag_file, "r") as f:
                for line in f:
                    if "timestamp_ms" in line: continue
                    parts = line.strip().split(',')
                    if len(parts) >= 2:
                        lag_val = int(parts[1])
                        if lag_val == 0:
                            milestones['t_backlog_zero'] = parts[0]
                            break
        except:
            pass

    # Determine if censored
    # If it's a failure run and it didn't finish restoring or reach backlog zero, it might be censored
    # Actually, verification.json tells if it matched expected count.
    verif_file = os.path.join(dirname, "verification.json")
    if os.path.exists(verif_file):
        try:
            with open(verif_file, 'r') as f:
                verif = json.load(f)
                if verif.get('missing_count', 0) > 0:
                    milestones['censored'] = True
        except:
            pass
            
    if milestones['censored'] and milestones['censor_cause'] is None:
        milestones['censor_cause'] = "genuine stall"

    return milestones

def main():
    base_dir = "experiments/results"
    out_csv = os.path.join(base_dir, "recovery_milestones.csv")
    
    trials = []
    for entry in os.listdir(base_dir):
        if "_failure_" in entry and "trial" in entry and os.path.isdir(os.path.join(base_dir, entry)):
            trials.append(os.path.join(base_dir, entry))
            
    results = []
    for t in trials:
        results.append(parse_trial(t))
        
    if not results:
        print("No trials found.")
        return
        
    keys = list(results[0].keys())
    with open(out_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(results)
        
    print(f"Extracted {len(results)} trials to {out_csv}")

if __name__ == "__main__":
    main()
