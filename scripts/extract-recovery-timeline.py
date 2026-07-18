#!/usr/bin/env python3
import sys
import re
from datetime import datetime

# Common Kafka streams and docker-compose log regex
log_pattern = re.compile(r"^(?:[\w-]+(?:_[\w-]+)?\s+\|\s+)?\[?(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2},\d{3})\]?\s+(\w+)\s+(.*)$")

def main():
    if len(sys.argv) < 2:
        print("Usage: extract-recovery-timeline.py <docker-compose.log>")
        sys.exit(1)

    log_file = sys.argv[1]

    timeline = []
    
    last_log_time = None
    gap_threshold_sec = 2.0  # Assumed restart or failure gap
    
    peak_backlog = 0

    try:
        with open(log_file, "r") as f:
            for line in f:
                # Some logs might have docker prefix, some might not.
                m = log_pattern.search(line.strip())
                if not m:
                    continue
                
                ts_str, level, msg = m.groups()
                
                try:
                    ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S,%f")
                except ValueError:
                    continue

                if last_log_time and (ts - last_log_time).total_seconds() > gap_threshold_sec:
                    timeline.append((ts, f"Silence / Restart detected (gap of {(ts - last_log_time).total_seconds():.2f}s)"))

                last_log_time = ts
                
                if "State transition from" in msg:
                    timeline.append((ts, f"State Transition: {msg}"))
                    if "to REBALANCING" in msg or "to PENDING_SHUTDOWN" in msg:
                        timeline.append((ts, "Failure detected / Rebalance triggered"))
                
                if "Restoring state store" in msg or "Restoring partition" in msg:
                    timeline.append((ts, "State restore started"))
                    
                if "Finished restoring partition" in msg or "restoration took" in msg.lower():
                    timeline.append((ts, "State restore completed"))
                    
                lag_match = re.search(r"lag[\s:=]+(\d+)", msg, re.IGNORECASE)
                if lag_match:
                    lag = int(lag_match.group(1))
                    if lag > peak_backlog:
                        peak_backlog = lag
                        timeline.append((ts, f"New Peak Backlog: {lag}"))
                        
    except Exception as e:
        print(f"Error parsing logs: {e}")
        return

    print("=== Recovery Timeline ===")
    for ts, event in timeline:
        print(f"[{ts}] {event}")
        
    print(f"\nPeak Backlog observed in logs: {peak_backlog}")

if __name__ == "__main__":
    main()
