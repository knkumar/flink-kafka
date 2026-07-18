import argparse
import csv
import subprocess
import time
from pathlib import Path

def get_lag(compose_file: Path, group_id: str) -> dict[str, int]:
    cmd = [
        "docker", "compose", "-f", str(compose_file), "exec", "-T", "kafka",
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server", "kafka:9092",
        "--describe", "--group", group_id
    ]
    try:
        output = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        return {}
    
    lags = {}
    for line in output.splitlines():
        parts = line.split()
        if len(parts) >= 6 and parts[0] == group_id and parts[2].isdigit() and parts[5].isdigit():
            topic = parts[1]
            lag = int(parts[5])
            lags[topic] = lags.get(topic, 0) + lag
    return lags

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--compose-file", type=Path, required=True)
    parser.add_argument("--group-id", required=True)
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--interval-sec", type=int, default=5)
    args = parser.parse_args()
    
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp_ms", "topic", "lag"])
        f.flush()
        
        while True:
            try:
                lags = get_lag(args.compose_file, args.group_id)
                now = int(time.time() * 1000)
                for topic, lag in lags.items():
                    writer.writerow([now, topic, lag])
                f.flush()
                time.sleep(args.interval_sec)
            except KeyboardInterrupt:
                break
            except Exception:
                time.sleep(args.interval_sec)

if __name__ == "__main__":
    main()
