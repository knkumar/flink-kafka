#!/usr/bin/env bash
set -eo pipefail

OUTFILE="experiments/results/environment.json"
mkdir -p "$(dirname "$OUTFILE")"

cat <<EOF > "$OUTFILE"
{
  "host": {
    "os": "$(cat /etc/os-release | grep PRETTY_NAME | cut -d= -f2 | tr -d '"' || true)",
    "kernel": "$(uname -r || true)",
    "cpu_model": "$(lscpu | grep 'Model name' | awk -F ':' '{print $2}' | xargs || true)",
    "logical_cores": "$(nproc || true)",
    "ram_total_kb": "$(grep MemTotal /proc/meminfo | awk '{print $2}' || true)",
    "disk_fs": "$(df -T / | tail -1 | awk '{print $2}' || true)"
  },
  "versions": {
    "docker": "$(docker --version || true)",
    "jvm": "$(java -version 2>&1 | head -1 | tr -d '"' || true)"
  },
  "containers": [
EOF

FIRST=true
if systemctl is-active --quiet docker || service docker status > /dev/null 2>&1 || docker info > /dev/null 2>&1; then
    for cid in $(docker ps -q); do
        if [ "$FIRST" = true ]; then
            FIRST=false
        else
            echo "," >> "$OUTFILE"
        fi
        cname=$(docker inspect --format '{{.Name}}' "$cid" | sed 's/^\///')
        cpus=$(docker inspect --format '{{.HostConfig.NanoCpus}}' "$cid")
        mem=$(docker inspect --format '{{.HostConfig.Memory}}' "$cid")
        cat <<EOF >> "$OUTFILE"
    {
      "id": "$cid",
      "name": "$cname",
      "cpu_limit_nano": $cpus,
      "memory_limit_bytes": $mem
    }
EOF
    done
fi

cat <<EOF >> "$OUTFILE"
  ]
}
EOF
echo "Wrote environment info to $OUTFILE"
