#!/usr/bin/env bash
set -euo pipefail

memory_file=".agents/MEMORY.md"

test -f "$memory_file"
for section in PLANS DECISIONS PROGRESS DISCOVERIES OUTCOMES; do
  grep -q "^## \\[$section\\]$" "$memory_file"
done

if grep -E '^- ' "$memory_file" | grep -Ev '^- [0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[-+Z0-9:]* \[(USER|CODE|TOOL|ASSUMPTION)\] ' >/dev/null; then
  echo "memory contains malformed bullet(s)" >&2
  exit 1
fi
