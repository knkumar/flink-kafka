#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: scripts/memory-note.sh SECTION PROVENANCE MESSAGE" >&2
  echo "example: scripts/memory-note.sh PROGRESS TOOL 'Ran make check successfully.'" >&2
  exit 2
fi

section="$1"
provenance="$2"
message="$3"
memory_file=".agents/MEMORY.md"
timestamp="$(date -Is)"

case "$section" in
  PLANS|DECISIONS|PROGRESS|DISCOVERIES|OUTCOMES) ;;
  *)
    echo "unknown section: $section" >&2
    exit 2
    ;;
esac

case "$provenance" in
  USER|CODE|TOOL|ASSUMPTION) ;;
  *)
    echo "unknown provenance: $provenance" >&2
    exit 2
    ;;
esac

tmp="$(mktemp)"
awk -v section="## [$section]" -v note="- ${timestamp} [${provenance}] ${message}" '
  $0 == section {
    print
    print note
    inserted = 1
    next
  }
  { print }
  END {
    if (!inserted) {
      exit 1
    }
  }
' "$memory_file" > "$tmp"
mv "$tmp" "$memory_file"
