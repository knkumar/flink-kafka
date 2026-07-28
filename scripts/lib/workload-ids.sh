#!/usr/bin/env bash
# Shared workload-id -> workload-name lookup used by scripts that accept a
# short id (w1..w5) and need the stream_state_bench workload name it maps to.

workload_name_for_id() {
  case "$1" in
    w2) echo "filter_map" ;;
    w3) echo "tumbling_count" ;;
    w4) echo "sliding_sum" ;;
    w5) echo "stream_stream_join" ;;
    *) echo "identity" ;;
  esac
}
