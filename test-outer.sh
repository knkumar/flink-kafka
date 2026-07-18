#!/bin/bash
WORKLOAD="w1"
local_workload_name="identity"
EVENTS=2000 RATE_PER_SEC=20 RUN_LABEL=node_loss WORKLOAD="$local_workload_name" WORKLOAD_ID="${WORKLOAD}_latency" ./test-inner.sh
