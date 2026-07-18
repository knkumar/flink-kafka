#!/bin/sh
set -eu

: "${PYTHON:=python3}"
: "${PYTHONPATH:=src}"
export PYTHONPATH

"$PYTHON" -m unittest discover -s tests
"$PYTHON" -m stream_state_bench.run_local_experiment --workload all --events 1000 --keys 100 --seed 7 --out experiments/results/local_semantic_results.json
"$PYTHON" -m stream_state_bench.analyze_results experiments/results/local_semantic_results.json
