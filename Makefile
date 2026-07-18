.PHONY: test experiment analyze engine-summary latency-summary check check-script kafka-streams-w1 kafka-streams-w1-latency kafka-streams-w2 kafka-streams-w2-latency kafka-streams-w3 kafka-streams-w3-latency kafka-streams-w4 kafka-streams-w4-latency kafka-streams-w5 kafka-streams-w5-latency flink-w1 flink-w1-latency flink-w2 flink-w2-latency flink-w3 flink-w3-latency flink-w4 flink-w4-latency flink-w5 flink-w5-latency repeat-correctness w1-latency-sweep w2-latency-sweep w3-latency-sweep w4-latency-sweep w5-latency-sweep

PYTHON ?= python3

test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests

experiment:
	PYTHONPATH=src $(PYTHON) -m stream_state_bench.run_local_experiment --workload all --events 1000 --keys 100 --seed 7 --out experiments/results/local_semantic_results.json

analyze:
	PYTHONPATH=src $(PYTHON) -m stream_state_bench.analyze_results experiments/results/local_semantic_results.json

engine-summary:
	PYTHONPATH=src $(PYTHON) -m stream_state_bench.summarize_engine_results

latency-summary:
	PYTHONPATH=src $(PYTHON) -m stream_state_bench.summarize_latency_results

check-script:
	./scripts/run-local-check.sh

check: check-script

kafka-streams-w1:
	./scripts/run-kafka-streams-w1.sh

kafka-streams-w1-latency:
	./scripts/run-kafka-streams-w1-latency.sh

kafka-streams-w2:
	WORKLOAD=filter_map WORKLOAD_ID=w2 ./scripts/run-kafka-streams-w1.sh

kafka-streams-w2-latency:
	WORKLOAD=filter_map WORKLOAD_ID=w2_latency ./scripts/run-kafka-streams-w1-latency.sh

kafka-streams-w3:
	WORKLOAD=tumbling_count WORKLOAD_ID=w3 ./scripts/run-kafka-streams-w1.sh

kafka-streams-w3-latency:
	WORKLOAD=tumbling_count WORKLOAD_ID=w3_latency ./scripts/run-kafka-streams-w1-latency.sh

kafka-streams-w4:
	WORKLOAD=sliding_sum WORKLOAD_ID=w4 START_MS=600000 ./scripts/run-kafka-streams-w1.sh

kafka-streams-w4-latency:
	WORKLOAD=sliding_sum WORKLOAD_ID=w4_latency START_MS=600000 ./scripts/run-kafka-streams-w1-latency.sh

kafka-streams-w5:
	WORKLOAD=stream_stream_join WORKLOAD_ID=w5 START_MS=1000 ./scripts/run-kafka-streams-w1.sh

kafka-streams-w5-latency:
	WORKLOAD=stream_stream_join WORKLOAD_ID=w5_latency START_MS=1000 ./scripts/run-kafka-streams-w1-latency.sh

flink-w1:
	./scripts/run-flink-w1.sh

flink-w1-latency:
	./scripts/run-flink-w1-latency.sh

flink-w2:
	WORKLOAD=filter_map WORKLOAD_ID=w2 ./scripts/run-flink-w1.sh

flink-w2-latency:
	WORKLOAD=filter_map WORKLOAD_ID=w2_latency ./scripts/run-flink-w1-latency.sh

flink-w3:
	WORKLOAD=tumbling_count WORKLOAD_ID=w3 ./scripts/run-flink-w1.sh

flink-w3-latency:
	WORKLOAD=tumbling_count WORKLOAD_ID=w3_latency ./scripts/run-flink-w1-latency.sh

flink-w4:
	WORKLOAD=sliding_sum WORKLOAD_ID=w4 START_MS=600000 ./scripts/run-flink-w1.sh

flink-w4-latency:
	WORKLOAD=sliding_sum WORKLOAD_ID=w4_latency START_MS=600000 ./scripts/run-flink-w1-latency.sh

flink-w5:
	WORKLOAD=stream_stream_join WORKLOAD_ID=w5 START_MS=1000 ./scripts/run-flink-w1.sh

flink-w5-latency:
	WORKLOAD=stream_stream_join WORKLOAD_ID=w5_latency START_MS=1000 ./scripts/run-flink-w1-latency.sh

repeat-correctness:
	./scripts/run-correctness-repeat.sh

w1-latency-sweep:
	./scripts/run-w1-latency-sweep.sh

w2-latency-sweep:
	./scripts/run-w2-latency-sweep.sh

w3-latency-sweep:
	./scripts/run-w3-latency-sweep.sh

w4-latency-sweep:
	./scripts/run-w4-latency-sweep.sh

w5-latency-sweep:
	./scripts/run-w5-latency-sweep.sh
