# Streaming State Cost Benchmark Artifact

A reproducible benchmark comparing Apache Flink and Kafka Streams on Apache Kafka (KRaft mode) across five workloads: a stateless identity transform, a stateless filter/map, a tumbling-count window, a sliding-sum window, and a stream-stream join. Both engines run against the same generated input, the same multiset verifier, and the same four-point (T0-T3) latency decomposition.

**Start with `paper/final_paper.md`** for the current findings, what is and is not measured, and where each number comes from. This README covers how to run the artifact, not what it found.

## Current Artifact Status

See `paper/final_paper.md` for the full results and their scope, `docs/results_appendix.md` for exhaustive per-run tables, `docs/claim_evidence_map.md` for a claim-by-claim pointer to the exact evidence file, and `docs/rubric_assessment.md` for a self-assessment against `rubric.md`. `docs/project_log.md` has the chronological record of what was run, in what order, and what failed along the way; that log, not this README, is the place to look for command-by-command history.

In one paragraph: correctness passes for all five workloads on both engines (two independent runs each, zero missing/unexpected/duplicate records). Fixed-rate latency is measured at 10/20/40 records/sec with a repeat at 20/sec. A 30-minute, 100 events/sec sustained-load stability check covers both engines on all five workloads (W5 uses a bounded, documented alternative); it shows both engines' windowed workloads carrying a multi-second processing-hop cost, roughly a thousandfold jump from their own stateless-workload baseline, that a matched tuning comparison traces to two different mechanisms per engine: Kafka Streams' commit interval delays when the engine decides a window has closed, Flink's checkpoint interval delays when an already-computed result becomes visible under exactly-once delivery. A full fault-injection matrix (JVM kill, broker kill, node loss, both engines) exists for the identity workload and shows a five-to-six-times gap in recovery cost between the engines, on a single trial. None of this yet supports a throughput envelope, an infrastructure-cost attribution, or a state-size-dependent recovery claim; the paper says so explicitly rather than implying otherwise, and self-scores 90/100 against `rubric.md` (`docs/rubric_assessment.md`).

## Quick Start

Run locally:

```bash
make check
```

The same checks can be run directly with:

```bash
./scripts/run-local-check.sh
```

Run in a container:

```bash
docker build -t stream-state-bench .
docker run --rm stream-state-bench
```

Generated files:

- `experiments/results/local_semantic_results.json`
- `experiments/results/local_semantic_summary.csv`
- `experiments/results/local_semantic_summary.md`

Run Kafka Streams W1 identity:

```bash
make kafka-streams-w1
```

Generated files:

- `experiments/results/kafka_streams_w1/input.tsv`
- `experiments/results/kafka_streams_w1/actual.jsonl`
- `experiments/results/kafka_streams_w1/verification.json`
- `experiments/results/kafka_streams_w1/run_metadata.json`
- `experiments/results/kafka_streams_w1/docker-compose.log`

Run Kafka Streams W2 filter/map:

```bash
make kafka-streams-w2
```

Generated files:

- `experiments/results/kafka_streams_w2/input.tsv`
- `experiments/results/kafka_streams_w2/actual.jsonl`
- `experiments/results/kafka_streams_w2/verification.json`
- `experiments/results/kafka_streams_w2/run_metadata.json`
- `experiments/results/kafka_streams_w2/docker-compose.log`

Run Kafka Streams W3 tumbling count:

```bash
make kafka-streams-w3
```

Generated files:

- `experiments/results/kafka_streams_w3/input.tsv`
- `experiments/results/kafka_streams_w3/producer_input.tsv`
- `experiments/results/kafka_streams_w3/actual.jsonl`
- `experiments/results/kafka_streams_w3/verification.json`
- `experiments/results/kafka_streams_w3/run_metadata.json`
- `experiments/results/kafka_streams_w3/docker-compose.log`

Run Kafka Streams W4 sliding sum:

```bash
make kafka-streams-w4
```

Generated files:

- `experiments/results/kafka_streams_w4/input.tsv`
- `experiments/results/kafka_streams_w4/producer_input.tsv`
- `experiments/results/kafka_streams_w4/actual.jsonl`
- `experiments/results/kafka_streams_w4/verification.json`
- `experiments/results/kafka_streams_w4/run_metadata.json`
- `experiments/results/kafka_streams_w4/docker-compose.log`

Run Kafka Streams W5 stream-stream join:

```bash
make kafka-streams-w5
```

Generated files:

- `experiments/results/kafka_streams_w5/input.tsv`
- `experiments/results/kafka_streams_w5/left_input.tsv`
- `experiments/results/kafka_streams_w5/right_input.tsv`
- `experiments/results/kafka_streams_w5/actual.jsonl`
- `experiments/results/kafka_streams_w5/verification.json`
- `experiments/results/kafka_streams_w5/run_metadata.json`
- `experiments/results/kafka_streams_w5/docker-compose.log`

Run Flink W1 identity:

```bash
make flink-w1
```

Generated files:

- `experiments/results/flink_w1/input.tsv`
- `experiments/results/flink_w1/actual.jsonl`
- `experiments/results/flink_w1/verification.json`
- `experiments/results/flink_w1/run_metadata.json`
- `experiments/results/flink_w1/docker-compose.log`

Run Flink W2 filter/map:

```bash
make flink-w2
```

Generated files:

- `experiments/results/flink_w2/input.tsv`
- `experiments/results/flink_w2/actual.jsonl`
- `experiments/results/flink_w2/verification.json`
- `experiments/results/flink_w2/run_metadata.json`
- `experiments/results/flink_w2/docker-compose.log`

Run Flink W3 tumbling count:

```bash
make flink-w3
```

Generated files:

- `experiments/results/flink_w3/input.tsv`
- `experiments/results/flink_w3/producer_input.tsv`
- `experiments/results/flink_w3/actual.jsonl`
- `experiments/results/flink_w3/verification.json`
- `experiments/results/flink_w3/run_metadata.json`
- `experiments/results/flink_w3/docker-compose.log`

Run Flink W4 sliding sum:

```bash
make flink-w4
```

Generated files:

- `experiments/results/flink_w4/input.tsv`
- `experiments/results/flink_w4/producer_input.tsv`
- `experiments/results/flink_w4/actual.jsonl`
- `experiments/results/flink_w4/verification.json`
- `experiments/results/flink_w4/run_metadata.json`
- `experiments/results/flink_w4/docker-compose.log`

Run Flink W5 stream-stream join:

```bash
make flink-w5
```

Generated files:

- `experiments/results/flink_w5/input.tsv`
- `experiments/results/flink_w5/left_input.tsv`
- `experiments/results/flink_w5/right_input.tsv`
- `experiments/results/flink_w5/actual.jsonl`
- `experiments/results/flink_w5/verification.json`
- `experiments/results/flink_w5/run_metadata.json`
- `experiments/results/flink_w5/docker-compose.log`

Run the repeated correctness matrix:

```bash
make repeat-correctness
make engine-summary
```

Generated files:

- `experiments/results/kafka_streams_w1_repeat1/` through `experiments/results/kafka_streams_w5_repeat1/`
- `experiments/results/flink_w1_repeat1/` through `experiments/results/flink_w5_repeat1/`
- `experiments/results/engine_correctness_summary.csv`
- `experiments/results/engine_correctness_summary.md`

Run the Kafka Streams W1 latency probe:

```bash
make kafka-streams-w1-latency
```

Generated files:

- `experiments/results/kafka_streams_w1_latency/input.tsv`
- `experiments/results/kafka_streams_w1_latency/actual.jsonl`
- `experiments/results/kafka_streams_w1_latency/verification.json`
- `experiments/results/kafka_streams_w1_latency/latency_summary.json`
- `experiments/results/kafka_streams_w1_latency/latency_samples.csv`
- `experiments/results/kafka_streams_w1_latency/run_metadata.json`
- `experiments/results/kafka_streams_w1_latency/docker-compose.log`

Run the Flink W1 latency probe:

```bash
make flink-w1-latency
```

Generated files:

- `experiments/results/flink_w1_latency/input.tsv`
- `experiments/results/flink_w1_latency/actual.jsonl`
- `experiments/results/flink_w1_latency/verification.json`
- `experiments/results/flink_w1_latency/latency_summary.json`
- `experiments/results/flink_w1_latency/latency_samples.csv`
- `experiments/results/flink_w1_latency/run_metadata.json`
- `experiments/results/flink_w1_latency/docker-compose.log`

Run the W2 latency probes:

```bash
make kafka-streams-w2-latency
make flink-w2-latency
```

Generated files:

- `experiments/results/kafka_streams_w2_latency/`
- `experiments/results/flink_w2_latency/`

Run the W3 latency probes:

```bash
make kafka-streams-w3-latency
make flink-w3-latency
```

Generated files:

- `experiments/results/kafka_streams_w3_latency/`
- `experiments/results/flink_w3_latency/`

Run the W4 latency probes:

```bash
make kafka-streams-w4-latency
make flink-w4-latency
```

Generated files:

- `experiments/results/kafka_streams_w4_latency/`
- `experiments/results/flink_w4_latency/`

Run the W5 latency probes:

```bash
make kafka-streams-w5-latency
make flink-w5-latency
```

Generated files:

- `experiments/results/kafka_streams_w5_latency/`
- `experiments/results/flink_w5_latency/`

Run the W1 latency sweep and repeat:

```bash
make w1-latency-sweep
```

Generated files:

- `experiments/results/kafka_streams_w1_latency_rate10/`
- `experiments/results/flink_w1_latency_rate10/`
- `experiments/results/kafka_streams_w1_latency_rate40/`
- `experiments/results/flink_w1_latency_rate40/`
- `experiments/results/kafka_streams_w1_latency_repeat1/`
- `experiments/results/flink_w1_latency_repeat1/`
- `experiments/results/latency_summary.csv`
- `experiments/results/latency_summary.md`
- `experiments/results/latency_aggregate_summary.csv`
- `experiments/results/latency_aggregate_summary.md`

Run the W2 latency sweep and repeat:

```bash
make w2-latency-sweep
```

Generated files:

- `experiments/results/kafka_streams_w2_latency_rate10/`
- `experiments/results/flink_w2_latency_rate10/`
- `experiments/results/kafka_streams_w2_latency_rate40/`
- `experiments/results/flink_w2_latency_rate40/`
- `experiments/results/kafka_streams_w2_latency_repeat1/`
- `experiments/results/flink_w2_latency_repeat1/`
- `experiments/results/latency_summary.csv`
- `experiments/results/latency_summary.md`
- `experiments/results/latency_aggregate_summary.csv`
- `experiments/results/latency_aggregate_summary.md`

Run the W3 latency sweep and repeat:

```bash
make w3-latency-sweep
```

Generated files:

- `experiments/results/kafka_streams_w3_latency_rate10/`
- `experiments/results/flink_w3_latency_rate10/`
- `experiments/results/kafka_streams_w3_latency_rate40/`
- `experiments/results/flink_w3_latency_rate40/`
- `experiments/results/kafka_streams_w3_latency_repeat1/`
- `experiments/results/flink_w3_latency_repeat1/`
- `experiments/results/latency_summary.csv`
- `experiments/results/latency_summary.md`
- `experiments/results/latency_aggregate_summary.csv`
- `experiments/results/latency_aggregate_summary.md`

Run the W4 latency sweep and repeat:

```bash
make w4-latency-sweep
```

Generated files:

- `experiments/results/kafka_streams_w4_latency_rate10/`
- `experiments/results/flink_w4_latency_rate10/`
- `experiments/results/kafka_streams_w4_latency_rate40/`
- `experiments/results/flink_w4_latency_rate40/`
- `experiments/results/kafka_streams_w4_latency_repeat1/`
- `experiments/results/flink_w4_latency_repeat1/`
- `experiments/results/latency_summary.csv`
- `experiments/results/latency_summary.md`
- `experiments/results/latency_aggregate_summary.csv`
- `experiments/results/latency_aggregate_summary.md`

Run the W5 latency sweep and repeat:

```bash
make w5-latency-sweep
```

Generated files:

- `experiments/results/kafka_streams_w5_latency_rate10/`
- `experiments/results/flink_w5_latency_rate10/`
- `experiments/results/kafka_streams_w5_latency_rate40/`
- `experiments/results/flink_w5_latency_rate40/`
- `experiments/results/kafka_streams_w5_latency_repeat1/`
- `experiments/results/flink_w5_latency_repeat1/`
- `experiments/results/latency_summary.csv`
- `experiments/results/latency_summary.md`
- `experiments/results/latency_aggregate_summary.csv`
- `experiments/results/latency_aggregate_summary.md`

## Main Files

- `src/stream_state_bench/`: workload definitions, verification logic, experiment runner, and result analyzer.
- `tests/`: unit tests for workload and verifier behavior.
- `src/stream_state_bench/verify_external_output.py`: JSONL verifier for Flink or Kafka Streams output records.
- `src/stream_state_bench/kafka_latency_probe.py`: fixed-rate Kafka latency probe with decomposed T0-T3 timestamp extraction used by the W1-W5 latency runs.
- `src/stream_state_bench/summarize_latency_results.py`: latency result summarizer and p99 aggregate table generator.
- `experiments/kafka_streams_w1/`: Kafka Streams W1-W5 app and Docker Compose workflow.
- `experiments/flink_w1/`: Flink W1-W5 app and Docker Compose workflow.
- `paper/final_paper.md`: evidence-limited paper draft.
- `docs/reproducibility.md`: commands and artifact scope.
- `docs/claim_evidence_map.md`: claims mapped to current evidence.
- `docs/rubric_assessment.md`: current rubric assessment.
- `docs/project_log.md`: project changes, failures, and limitations.
