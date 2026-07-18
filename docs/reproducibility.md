# Reproducibility Instructions

Current date checked: `2026-07-15T15:46:48-07:00`.

## Scope

These instructions reproduce the current local semantic harness, Kafka Streams W1-W5 runs, Flink W1-W5 runs, one repeated correctness pass for each engine/workload pair, W1-W5 host-side latency proxy runs for Kafka Streams and Flink, and W1-W5 latency sweeps at 10/sec, 20/sec, and 40/sec. They do not reproduce object-store checkpoints, resource-cost attribution, or failure-injection experiments because those systems are not implemented in this repository yet.

## Requirements

Local path:

- Python 3.11 or newer.
- GNU Make.

Container path:

- Docker or a compatible container runtime.

No Python packages are installed from the network. The harness uses the Python standard library.

## Local Reproduction

From the repository root:

```bash
make check
```

This command runs:

```bash
./scripts/run-local-check.sh
```

The script runs the unit tests, the local semantic experiment, and the result analyzer.

## Verifying External Engine Output

Flink or Kafka Streams jobs should write one JSON object per output record using this schema:

```json
{"output_id":"a","key":1,"window_start_ms":null,"window_end_ms":null,"value":10,"source_event_ids":["a"]}
```

Verify the file against the deterministic reference for the same workload and input parameters:

```bash
PYTHONPATH=src python3 -m stream_state_bench.verify_external_output --workload identity --actual-jsonl path/to/actual.jsonl --events 1000 --keys 100 --seed 7
```

The command exits with status 0 only when no missing, unexpected, or duplicate output records are found.

Expected output:

- 20 unit tests pass.
- `experiments/results/local_semantic_results.json` contains five workload entries.
- Every workload entry has `verification.passed: true`.
- The generated summary files are `experiments/results/local_semantic_summary.csv` and `experiments/results/local_semantic_summary.md`.

## Container Reproduction

Build and run:

```bash
docker build -t stream-state-bench .
docker run --rm stream-state-bench
```

The container runs `make check` as its default command.

## Kafka Streams W1 Reproduction

Run:

```bash
make kafka-streams-w1
```

This command:

- Generates 1000 deterministic W1 input events with 100 keys and seed 7.
- Starts `apache/kafka:4.3.1` in single-node KRaft mode.
- Builds a Java 17 Kafka Streams application using `org.apache.kafka:kafka-streams:4.3.1`.
- Creates `bench-w1-input` and `bench-w1-output`.
- Runs the Kafka Streams identity app with `processing.guarantee=exactly_once_v2`.
- Consumes output with `read_committed`.
- Verifies the output JSONL file against the deterministic W1 reference.

Expected verification file:

```json
{
  "workload": "identity",
  "events": 1000,
  "keys": 100,
  "seed": 7,
  "verification": {
    "expected_count": 1000,
    "actual_count": 1000,
    "missing_count": 0,
    "unexpected_count": 0,
    "duplicate_count": 0,
    "passed": true
  }
}
```

The run metadata is written to `experiments/results/kafka_streams_w1/run_metadata.json`; logs are written to `experiments/results/kafka_streams_w1/docker-compose.log`.

## Kafka Streams W2 Reproduction

Run:

```bash
make kafka-streams-w2
```

This command uses the same Kafka and Kafka Streams versions as W1, but sets `WORKLOAD=filter_map` and writes to `bench-w2-input` and `bench-w2-output`.

Expected verification file:

```json
{
  "workload": "filter_map",
  "events": 1000,
  "keys": 100,
  "seed": 7,
  "verification": {
    "expected_count": 513,
    "actual_count": 513,
    "missing_count": 0,
    "unexpected_count": 0,
    "duplicate_count": 0,
    "passed": true
  }
}
```

The run metadata is written to `experiments/results/kafka_streams_w2/run_metadata.json`; logs are written to `experiments/results/kafka_streams_w2/docker-compose.log`.

## Kafka Streams W3 Reproduction

Run:

```bash
make kafka-streams-w3
```

This command uses the same Kafka and Kafka Streams versions as W1/W2, but sets `WORKLOAD=tumbling_count` and writes to `bench-w3-input` and `bench-w3-output`. The producer sends 1000 benchmark events plus one `__tick__` event in `producer_input.tsv`; the Kafka Streams job sends the tick through the window operator under a sentinel key so suppressed final window results are emitted, then filters the sentinel key before writing output.

Expected verification file:

```json
{
  "workload": "tumbling_count",
  "events": 1000,
  "keys": 100,
  "seed": 7,
  "verification": {
    "expected_count": 199,
    "actual_count": 199,
    "missing_count": 0,
    "unexpected_count": 0,
    "duplicate_count": 0,
    "passed": true
  }
}
```

The run metadata is written to `experiments/results/kafka_streams_w3/run_metadata.json`; logs are written to `experiments/results/kafka_streams_w3/docker-compose.log`.

## Kafka Streams W4 Reproduction

Run:

```bash
make kafka-streams-w4
```

This command uses `WORKLOAD=sliding_sum`, `START_MS=600000`, and writes to `bench-w4-input` and `bench-w4-output`. The nonzero start timestamp avoids negative window starts, matching Kafka Streams hopping-window behavior. The producer sends 1000 benchmark events plus one `__tick__` event in `producer_input.tsv`; the Kafka Streams job sends the tick through the hopping-window operator under a sentinel key so suppressed final window results are emitted, then filters the sentinel key before writing output.

Expected verification file:

```json
{
  "workload": "sliding_sum",
  "events": 1000,
  "keys": 100,
  "seed": 7,
  "start_ms": 600000,
  "verification": {
    "expected_count": 1099,
    "actual_count": 1099,
    "missing_count": 0,
    "unexpected_count": 0,
    "duplicate_count": 0,
    "passed": true
  }
}
```

The run metadata is written to `experiments/results/kafka_streams_w4/run_metadata.json`; logs are written to `experiments/results/kafka_streams_w4/docker-compose.log`.

## Kafka Streams W5 Reproduction

Run:

```bash
make kafka-streams-w5
```

This command uses `WORKLOAD=stream_stream_join`, `START_MS=1000`, and writes to `bench-w5-left-input`, `bench-w5-right-input`, and `bench-w5-output`. The generator writes 1000 left records and 1000 right records from `paired_join_events`. The nonzero start timestamp avoids negative event timestamps from right-side skew. Kafka Streams uses a KStream-KStream inner join with a 600000 ms no-grace join window.

Expected verification file:

```json
{
  "workload": "stream_stream_join",
  "events": 1000,
  "keys": 100,
  "seed": 7,
  "start_ms": 1000,
  "verification": {
    "expected_count": 11024,
    "actual_count": 11024,
    "missing_count": 0,
    "unexpected_count": 0,
    "duplicate_count": 0,
    "passed": true
  }
}
```

The run metadata is written to `experiments/results/kafka_streams_w5/run_metadata.json`; logs are written to `experiments/results/kafka_streams_w5/docker-compose.log`.

## Flink W1 Reproduction

Run:

```bash
make flink-w1
```

This command:

- Generates 1000 deterministic W1 input events with 100 keys and seed 7.
- Starts `apache/kafka:4.3.1` in single-node KRaft mode.
- Builds a Java 17 Flink application using Flink `2.2.0` and `flink-connector-kafka` `5.0.0-2.2`.
- Creates `bench-w1-input` and `bench-w1-flink-output`.
- Produces the input records, then runs a bounded Flink identity job.
- Writes Kafka sink output with Flink's Kafka sink `EXACTLY_ONCE` delivery guarantee.
- Consumes output with `read_committed`.
- Verifies the output JSONL file against the deterministic W1 reference.

Expected verification file:

```json
{
  "workload": "identity",
  "events": 1000,
  "keys": 100,
  "seed": 7,
  "verification": {
    "expected_count": 1000,
    "actual_count": 1000,
    "missing_count": 0,
    "unexpected_count": 0,
    "duplicate_count": 0,
    "passed": true
  }
}
```

The run metadata is written to `experiments/results/flink_w1/run_metadata.json`; logs are written to `experiments/results/flink_w1/docker-compose.log`.

## Flink W2 Reproduction

Run:

```bash
make flink-w2
```

This command uses the same Kafka, Flink, and connector versions as W1, but sets `WORKLOAD=filter_map` and writes to `bench-w2-input` and `bench-w2-flink-output`.

Expected verification file:

```json
{
  "workload": "filter_map",
  "events": 1000,
  "keys": 100,
  "seed": 7,
  "verification": {
    "expected_count": 513,
    "actual_count": 513,
    "missing_count": 0,
    "unexpected_count": 0,
    "duplicate_count": 0,
    "passed": true
  }
}
```

The run metadata is written to `experiments/results/flink_w2/run_metadata.json`; logs are written to `experiments/results/flink_w2/docker-compose.log`.

## Flink W3 Reproduction

Run:

```bash
make flink-w3
```

This command uses the same Kafka, Flink, and connector versions as W1/W2, but sets `WORKLOAD=tumbling_count` and writes to `bench-w3-input` and `bench-w3-flink-output`. The producer sends 1000 benchmark events plus one `__tick__` event in `producer_input.tsv`; the Flink job assigns event-time timestamps from the input payload, ignores the tick before business aggregation, and emits finite tumbling-window results.

Expected verification file:

```json
{
  "workload": "tumbling_count",
  "events": 1000,
  "keys": 100,
  "seed": 7,
  "verification": {
    "expected_count": 199,
    "actual_count": 199,
    "missing_count": 0,
    "unexpected_count": 0,
    "duplicate_count": 0,
    "passed": true
  }
}
```

The run metadata is written to `experiments/results/flink_w3/run_metadata.json`; logs are written to `experiments/results/flink_w3/docker-compose.log`.

## Flink W4 Reproduction

Run:

```bash
make flink-w4
```

This command uses `WORKLOAD=sliding_sum`, `START_MS=600000`, and writes to `bench-w4-input` and `bench-w4-flink-output`. The producer sends 1000 benchmark events plus one `__tick__` event in `producer_input.tsv`; the Flink job assigns event-time timestamps from the input payload, ignores the tick before business aggregation, and emits finite sliding-window results.

Expected verification file:

```json
{
  "workload": "sliding_sum",
  "events": 1000,
  "keys": 100,
  "seed": 7,
  "start_ms": 600000,
  "verification": {
    "expected_count": 1099,
    "actual_count": 1099,
    "missing_count": 0,
    "unexpected_count": 0,
    "duplicate_count": 0,
    "passed": true
  }
}
```

The run metadata is written to `experiments/results/flink_w4/run_metadata.json`; logs are written to `experiments/results/flink_w4/docker-compose.log`.

## Flink W5 Reproduction

Run:

```bash
make flink-w5
```

This command uses `WORKLOAD=stream_stream_join`, `START_MS=1000`, and writes to `bench-w5-left-input`, `bench-w5-right-input`, and `bench-w5-flink-output`. Flink reads the left and right topics as bounded Kafka sources, assigns event-time timestamps from the payload with 2 seconds of bounded out-of-orderness, and applies an interval join with bounds `[-600000 ms, 600000 ms]`.

Expected verification file:

```json
{
  "workload": "stream_stream_join",
  "events": 1000,
  "keys": 100,
  "seed": 7,
  "start_ms": 1000,
  "verification": {
    "expected_count": 11024,
    "actual_count": 11024,
    "missing_count": 0,
    "unexpected_count": 0,
    "duplicate_count": 0,
    "passed": true
  }
}
```

The run metadata is written to `experiments/results/flink_w5/run_metadata.json`; logs are written to `experiments/results/flink_w5/docker-compose.log`.

## Repeat Correctness Matrix

Run:

```bash
make repeat-correctness
```

This command runs Kafka Streams W1-W5 and Flink W1-W5 again with `RUN_LABEL=repeat1`. The label appends `_repeat1` to each result directory. For example, Kafka Streams W5 writes `experiments/results/kafka_streams_w5_repeat1/`, and Flink W5 writes `experiments/results/flink_w5_repeat1/`.

Observed repeat verification counts:

| Engine | W1 | W2 | W3 | W4 | W5 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Kafka Streams | 1000/1000 | 513/513 | 199/199 | 1099/1099 | 11024/11024 |
| Flink | 1000/1000 | 513/513 | 199/199 | 1099/1099 | 11024/11024 |

Each cell is `expected/actual`. The repeat verification files reported zero missing, zero unexpected, zero duplicate records, and `passed: true`.

Summarize baseline plus repeat results:

```bash
make engine-summary
```

Generated summary files:

- `experiments/results/engine_correctness_summary.csv`
- `experiments/results/engine_correctness_summary.md`

The current summary contains 20 rows: two runs for each of five workloads across two engines. These rows are correctness checks only. They do not include throughput, p99 visibility delay, resource cost, or recovery measurements.

## W1-W5 Latency Probes

Run:

```bash
make kafka-streams-w1-latency
make flink-w1-latency
make kafka-streams-w2-latency
make flink-w2-latency
make kafka-streams-w3-latency
make flink-w3-latency
make kafka-streams-w4-latency
make flink-w4-latency
make kafka-streams-w5-latency
make flink-w5-latency
```

These commands start Kafka in KRaft mode, start the selected engine app, write input records at 20 records/sec through `kafka-console-producer.sh`, and read committed output records concurrently through `kafka-console-consumer.sh`. The probe records host epoch time after each input line is flushed to the producer process (t0), Kafka LogAppendTime (t1), engine-injected wall-clock processing time (t2), and host epoch time when each output line is read from the consumer process (t3). W2 produces 56 output records from the 100 generated input records because odd payloads are filtered. W3 produces 71 final tumbling-window output records from 100 benchmark input records; W4 produces 710 final sliding-window output records from 100 benchmark input records with `START_MS=600000`; W5 produces 186 join output records from 100 left and 100 right input records with `START_MS=1000`. The W3 and W4 producers send one extra tick record to close finite windows. The W5 probe writes the left and right inputs to separate Kafka topics.

The Flink latency runner sets `SOURCE_BOUNDED=false` so the job can consume records produced after startup. The finite Flink correctness runners use the default `SOURCE_BOUNDED=true`.

Observed result:

| Engine | Workload | Rate | Input records | Output records | Correctness | p50 ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| Kafka Streams 4.3.1 | identity | 20/sec | 100 | 100 | 100/100, zero mismatches | 1332.012 | 2631.678 | 2831.634 |
| Flink 2.2.0 | identity | 20/sec | 100 | 100 | 100/100, zero mismatches | 951.127 | 2393.965 | 2593.946 |
| Kafka Streams 4.3.1 | filter_map | 20/sec | 100 | 56 | 56/56, zero mismatches | 1226.708 | 2843.058 | 2993.158 |
| Flink 2.2.0 | filter_map | 20/sec | 100 | 56 | 56/56, zero mismatches | 1400.690 | 3200.614 | 3350.702 |
| Kafka Streams 4.3.1 | tumbling_count | 20/sec | 100 + tick | 71 | 71/71, zero mismatches | 3973.097 | 6873.202 | 7123.280 |
| Flink 2.2.0 | tumbling_count | 20/sec | 100 + tick | 71 | 71/71, zero mismatches | 2167.446 | 5068.230 | 5317.375 |
| Kafka Streams 4.3.1 | sliding_sum | 20/sec | 100 + tick | 710 | 710/710, zero mismatches | 3964.038 | 6864.071 | 7113.249 |
| Flink 2.2.0 | sliding_sum | 20/sec | 100 + tick | 710 | 710/710, zero mismatches | 2638.442 | 5538.424 | 5786.186 |
| Kafka Streams 4.3.1 | stream_stream_join | 20/sec | 100 left + 100 right | 186 | 186/186, zero mismatches | 2544.645 | 3896.173 | 4096.137 |
| Flink 2.2.0 | stream_stream_join | 20/sec | 100 left + 100 right | 186 | 186/186, zero mismatches | 805.600 | 2321.768 | 2521.762 |

Run the W1 sweep and repeat:

```bash
make w1-latency-sweep
```

This command runs Kafka Streams and Flink W1 identity at 10 records/sec and 40 records/sec, then runs one additional 20 records/sec W1 repeat with `RUN_LABEL=repeat1`. The existing baseline W1 latency runs provide the first 20 records/sec data point.

Observed W1 sweep and repeat result:

| Engine | Rate | Run label | Correctness | p50 ms | p95 ms | p99 ms |
| --- | ---: | --- | --- | ---: | ---: | ---: |
| Kafka Streams 4.3.1 | 10/sec | rate10 | 100/100, zero mismatches | 1022.525 | 2334.453 | 2734.456 |
| Kafka Streams 4.3.1 | 20/sec | baseline | 100/100, zero mismatches | 1332.012 | 2631.678 | 2831.634 |
| Kafka Streams 4.3.1 | 20/sec | repeat1 | 100/100, zero mismatches | 1226.389 | 2633.999 | 2833.992 |
| Kafka Streams 4.3.1 | 40/sec | rate40 | 100/100, zero mismatches | 1704.546 | 2828.505 | 2928.505 |
| Flink 2.2.0 | 10/sec | rate10 | 100/100, zero mismatches | 1260.707 | 2060.664 | 2460.697 |
| Flink 2.2.0 | 20/sec | baseline | 100/100, zero mismatches | 951.127 | 2393.965 | 2593.946 |
| Flink 2.2.0 | 20/sec | repeat1 | 100/100, zero mismatches | 948.056 | 2447.962 | 2647.962 |
| Flink 2.2.0 | 40/sec | rate40 | 100/100, zero mismatches | 1990.019 | 3114.001 | 3214.316 |

The aggregate table in `experiments/results/latency_aggregate_summary.md` reports two W1 runs per engine at 20 records/sec. Kafka Streams W1 p99 was 2831.634 ms and 2833.992 ms, with mean p99 2832.813 ms. Flink W1 p99 was 2593.946 ms and 2647.962 ms, with mean p99 2620.954 ms.

Run the W2 sweep and repeat:

```bash
make w2-latency-sweep
```

This command runs Kafka Streams and Flink W2 filter/map at 10 records/sec and 40 records/sec, then runs one additional 20 records/sec W2 repeat with `RUN_LABEL=repeat1`. The existing baseline W2 latency runs provide the first 20 records/sec data point.

Observed W2 sweep and repeat result:

| Engine | Rate | Run label | Correctness | p50 ms | p95 ms | p99 ms |
| --- | ---: | --- | --- | ---: | ---: | ---: |
| Kafka Streams 4.3.1 | 10/sec | rate10 | 56/56, zero mismatches | 1221.640 | 2526.477 | 2825.823 |
| Kafka Streams 4.3.1 | 20/sec | baseline | 56/56, zero mismatches | 1226.708 | 2843.058 | 2993.158 |
| Kafka Streams 4.3.1 | 20/sec | repeat1 | 56/56, zero mismatches | 1265.364 | 2665.192 | 2814.333 |
| Kafka Streams 4.3.1 | 40/sec | rate40 | 56/56, zero mismatches | 1765.886 | 2890.619 | 2964.982 |
| Flink 2.2.0 | 10/sec | rate10 | 56/56, zero mismatches | 1332.854 | 2332.809 | 2632.060 |
| Flink 2.2.0 | 20/sec | baseline | 56/56, zero mismatches | 1400.690 | 3200.614 | 3350.702 |
| Flink 2.2.0 | 20/sec | repeat1 | 56/56, zero mismatches | 900.849 | 2611.810 | 2761.075 |
| Flink 2.2.0 | 40/sec | rate40 | 56/56, zero mismatches | 2088.450 | 3213.160 | 3287.256 |

The aggregate table in `experiments/results/latency_aggregate_summary.md` reports two W2 runs per engine at 20 records/sec. Kafka Streams W2 p99 was 2993.158 ms and 2814.333 ms, with mean p99 2903.745 ms. Flink W2 p99 was 3350.702 ms and 2761.075 ms, with mean p99 3055.889 ms.

Run the W3 sweep and repeat:

```bash
make w3-latency-sweep
```

This command runs Kafka Streams and Flink W3 tumbling count at 10 records/sec and 40 records/sec, then runs one additional 20 records/sec W3 repeat with `RUN_LABEL=repeat1`. The existing baseline W3 latency runs provide the first 20 records/sec data point. Each run produces 100 benchmark input records plus one tick and verifies 71 final window outputs.

Observed W3 sweep and repeat result:

| Engine | Rate | Run label | Correctness | p50 ms | p95 ms | p99 ms |
| --- | ---: | --- | --- | ---: | ---: | ---: |
| Kafka Streams 4.3.1 | 10/sec | rate10 | 71/71, zero mismatches | 5331.984 | 11132.103 | 11631.267 |
| Kafka Streams 4.3.1 | 20/sec | baseline | 71/71, zero mismatches | 3973.097 | 6873.202 | 7123.280 |
| Kafka Streams 4.3.1 | 20/sec | repeat1 | 71/71, zero mismatches | 3595.147 | 6495.271 | 6744.307 |
| Kafka Streams 4.3.1 | 40/sec | rate40 | 71/71, zero mismatches | 2368.306 | 3818.504 | 3942.612 |
| Flink 2.2.0 | 10/sec | rate10 | 71/71, zero mismatches | 4228.258 | 10028.509 | 10527.388 |
| Flink 2.2.0 | 20/sec | baseline | 71/71, zero mismatches | 2167.446 | 5068.230 | 5317.375 |
| Flink 2.2.0 | 20/sec | repeat1 | 71/71, zero mismatches | 2509.091 | 5409.743 | 5658.188 |
| Flink 2.2.0 | 40/sec | rate40 | 71/71, zero mismatches | 2077.073 | 3527.950 | 3651.222 |

The aggregate table in `experiments/results/latency_aggregate_summary.md` reports two W3 runs per engine at 20 records/sec. Kafka Streams W3 p99 was 7123.280 ms and 6744.307 ms, with mean p99 6933.793 ms. Flink W3 p99 was 5317.375 ms and 5658.188 ms, with mean p99 5487.782 ms.

Run the W4 sweep and repeat:

```bash
make w4-latency-sweep
```

This command runs Kafka Streams and Flink W4 sliding sum at 10 records/sec and 40 records/sec, then runs one additional 20 records/sec W4 repeat with `RUN_LABEL=repeat1`. The existing baseline W4 latency runs provide the first 20 records/sec data point. Each run uses `START_MS=600000`, produces 100 benchmark input records plus one tick, and verifies 710 final sliding-window outputs.

Observed W4 sweep and repeat result:

| Engine | Rate | Run label | Correctness | p50 ms | p95 ms | p99 ms |
| --- | ---: | --- | --- | ---: | ---: | ---: |
| Kafka Streams 4.3.1 | 10/sec | rate10 | 710/710, zero mismatches | 4865.798 | 10665.881 | 11164.941 |
| Kafka Streams 4.3.1 | 20/sec | baseline | 710/710, zero mismatches | 3964.038 | 6864.071 | 7113.249 |
| Kafka Streams 4.3.1 | 20/sec | repeat1 | 710/710, zero mismatches | 3191.806 | 6091.807 | 6338.270 |
| Kafka Streams 4.3.1 | 40/sec | rate40 | 710/710, zero mismatches | 2433.497 | 3883.589 | 4006.722 |
| Flink 2.2.0 | 10/sec | rate10 | 710/710, zero mismatches | 4091.697 | 9891.631 | 10390.270 |
| Flink 2.2.0 | 20/sec | baseline | 710/710, zero mismatches | 2638.442 | 5538.424 | 5786.186 |
| Flink 2.2.0 | 20/sec | repeat1 | 710/710, zero mismatches | 2359.980 | 5259.953 | 5507.883 |
| Flink 2.2.0 | 40/sec | rate40 | 710/710, zero mismatches | 1965.928 | 3415.926 | 3540.110 |

The aggregate table in `experiments/results/latency_aggregate_summary.md` reports two W4 runs per engine at 20 records/sec. Kafka Streams W4 p99 was 7113.249 ms and 6338.270 ms, with mean p99 6725.760 ms. Flink W4 p99 was 5786.186 ms and 5507.883 ms, with mean p99 5647.034 ms.

Run the W5 sweep and repeat:

```bash
make w5-latency-sweep
```

This command runs Kafka Streams and Flink W5 stream-stream join at 10 records/sec and 40 records/sec, then runs one additional 20 records/sec W5 repeat with `RUN_LABEL=repeat1`. The existing baseline W5 latency runs provide the first 20 records/sec data point. Each run uses `START_MS=1000`, produces 100 left and 100 right input records, and verifies 186 join outputs.

Observed W5 sweep and repeat result:

| Engine | Rate | Run label | Correctness | p50 ms | p95 ms | p99 ms |
| --- | ---: | --- | --- | ---: | ---: | ---: |
| Kafka Streams 4.3.1 | 10/sec | rate10 | 186/186, zero mismatches | 1835.608 | 3035.445 | 3435.417 |
| Kafka Streams 4.3.1 | 20/sec | baseline | 186/186, zero mismatches | 2544.645 | 3896.173 | 4096.137 |
| Kafka Streams 4.3.1 | 20/sec | repeat1 | 186/186, zero mismatches | 2540.653 | 3898.778 | 4098.726 |
| Kafka Streams 4.3.1 | 40/sec | rate40 | 186/186, zero mismatches | 2195.370 | 3338.270 | 3438.284 |
| Flink 2.2.0 | 10/sec | rate10 | 186/186, zero mismatches | 1135.848 | 2039.220 | 2439.144 |
| Flink 2.2.0 | 20/sec | baseline | 186/186, zero mismatches | 805.600 | 2321.768 | 2521.762 |
| Flink 2.2.0 | 20/sec | repeat1 | 186/186, zero mismatches | 1547.947 | 3059.302 | 3259.235 |
| Flink 2.2.0 | 40/sec | rate40 | 186/186, zero mismatches | 1531.194 | 2705.081 | 2805.058 |

The aggregate table in `experiments/results/latency_aggregate_summary.md` reports two W5 runs per engine at 20 records/sec. Kafka Streams W5 p99 was 4096.137 ms and 4098.726 ms, with mean p99 4097.431 ms. Flink W5 p99 was 2521.762 ms and 3259.235 ms, with mean p99 2890.499 ms.

Generated files:

- `experiments/results/kafka_streams_w1_latency/actual.jsonl`
- `experiments/results/kafka_streams_w1_latency/verification.json`
- `experiments/results/kafka_streams_w1_latency/latency_summary.json`
- `experiments/results/kafka_streams_w1_latency/latency_samples.csv`
- `experiments/results/kafka_streams_w1_latency/run_metadata.json`
- `experiments/results/kafka_streams_w1_latency/docker-compose.log`
- `experiments/results/flink_w1_latency/actual.jsonl`
- `experiments/results/flink_w1_latency/verification.json`
- `experiments/results/flink_w1_latency/latency_summary.json`
- `experiments/results/flink_w1_latency/latency_samples.csv`
- `experiments/results/flink_w1_latency/run_metadata.json`
- `experiments/results/flink_w1_latency/docker-compose.log`
- `experiments/results/kafka_streams_w2_latency/`
- `experiments/results/flink_w2_latency/`
- `experiments/results/kafka_streams_w3_latency/`
- `experiments/results/flink_w3_latency/`
- `experiments/results/kafka_streams_w4_latency/`
- `experiments/results/flink_w4_latency/`
- `experiments/results/kafka_streams_w5_latency/`
- `experiments/results/flink_w5_latency/`
- `experiments/results/kafka_streams_w1_latency_rate10/`
- `experiments/results/flink_w1_latency_rate10/`
- `experiments/results/kafka_streams_w1_latency_rate40/`
- `experiments/results/flink_w1_latency_rate40/`
- `experiments/results/kafka_streams_w1_latency_repeat1/`
- `experiments/results/flink_w1_latency_repeat1/`
- `experiments/results/kafka_streams_w2_latency_rate10/`
- `experiments/results/flink_w2_latency_rate10/`
- `experiments/results/kafka_streams_w2_latency_rate40/`
- `experiments/results/flink_w2_latency_rate40/`
- `experiments/results/kafka_streams_w2_latency_repeat1/`
- `experiments/results/flink_w2_latency_repeat1/`
- `experiments/results/kafka_streams_w3_latency_rate10/`
- `experiments/results/flink_w3_latency_rate10/`
- `experiments/results/kafka_streams_w3_latency_rate40/`
- `experiments/results/flink_w3_latency_rate40/`
- `experiments/results/kafka_streams_w3_latency_repeat1/`
- `experiments/results/flink_w3_latency_repeat1/`
- `experiments/results/kafka_streams_w4_latency_rate10/`
- `experiments/results/flink_w4_latency_rate10/`
- `experiments/results/kafka_streams_w4_latency_rate40/`
- `experiments/results/flink_w4_latency_rate40/`
- `experiments/results/kafka_streams_w4_latency_repeat1/`
- `experiments/results/flink_w4_latency_repeat1/`
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

This is a host-side visibility-delay probe that decomposes latency into constituent components using T0-T3 timestamps. It includes console client, Docker exec, Kafka log append time (T1-T0), engine processing time (T2-T1), and transaction commit / consumer read time (T3-T2). For W3, W4, and W5, each output is timed from the latest sent contributing benchmark input event to the committed output read. It does not record synchronized node clocks, or enough repeats for confidence intervals. Treat it as an executable first latency comparison for W1-W5 only, not as a throughput envelope.

## Long-Duration Stability Reproduction

```bash
RATE_PER_SEC=100 DURATION_SEC=1800 scripts/run-stability-tests.sh kafka-streams w1
RATE_PER_SEC=100 DURATION_SEC=1800 scripts/run-stability-tests.sh flink w1
```

`scripts/run-stability-tests.sh <engine> <workload>` runs an open-loop producer at `RATE_PER_SEC` for `DURATION_SEC` seconds (`EVENTS = RATE_PER_SEC * DURATION_SEC`), starts `monitor_lag.py` in the background to poll consumer-group lag every 5 seconds, and writes `lag.csv`, `latency_summary.json`, and `verification.json` under `experiments/results/<engine>_<workload>_latency_stability_<rate>/`. `scripts/run-all-30m-sweeps.sh` and `scripts/resume-all-30m-sweeps.sh` chain this across all engines and workloads at the 30-minute (1800 second) duration used for the results in this paper.

**Do not run `w5` (`stream_stream_join`) at `DURATION_SEC=1800` with `RATE_PER_SEC=100`.** The join workload's expected-output cardinality grows worse than linearly with input volume under its 10-minute join window; the 180,000-event run this produces did not finish after 11 hours and produced a 3.2 GB expected-output file (see `docs/final_paper.md` Section 7 and `experiments/results/kafka_streams_w5_latency_stability_100_incomplete/`). Use a bounded duration instead, for example `DURATION_SEC=120`, which is what this paper's W5 stability figures use for both engines.

## Resource-Metrics Reproduction

```bash
python3 src/stream_state_bench/resource_monitor.py \
  --containers kafka_streams_w1-kafka-streams-identity-1 stream-state-bench-kafka \
  --output-csv experiments/results/resource_metrics/kafka_streams_w1_resource.csv \
  --interval-sec 10
```

Run this alongside any of the latency, stability, or failure scripts above (it polls `docker stats --no-stream` for the named containers on a fixed interval and appends rows to the CSV until killed or, with `--duration-sec`, until that many seconds elapse). It is a shared-Docker-daemon, cgroup-level sample, not an isolated hardware profile; see the Known Limitations below.

## Tuning-Matrix Reproduction

Both engines hardcoded their commit/checkpoint interval at 1000 ms; both now read it from an environment variable so a second configuration can be compared against the default:

```bash
COMMIT_INTERVAL_MS=10000 EVENTS=100 RATE_PER_SEC=20 WORKLOAD=tumbling_count WORKLOAD_ID=w3_latency RUN_LABEL=tuning_commit10s scripts/run-kafka-streams-w1-latency.sh
CHECKPOINT_INTERVAL_MS=10000 EVENTS=100 RATE_PER_SEC=20 WORKLOAD=tumbling_count WORKLOAD_ID=w3_latency RUN_LABEL=tuning_checkpoint10s scripts/run-flink-w1-latency.sh
```

`COMMIT_INTERVAL_MS` sets `StreamsConfig.COMMIT_INTERVAL_MS_CONFIG` in `experiments/kafka_streams_w1/src/main/java/bench/IdentityApp.java`; `CHECKPOINT_INTERVAL_MS` sets the interval passed to `env.enableCheckpointing(...)` in `experiments/flink_w1/src/main/java/bench/FlinkIdentityJob.java`. Both default to `1000` (matching every other result in this paper) when unset.

## Fault-Injection Reproduction

```bash
scripts/run-failure-test.sh kafka-streams w1 jvm_kill 20 2000
scripts/run-failure-test.sh kafka-streams w1 broker_kill 20 2000
scripts/run-failure-test.sh kafka-streams w1 node_loss 20 2000
```

`scripts/run-failure-test.sh <engine> <workload> <failure_mode> <rate_per_sec> <events>` starts the standard latency runner in the background, waits until half the run's expected duration has elapsed, then injects one of three failures into the running worker or broker container: `jvm_kill` (`docker compose kill` then `start` 5 seconds later, local state volume intact), `broker_kill` (`docker compose stop`/`start` on the Kafka container 5 seconds later), or `node_loss` (`docker compose rm -fsv` on the worker container, forcing removal of its anonymous volumes, then `docker compose up -d --no-deps` 5 seconds later to recreate it from scratch). Results land in `experiments/results/<engine>_<workload>_latency_failure_<mode>/`; `experiments/results/failure_latency_aggregated.csv` aggregates across runs found under that naming pattern.

## Experiment Parameters

Default local experiment:

- Workloads: `identity`, `filter_map`, `tumbling_count`, `sliding_sum`, `stream_stream_join`.
- Events per workload: `1000`.
- Key count: `100`.
- Random seed: `7`.
- Engine label: `local_semantic_harness`.

## Known Limitations

- The local harness uses its deterministic reference implementation as the stand-in for actual engine output.
- It does not measure p99 downstream visibility delay, backlog growth, worker resource use, Kafka broker cost, Flink checkpoint storage, Kafka Streams changelog storage, recovery time, or reprocessing.
- It validates shared W1-W5 inputs through both engines, but it does not validate recovery or tuned configuration parity.
- It has one repeated correctness pass per engine/workload pair. W1-W5 latency has one 20/sec repeat per engine, but the artifact does not compute confidence intervals.
- W1-W5 have three-rate host-side proxy sweeps and one 20/sec repeat. Calibrated T0-T3 decomposed latencies have been instrumented and tested across all W1-W5 latency sweeps.
- Backlog tracking (`monitor_lag.py`) has been added and validated via 2-minute W1 stability tests at 100 events/sec, but not yet applied across W2-W5 or longer durations.
- The Kafka Streams and Flink W1-W5 runs use one broker with transaction-log replication and ISR set to 1 for local development. This is not a production fault-tolerance configuration.
- The Flink W1-W5 runs use bounded sources over pre-produced input. They verify finite executions, not continuous streaming stability.
- W4 external runs use `START_MS=600000` so native engine windows and the verifier avoid negative window-start timestamps.
- W5 external runs use `START_MS=1000` so generated right-side skew does not produce negative event timestamps.
- The original proposal named Flink 2.3.x. The current Flink artifact uses Flink 2.2.0 because Apache's downloads page lists Kafka Connector 5.0.0 compatibility for Flink 2.1.x and 2.2.x.
