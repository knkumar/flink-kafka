# Benchmark Fairness Configuration

This document records the exact configuration values across Kafka Streams and Flink to demonstrate equivalent event-time semantics and state-matched resource budgets.

## 1. Kafka Topics
* **Partition Counts:** 16 (for all input and output topics)
* **Replication Factor:** 1 (single broker setup for benchmarks)

## 2. Parallelism and Concurrency
* **Kafka Streams:**
  * `num.stream.threads`: 4
  * Tasks: Determined by partition count (16 tasks distributed across 4 threads)
* **Flink:**
  * `parallelism.default`: 4 (sources, operators, and sinks match Kafka Streams thread count)

## 3. State Backend and Checkpointing
* **Flink:**
  * **State Backend:** RocksDB (`state.backend: rocksdb`)
  * **Incremental Checkpointing:** Enabled (`state.backend.incremental: true`)
  * **Checkpoint Storage:** FileSystem (`state.checkpoints.dir: file:///flink/checkpoints`)
  * **RocksDB Config:** Default managed memory limits
* **Kafka Streams:**
  * **Cache Size:** `cache.max.bytes.buffering = 10485760` (10 MB per thread by default)
  * **Suppression Buffer:** Memory limits applied per suppression operator
  * **Window Grace / Retention:** Grace period set to match Flink's allowed lateness. Retention set to 1 day.

## 4. Watermarks and Time Semantics
* **Watermark Generation:** 
  * Flink: BoundedOutOfOrderness watermarks with 500ms delay.
  * Kafka Streams: Wall-clock / stream time progression based on event timestamps.
* **Idle-Partition Handling:** 
  * Flink: `table.exec.source.idle-timeout` / `WatermarkStrategy.withIdleness` configured to prevent stalled partitions.
  * Kafka Streams: `max.task.idle.ms` configured to align task progress.

## 5. Client Properties
* **Producer:**
  * `linger.ms`: 0 (or matched if optimized)
  * `batch.size`: 16384 (16 KB)
  * `acks`: `all`
  * `compression.type`: `none` (or `lz4`/`snappy` if matched)
  * `retries`: Integer.MAX_VALUE with idempotence enabled
* **Consumer:**
  * `fetch.min.bytes`: 1
  * `max.poll.records`: 500
* **Transactions:**
  * Flink: `delivery.guarantee: exactly-once` (uses Kafka transactions).
  * Kafka Streams: `processing.guarantee="exactly_once_v2"`.

## 6. Restart Policies
* **Flink:** `restart-strategy: fixed-delay` with 10s delay.
* **Kafka Streams:** Default thread exception handler (replace thread / restart app).

## Equivalency and Differences
Both engines are configured for exactly-once processing using Kafka transactions (`acks=all`, `exactly_once_v2` / `exactly-once`). Parallelism is matched (4 threads / parallelism=4). Memory budgets for RocksDB and Kafka Streams caches are aligned via container limits. Event-time processing relies on embedded timestamps, with aligned grace periods for late arrivals.
