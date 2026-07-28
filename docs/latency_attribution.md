# Latency Attribution Definitions

This document details how timestamp eligibility ($T_e$) is determined across workloads for evaluating engine latency, defining which input timestamp anchors the output, and how various out-of-order scenarios are handled.

## Input Timestamp Anchor

For all stateful workloads, the output timestamp is anchored by the **maximum event time among contributing inputs**. This aligns with the latency probe's definition (`max(source_send_times)`).

- **W1 (Identity) / W2 (Filter-Map):** The single input event's timestamp is the anchor.
- **W3 (Tumbling Count) / W4 (Sliding Sum):** The anchor is the maximum event time among all events that fell into the respective window.
- **W5 (Stream-Stream Join):** The anchor is the maximum of the two joining events (left event and right event).

## $T_e$ Definition per Engine

$T_e$ represents the wall-clock eligibility time: the moment the engine theoretically has enough information to produce the result according to its watermarking or windowing rules. 

### Flink
- **W1 / W2 (Stateless):** $T_e$ is identical to $T_2$ (the engine output timestamp). There is no semantic wait.
- **W3 / W4 (Windowed):** $T_e$ is the wall-clock time when the event-time watermark first passes the window's firing boundary (i.e. `window.getEnd() + allowedLateness`). 
- **W5 (Join):** Eligibility occurs the moment both join inputs are present and the join fires. The engine evaluates the interval condition upon arrival, triggering immediately if satisfied.

### Kafka Streams
- **W1 / W2 (Stateless):** $T_e$ is identical to $T_2$ (the engine output timestamp).
- **W3 / W4 (Windowed):** $T_e$ is the wall-clock time when the stream time first passes the window end (plus grace period, if configured), triggering the window closure and the subsequent suppressed output.
- **W5 (Join):** Eligibility is reached as soon as both matching records are processed by the engine and the join output is triggered.

## Handling Late, Duplicate, and Out-of-Order Inputs
- **Late Inputs:** If an event arrives after the watermark or stream-time has advanced past the window boundary (and allowed lateness/grace period), it is discarded and does not contribute to the window's result or $T_e$.
- **Duplicate Inputs:** Handled idempotently depending on the workload characteristics. For aggregations, exact-once semantics (if configured) ensure they don't incorrectly duplicate counts.
- **Out-of-Order Inputs:** The engine buffers events until the watermark/stream-time reaches the threshold. The output anchor timestamp remains the maximum event time of all valid inputs processed, ignoring arrival order.
