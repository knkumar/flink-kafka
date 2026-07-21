# Acceptance checklist for the current draft

The paper is currently around **59/100** under the review rubric used in this discussion. To reach the likely acceptance range, it should score at least **82/100**, pass every scientific-validity gate below, and leave no critical category below 4/5.

The latest draft has already improved claim discipline, correctness replication, configuration-sensitivity replication, and treatment of censored failure trials. Its remaining blockers are the absence of a semantic-eligibility timestamp, insufficient replication of the principal sustained-load results, incomplete resource accounting, indirect recovery measurement, underdocumented configuration fairness, and lack of an identifiable immutable artifact. 

---

## A. Non-negotiable acceptance gates

### 1. Separate semantic window waiting from engine computation

* [ ] Add an eligibility timestamp (T_e) for every final window result.
* [ ] For Kafka Streams, define (T_e) as the time stream time first passes the window end plus grace.
* [ ] For Flink, define (T_e) as the time the relevant watermark first passes the firing boundary.
* [ ] For W5, formally define when a join result becomes semantically eligible.
* [ ] Replace the current combined interval with:

[
T_e-T_1=\text{semantic/event-time waiting}
]

[
T_2-T_e=\text{triggering and engine computation}
]

[
T_3-T_2=\text{transactional visibility and consumption}
]

* [ ] Report all three distributions separately.
* [ ] Remove “processing” labels from (T_2-T_1).
* [ ] Revise the title-level “state cost” claims based on (T_2-T_e), not (T_2-T_1).
* [ ] Show whether the Kafka Streams–Flink difference remains after semantic waiting is removed.
* [ ] Record raw watermark or stream-time values so that (T_e) is independently verifiable.

**Definition of done:** The principal result identifies actual engine-trigger/computation delay rather than the benchmark’s event-time progression.

---

### 2. Define latency attribution for aggregate and join outputs

* [ ] State which input timestamp anchors each W3 and W4 output.
* [ ] Choose and justify one rule, such as:

  * input with maximum event time;
  * final contributing input;
  * watermark-advancing input;
  * synthetic trigger/tick.
* [ ] Define how both input records contribute to W5 join latency.
* [ ] Include the attribution rule in the correctness oracle and raw output format.
* [ ] Explain how an output ID maps back to all contributing input IDs.
* [ ] Document how late, duplicate, or out-of-order inputs affect attribution.
* [ ] Verify that Flink and Kafka Streams use the same attribution semantics.

**Definition of done:** An independent reader can reproduce the latency of every aggregate and join result from raw records.

---

### 3. Replicate the main sustained-load experiment

* [ ] Run at least five independent 30-minute trials per engine/workload for W1–W4.
* [ ] Randomize or block the run order.
* [ ] Start every run from clean topics, state directories, checkpoints, and application identities.
* [ ] Report per-run p50, p95, and p99.
* [ ] Report median or mean across runs with confidence intervals.
* [ ] Include run-to-run dispersion.
* [ ] Publish ECDFs or complete latency-distribution plots.
* [ ] Publish lag time-series plots.
* [ ] Estimate lag trend or slope with uncertainty.
* [ ] Replace “backlog did not grow monotonically” with a quantitative stability criterion.
* [ ] Verify output correctness after every run.
* [ ] Report JVM warm-up and excluded warm-up periods, if any.

**Definition of done:** The central engine comparisons are stable across independent runs and are not based on one host-time realization.

---

### 4. Correct the tail-latency decomposition

Marginal p99 component values are not an additive decomposition of total p99 because each p99 may refer to a different result.

* [ ] Rank outputs by total (T_3-T_0).
* [ ] Select the same top-1% outputs for tail analysis.
* [ ] Report each component for those same outputs.
* [ ] Show conditional component distributions for high-total-latency outputs.
* [ ] Avoid adding marginal p99 values.
* [ ] State the exact quantile estimator.
* [ ] Report the number of observations behind every percentile.
* [ ] Handle time-series dependence with per-run inference or block bootstrap.
* [ ] Provide confidence intervals for differences between engines.
* [ ] Explain why (T_1-T_0) is consistently close to one second.
* [ ] Determine whether this interval includes producer pacing, queueing, linger, batching, retries, or flush behavior.

**Definition of done:** The paper can validly state which pipeline stage contributes to high end-to-end tail latency.

---

### 5. Make RQ3 consistent with what is measured

The current RQ3 asks about recovery time and backlog drain, while the paper states that it does not directly measure recovery time.

Choose one path.

#### Preferred path: directly measure recovery

* [ ] Record failure-injection time.
* [ ] Record process/container restart time.
* [ ] Record task assignment or consumer-group rebalance completion.
* [ ] Record state restoration start and completion.
* [ ] Record first post-recovery record processed.
* [ ] Record first correct committed output visible.
* [ ] Record the time backlog reaches zero.
* [ ] Record final correctness after quiescence.
* [ ] Report recovery time using these milestones rather than output-record p99.
* [ ] Report state size at failure.
* [ ] Report Kafka Streams changelog size and end offsets.
* [ ] Report Flink checkpoint size, age, and storage location.
* [ ] Report restore bytes, records, and throughput.
* [ ] Report whether input production continues during the fault.
* [ ] Complete five valid trials for every reported condition.
* [ ] State the exact observation timeout.
* [ ] Treat unfinished trials as right-censored observations.
* [ ] Diagnose the Kafka Streams unfinished trials from engine and broker logs.
* [ ] Distinguish slow restoration, repeated rebalance, transaction retries, identity errors, and genuine stalls.
* [ ] Do not call a timeout permanent data loss unless missing output remains after eventual recovery and quiescence.

#### Reduced-scope path

* [ ] Rewrite RQ3 to ask only which trials complete and pass final verification within a specified observation window.
* [ ] Remove “recovery time” and “backlog drain” from the RQ and contribution.
* [ ] Rename the section “Post-injection completion and correctness outcomes.”
* [ ] Avoid architectural recovery claims.
* [ ] Report censored outcomes with their exact timeout.

**Definition of done:** RQ3 asks exactly what the instrumentation can answer, and every outcome is classified correctly.

---

### 6. Report the resource metrics promised by RQ2

The current results provide only a small subset of the resource data named in RQ2.

* [ ] Report worker CPU utilization or CPU-seconds per workload.
* [ ] Report worker mean, p95, and peak memory.
* [ ] Report Kafka broker CPU and memory.
* [ ] Report engine and broker network bytes.
* [ ] Report engine and broker block-I/O bytes.
* [ ] Report Kafka changelog-topic bytes.
* [ ] Report Kafka repartition-topic bytes.
* [ ] Report Kafka transaction and restore traffic where available.
* [ ] Report Flink checkpoint size and duration.
* [ ] Report checkpoint-storage reads and writes.
* [ ] Report RocksDB state size for both engines.
* [ ] Report state-restoration I/O.
* [ ] Include Flink JobManager resources.
* [ ] Report Flink values separately for each workload instead of one blended mean.
* [ ] Record idle/no-workload baselines for both systems.
* [ ] Normalize results per input, per output, and per unit of wall-clock time where meaningful.
* [ ] Report total-system costs as well as individual-container costs.
* [ ] Include variability across independent runs.

**Definition of done:** The paper can support its title-level claim about where state cost is placed across workers, brokers, changelogs, and checkpoint storage.

**Alternative:** If these measurements cannot be completed, narrow the title and RQ2 to “container-level observations” and stop claiming to locate streaming state cost across the system.

---

### 7. Fully document comparison fairness

* [ ] Report host CPU model, physical/logical core count, RAM, disk type, and filesystem.
* [ ] Report operating system, kernel, Docker, and JVM versions.
* [ ] Report container CPU and memory limits.
* [ ] Report all Kafka topic partition counts.
* [ ] Report topic replication factors.
* [ ] Report Kafka Streams thread and task counts.
* [ ] Report Flink source, operator, and sink parallelism.
* [ ] Explicitly identify the Flink state backend.
* [ ] Report RocksDB configuration for both engines.
* [ ] Report Flink checkpoint-storage configuration.
* [ ] State whether incremental checkpoints are enabled.
* [ ] Report Kafka Streams cache size.
* [ ] Report suppression-buffer settings.
* [ ] Report window grace and retention settings.
* [ ] Report watermark generation and idle-partition handling.
* [ ] Report producer `linger`, batching, acknowledgments, compression, and retry settings.
* [ ] Report consumer fetch and polling settings.
* [ ] Report transaction settings.
* [ ] State all restart policies.
* [ ] Demonstrate equivalent event-time semantics.
* [ ] Give both engines matched resource budgets or explain differences.
* [ ] Remove the assertion that shared-host contention affects both engines equally.
* [ ] Measure host-level utilization so shared-resource interference can be assessed.

**Definition of done:** Another expert can reconstruct the deployment and judge whether guarantees, semantics, and resource allocations are comparable.

---

### 8. Publish an immutable, reviewable artifact

* [ ] Include a repository URL in the paper.
* [ ] Include an immutable commit hash.
* [ ] Create an archival release or DOI.
* [ ] Pin container images by digest.
* [ ] Provide one-command environment setup.
* [ ] Provide one command for each central experiment.
* [ ] Include all engine and Kafka configuration files.
* [ ] Include raw (T_0,T_1,T_e,T_2,T_3) observations.
* [ ] Include raw lag traces.
* [ ] Include raw resource samples.
* [ ] Include all fault-injection logs.
* [ ] Include successful, failed, and censored trials.
* [ ] Provide scripts that regenerate every table and figure.
* [ ] Provide a manifest mapping each result in the paper to raw data and a command.
* [ ] Add checks for stale topics, checkpoints, volumes, offsets, and application identities.
* [ ] State required runtime, storage, CPU, and memory.
* [ ] Demonstrate reproduction from a clean machine or virtual environment.

**Definition of done:** A reviewer can regenerate every headline result without undocumented intervention.

---

## B. Scope decisions that must be resolved

### 9. Decide whether W5 is a primary workload

#### Keep W5 as a central workload

* [ ] Redesign the join to control selectivity.
* [ ] Bound the join window relative to run duration.
* [ ] Report input and output cardinalities.
* [ ] Develop a scalable correctness oracle.
* [ ] Run W5 under a sustained protocol comparable to W1–W4.
* [ ] Include W5 in resource measurements.
* [ ] Include W5 in failure testing.
* [ ] Define two-input latency attribution.
* [ ] Repeat the experiment independently.

#### Narrow the scope

* [ ] Remove W5 from the headline comparative claims.
* [ ] State that the main findings cover stateless transforms and final-only windowed aggregations.
* [ ] Retain W5 only as a benchmark-design or correctness case study.
* [ ] Avoid suggesting that the main latency and recovery results generalize to joins.

**Definition of done:** W5 is either experimentally comparable or clearly outside the principal evidence.

---

### 10. Decide whether the paper is fixed-load or performance-envelope work

The current fixed-load scope is acceptable if consistently maintained.

* [ ] Keep RQ1 explicitly limited to 100 events/s on the documented host.
* [ ] Remove saturation, sustainable-throughput, or capacity-envelope implications.
* [ ] State that neither engine was tested near its throughput limit.
* [ ] Do not interpret small 10/20/40 events/s finite-input results as scaling behavior.
* [ ] Keep conclusions conditional on the evaluated workload, rate, state size, and hardware.

Alternatively:

* [ ] Complete a load sweep through saturation.
* [ ] Define bounded latency and sustainable throughput.
* [ ] Verify correctness at every rate.
* [ ] Repeat the sweep for several state sizes, partition counts, and parallelism levels.
* [ ] Measure backlog growth and drain quantitatively.

**Definition of done:** The contribution and conclusions match either a fixed-load study or a real capacity-envelope study, but do not mix the two.

---

### 11. Align the title with the completed evidence

Retain **“Where Does Streaming State Cost Go?”** only if the paper adds:

* [ ] (T_e)-based computation isolation.
* [ ] Worker and broker resource accounting.
* [ ] Changelog and checkpoint-storage measurements.
* [ ] Direct recovery instrumentation.
* [ ] Repeated sustained-load evidence.

Otherwise, use a narrower title such as:

> **Fixed-Load Visibility and Fault-Injection Observations for Flink and Kafka Streams**

or:

> **A Reproducible Fixed-Load Comparison of Flink and Kafka Streams**

**Definition of done:** The title makes no claim broader than the actual measurements.

---

## C. Configuration-sensitivity checklist

### 12. Strengthen the tuning result

* [ ] Test at least three commit/checkpoint intervals rather than only two.
* [ ] Use at least five independent trials per interval.
* [ ] Run tuning experiments at sustained scale.
* [ ] Report per-run results and confidence intervals.
* [ ] Repeat the Kafka Streams interval experiment with caching disabled.
* [ ] Independently vary cache size.
* [ ] Record stream time.
* [ ] Record suppression release.
* [ ] Record cache flushes and task commits.
* [ ] Record Flink checkpoint start and completion.
* [ ] Record sink precommit and Kafka transaction commit.
* [ ] Avoid claiming a specific internal mechanism without direct instrumentation.
* [ ] Show whether the effect persists after separating (T_e-T_1) from (T_2-T_e).

**Definition of done:** The paper demonstrates not only configuration sensitivity but also which mechanism moves which latency stage.

---

## D. Writing and presentation checklist

### 13. Fix consistency and typesetting

* [ ] Correct visible LaTeX artifacts such as `extttT2-T1` and `extttT_e`.
* [ ] Use one notation style consistently: (T_0,T_1,T_e,T_2,T_3).
* [ ] Replace every (T_2-T_1) “processing” label with an accurate label until (T_e) is implemented.
* [ ] Give every table and figure a numbered caption.
* [ ] Ensure engine and workload names do not concatenate or wrap ambiguously.
* [ ] State units in all headings.
* [ ] State sample sizes in every table or caption.
* [ ] State whether values are single-run, pooled, mean, or median-of-runs.
* [ ] Add latency ECDFs.
* [ ] Add lag-over-time figures.
* [ ] Add resource-utilization figures.
* [ ] Add recovery-timeline figures.
* [ ] Keep terminology consistent between RQs, methods, results, discussion, and conclusion.
* [ ] Remove causal words such as “because,” “controls,” or “gates” unless directly measured.
* [ ] Avoid “recovery cost” where only output-delay observations are available.
* [ ] Avoid generalizing beyond one shared-host deployment.
* [ ] Ensure the abstract reports only results that are fully supported later.

---

### 14. Strengthen related work and novelty

* [ ] Compare directly against prior latency-attribution studies.
* [ ] Cover exactly-once processing overhead literature.
* [ ] Cover checkpoint and transactional-sink latency work.
* [ ] Cover state-restoration and fault-recovery benchmarking.
* [ ] Explain precisely what T0–(T_e)–T3 instrumentation adds beyond prior benchmarks.
* [ ] Distinguish workload-suite novelty from measurement-methodology novelty.
* [ ] Avoid broad claims that prior studies report only one aggregate number unless comprehensively substantiated.
* [ ] State which component is the principal contribution:

  * benchmark artifact;
  * latency methodology;
  * resource-cost analysis;
  * recovery analysis.

**Definition of done:** The novelty claim is precise and positioned against the closest methodology, not only general stream-processing benchmarks.

---

# Final go/no-go test

Before submission, every answer below should be **yes**:

* [ ] Does the paper separate semantic waiting from actual engine computation?
* [ ] Is aggregate and join latency attribution formally defined?
* [ ] Are the main W1–W4 results based on repeated independent runs?
* [ ] Are uncertainty and complete latency distributions reported?
* [ ] Are tail components analyzed for the same high-latency records?
* [ ] Does RQ2 report the worker and broker metrics it asks about?
* [ ] Does RQ3 match the recovery information actually measured?
* [ ] Are unfinished trials treated as censored rather than as data loss?
* [ ] Is the exact failure-observation timeout reported?
* [ ] Are recovery milestones and state sizes available?
* [ ] Are all fairness-critical configurations documented?
* [ ] Is W5 either comparable or explicitly outside the main claims?
* [ ] Is the artifact immutable, accessible, and able to regenerate the paper?
* [ ] Does the title accurately describe the completed evaluation?
* [ ] Are all tables, terminology, and numerical claims synchronized?
* [ ] Does every critical rubric category score at least 4/5?
* [ ] Does the estimated total score reach at least 82/100?

## Most direct path from 59 to acceptance

The highest-value sequence is:

1. Implement (T_e) and formal output attribution.
2. Rerun W3/W4 sustained and tuning experiments with at least five independent trials.
3. Add direct fault-recovery milestones and diagnose censored Kafka Streams trials.
4. Report full per-workload worker and broker resource measurements.
5. Freeze and publish complete configurations and an immutable artifact.
6. Narrow W5, RQ3, and the title wherever the completed evidence remains limited.
7. Rebuild the paper’s tables, figures, abstract, and conclusion from one final result manifest.

Completing these items should plausibly move the paper into approximately the **82–88/100 range**, assuming the new measurements preserve a clear and statistically supported contribution.
