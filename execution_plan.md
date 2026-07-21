# Execution Plan — Move the Paper from ~59/100 into the 82–88/100 Acceptance Range

> **For agentic workers:** Steps use checkbox (`- [ ]`) syntax for tracking. Implement task-by-task; each task ends with an independently verifiable deliverable and cites the `review_plan.md` gate it closes. Where a step runs Docker experiments, the "test" is verifier PASS + expected row/sample counts, not a unit test.

**Goal:** Close every non-negotiable acceptance gate in `review_plan.md` (§A.1–A.8), resolve the three scope decisions (§B.9–B.11), strengthen the tuning result (§C.12), and rebuild the paper so its claims, tables, figures, and title match the completed evidence.

**Architecture:** The repo already contains most harness plumbing — a calibrated `T0/T1/T2/T3` probe (`src/stream_state_bench/kafka_latency_probe.py`) with a derived `l_closure` proxy, five-trial fault-injection directories, a resource sampler (`src/stream_state_bench/resource_monitor.py`), a recovery-timeline extractor (`scripts/extract-recovery-timeline.py`), and saturation/partition/state-size sweep scripts. The plan's job is to (a) add a *real* watermark/stream-time `T_e` to the two engine jobs and the probe, (b) run the sustained and tuning experiments to a five-trial statistical standard, (c) turn the recovery and resource harnesses into reported milestones, (d) freeze an immutable artifact with a result manifest, and (e) regenerate the paper from that manifest.

**Tech Stack:** Apache Kafka 4.3.1 (KRaft), Kafka Streams 4.3.1, Apache Flink 2.2.0 + flink-connector-kafka 5.0.0-2.2, Docker Compose, Python 3.11 (pandas), pandoc + pdflatex.

## Global Constraints

- Engine/version pins are fixed: Kafka 4.3.1, Kafka Streams 4.3.1, Flink 2.2.0, connector 5.0.0-2.2. Do not bump without recording compatibility evidence in `docs/project_log.md`.
- Every reported number must trace to a directory under `experiments/results/`. No hand-entered values in the paper.
- Every sustained/tuning/recovery/resource cell requires **five independent trials** started from clean topics, state dirs, checkpoints, and application IDs. Reuse the existing `_trialN` directory naming.
- Flink runs MUST clear `experiments/flink_w1/checkpoints/` at the start of every invocation (the host bind-mount bug documented 2026-07-17); confirm the run script still does this before trusting any windowed/stability result.
- Notation is `T0,T1,T_e,T2,T3` everywhere (prose, code fields, tables, captions). No `extttT...` LaTeX artifacts.
- Treat unfinished trials as right-censored with the exact observation timeout reported — never as "data loss" unless output is still missing after eventual quiescence.
- Keep `.agents/MEMORY.md` updated per phase (PLANS/DECISIONS/PROGRESS/DISCOVERIES/OUTCOMES with ISO timestamps and provenance tags), per the global CLAUDE.md workflow.

---

## Phase 0 — Freeze the baseline and scaffold the result manifest

### Task 0.1: Branch, snapshot, and clean stale artifacts

**Files:**
- Modify (delete/relocate): `experiments/results/*_buggy_run*`, `*_incomplete`
- Create: `docs/results_manifest.csv`

- [ ] **Step 1:** Create a working branch off `master`: `git checkout -b paper-acceptance-pass`.
- [ ] **Step 2:** Enumerate stale/known-bad result dirs so they are never cited: `ls experiments/results/ | grep -iE "buggy_run|incomplete|_trial1$" | sort`. Move `*buggy_run*` and `*incomplete*` under `experiments/results/_archived/` (keep for provenance, out of the citeable set).
- [ ] **Step 3:** Create `docs/results_manifest.csv` with header `paper_element,claim,result_dir,command,raw_files,n_trials,notes`. This is the single source of truth that §A.8 requires; every later task appends its rows here.
- [ ] **Step 4:** Commit: `git add -A && git commit -m "chore: branch, archive stale results, scaffold result manifest"`.

**Definition of done:** A clean branch exists; no `buggy_run`/`incomplete` directory sits in the citeable result set; `docs/results_manifest.csv` exists and is empty except its header. (Supports §A.8.)

---

## Phase 1 — Semantic eligibility `T_e` and formal latency attribution

This is the highest-value work (`review_plan.md` §A.1, §A.2, and the "most direct path" item 1). Everything downstream re-reports against `T_e`.

### Task 1.1: Emit watermark / stream-time into the output payload from both engines

**Files:**
- Modify: `experiments/flink_w1/src/main/java/bench/FlinkIdentityJob.java`
- Modify: `experiments/kafka_streams_w1/src/main/java/bench/IdentityApp.java`

**Interfaces:**
- Produces: two new output-payload fields — `t_e_ms` (long, ms since epoch) and `wm_ms` (long, the watermark/stream-time value that made the result eligible). Consumed by Task 1.2's probe and Task 1.3's attribution.

- [ ] **Step 1:** In `FlinkIdentityJob.java`, in the windowed branches (W3 tumbling, W4 sliding, W5 join), inside the `ProcessWindowFunction`/`process` emit path, read `ctx.currentWatermark()` and stamp it as `wm_ms`; compute `t_e_ms` as the wall-clock time when the watermark first passed the firing boundary (`window.getEnd() + allowedLateness`). For stateless W1/W2, set `t_e_ms == t2_ms` (no semantic wait) and `wm_ms` = current watermark. Add both to the JSON output payload next to the existing `t2_ms`.
- [ ] **Step 2:** In `IdentityApp.java`, in the `suppress(untilWindowCloses(...))`/windowed emit path, capture the current stream time (`context.currentStreamTimeMs()` via a `Transformer`/`Processor` wrapping the suppression output, or the `Record` timestamp of the firing) as `wm_ms`, and set `t_e_ms` to the time stream time first passed `windowEnd + grace`. For W5 join, define eligibility as the time both join inputs are present and the join fires (document this in Step 4). For W1/W2 set `t_e_ms == t2_ms`.
- [ ] **Step 3:** Rebuild both engine images and run a **W3 smoke run** for each engine (`make kafka-streams-w1` variants / the W3 latency script at small scale). Confirm output payloads now contain non-null `t_e_ms` and `wm_ms`, and that for W3 `t_e_ms < t2_ms` (engine trigger strictly after eligibility) while for W1 `t_e_ms == t2_ms`.
- [ ] **Step 4:** Write a short `docs/latency_attribution.md` defining, per workload: which input timestamp anchors the output (rule: **maximum event time among contributing inputs** — matches the probe's existing `max(source_send_times)`), how `T_e` is defined for each engine, how W5's two inputs both contribute, and how late/duplicate/out-of-order inputs are handled. This document is the §A.2 deliverable.
- [ ] **Step 5:** Commit: `git add -A && git commit -m "feat(engines): emit watermark/stream-time T_e and eligibility into output payload"`.

**Definition of done:** Both engine jobs emit `t_e_ms` and `wm_ms`; a smoke run shows `T_e`-before-trigger on windowed workloads; `docs/latency_attribution.md` states the attribution rule and both-engine equivalence. (Closes §A.1 first six boxes and §A.2.)

### Task 1.2: Record and decompose `T_e` in the probe

**Files:**
- Modify: `src/stream_state_bench/kafka_latency_probe.py` (sample dict ~lines 329–341; CSV header ~line 118)
- Modify: `src/stream_state_bench/summarize_latency_results.py`
- Test: `tests/test_kafka_latency_probe.py`

**Interfaces:**
- Consumes: `t_e_ms`, `wm_ms` from Task 1.1 payloads.
- Produces: three separately reported distributions — `semantic_wait_ms = T_e - T1`, `engine_compute_ms = T2 - T_e`, `visibility_ms = T3 - T2` — plus raw `t_e_ms`/`wm_ms` columns in `latency_samples.csv`.

- [ ] **Step 1 (failing test):** In `tests/test_kafka_latency_probe.py` add `test_te_decomposition_splits_semantic_from_compute()` feeding a synthetic output with `t1=1000, t_e=5000, t2=5010, t3=5800` and asserting `semantic_wait_ms==4000`, `engine_compute_ms==10`, `visibility_ms==790`. Run: `pytest tests/test_kafka_latency_probe.py::test_te_decomposition_splits_semantic_from_compute -v` → expect FAIL.
- [ ] **Step 2:** In `kafka_latency_probe.py`, parse `t_e_ms`/`wm_ms` from the payload alongside `t2_ms` (near line 320). Add `semantic_wait_ms`, `engine_compute_ms`, `visibility_ms` to the `sample` dict; keep the legacy `input_append_to_result_emission_latency_ms` (`T2-T1`) for backward-compatible tables but stop labeling it "processing". Add `t_e_ms`, `wm_ms` to the CSV header list (line ~118).
- [ ] **Step 3:** Run the test → expect PASS.
- [ ] **Step 4:** In `summarize_latency_results.py`, add p50/p95/p99 columns for `semantic_wait`, `engine_compute`, `visibility` (mirror the existing `l_visibility`/`l_closure` handling around lines 164/201/216) with the existing 95% CI and effect-size machinery.
- [ ] **Step 5:** Re-run one existing W3 latency directory through the summarizer to confirm the new columns populate (older dirs will show null `T_e` — that is expected until re-run in Phase 2).
- [ ] **Step 6:** Commit: `git add -A && git commit -m "feat(probe): decompose latency into semantic-wait, engine-compute (T2-Te), visibility"`.

**Definition of done:** `latency_samples.csv` carries raw `t_e_ms`/`wm_ms`; summaries report the three distributions separately; the test proves the split. (Closes §A.1 "report all three separately", "remove processing labels", "record raw watermark values".)

---

## Phase 2 — Five-trial sustained and tuning experiments (W1–W4)

Addresses `review_plan.md` §A.3 (replication), §A.4 (tail decomposition), and §C.12 (tuning). Requires Phase 1 so every run records `T_e`.

### Task 2.1: Run five blocked 30-minute sustained trials per engine × W1–W4

**Files:**
- Use: `scripts/run-stability-tests.sh` (30-min, 180k-event runner)
- Create: `scripts/run-sustained-matrix.sh` (wrapper that blocks/randomizes order and writes `_trialN`)
- Output: `experiments/results/{engine}_w{1..4}_latency_stability_100_trial{1..5}/`

- [ ] **Step 1:** Write `scripts/run-sustained-matrix.sh` that loops `engine ∈ {kafka-streams, flink}`, `workload ∈ {w1,w2,w3,w4}`, `trial ∈ {1..5}` in a **randomized order** (shuffle the 40-cell list, seed recorded to a `run_order.txt`), each cell calling `RUN_LABEL=stability_100_trial$trial RATE_PER_SEC=100 DURATION_SEC=1800 scripts/run-stability-tests.sh $engine $workload`. Confirm the script clears Flink checkpoints and scales the consumer idle timeout (both fixes are already in `run-stability-tests.sh` / `run-flink-w1-latency.sh`; verify by reading them first).
- [ ] **Step 2:** Dry-run one cell (`flink w1 trial1`) and confirm: verifier PASS, `latency_samples.csv` non-empty, `t_e_ms` populated, `run_metadata.json` records rate/duration/seed.
- [ ] **Step 3:** Launch the full matrix in the background (40 runs × ~30 min ≈ long; run overnight). Record start in `docs/project_log.md`.
- [ ] **Step 4:** After completion, verify **every** cell PASSED correctness (§A.3 "verify output correctness after every run") and that warm-up exclusion (first 5% by `t0_ms`, already in `summarize_latency_results.py:75`) is applied.
- [ ] **Step 5:** Append 40 rows to `docs/results_manifest.csv`.
- [ ] **Step 6:** Commit results + manifest rows.

**Definition of done:** Five verified 30-minute trials per engine × W1–W4 exist with randomized order recorded and `T_e` captured. (Closes §A.3 boxes 1–5, 12–13.)

### Task 2.2: Per-run statistics, ECDFs, lag trend, and stability criterion

**Files:**
- Create: `src/stream_state_bench/aggregate_runs.py`
- Create: `src/stream_state_bench/make_figures.py`
- Output: `experiments/results/figures/{ecdf_*,lag_*}.pdf`, `experiments/results/sustained_summary.csv`

- [ ] **Step 1:** In `aggregate_runs.py`, for each engine×workload compute per-run p50/p95/p99 of total and of each `T_e` component, then median-of-runs with a **percentile-bootstrap 95% CI across the five runs** and report run-to-run dispersion (IQR). State the quantile estimator explicitly (linear interpolation, `numpy.percentile` default) and the N behind each percentile (§A.4 boxes 7–9).
- [ ] **Step 2:** In `make_figures.py`, plot per-workload latency **ECDFs** (all five runs overlaid) and **lag-over-time** series from each run's `lag.csv`. Fit a lag slope with uncertainty (OLS on lag vs. wall-clock, report slope ± CI) and define a quantitative stability criterion: *slope CI upper bound < X events/min ⇒ stable*, replacing the prose "backlog did not grow monotonically" (§A.3 boxes 7–11).
- [ ] **Step 3:** Run both scripts; confirm `sustained_summary.csv` and figure PDFs are produced and legible.
- [ ] **Step 4:** Commit.

**Definition of done:** Median-of-runs with CIs, ECDFs, lag time-series, and a numeric stability criterion exist for W1–W4. (Closes §A.3 boxes 6–11.)

### Task 2.3: Correct the tail (p99) decomposition to same-record analysis

**Files:**
- Create: `src/stream_state_bench/tail_decomposition.py`
- Output: `experiments/results/tail_decomposition.csv`

- [ ] **Step 1 (failing test):** Add `tests/test_tail_decomposition.py::test_ranks_by_total_then_reports_components` — feed samples where the max-total record is not the max-component record, assert the tool selects the top-1% by `latency_ms (T3-T0)` and reports *their* component values (not marginal per-component p99s). Run → FAIL.
- [ ] **Step 2:** Implement `tail_decomposition.py`: rank outputs by total `T3-T0`, select the same top-1%, report each component (`semantic_wait`, `engine_compute`, `visibility`) for those records, and the conditional component distributions for high-total records. Never sum marginal p99s.
- [ ] **Step 3:** Run test → PASS.
- [ ] **Step 4:** Add block-bootstrap (per-run blocks) for engine-difference CIs to handle time-series dependence (§A.4 box "per-run inference or block bootstrap"). Emit `tail_decomposition.csv`.
- [ ] **Step 5:** Investigate the `T1-T0 ≈ 1s` question (§A.4 last two boxes): inspect producer config in the compose files / probe (`linger`, batching, acks, flush) and write a one-paragraph finding into `docs/latency_attribution.md` stating whether producer pacing/queueing explains it.
- [ ] **Step 6:** Commit.

**Definition of done:** Tail components are reported for the *same* top-1% records with engine-difference CIs; the ~1s ingestion hop is explained. (Closes §A.4.)

### Task 2.4: Strengthen the tuning study to ≥3 intervals × 5 trials, sustained

**Files:**
- Use: `scripts/run-w3-tuning.sh`, `scripts/run-expert-tuning.sh`, `scripts/run-ablation-kafka-cache.sh`, `scripts/run-ablation-kafka-commit.sh`
- Output: `experiments/results/{engine}_w3_latency_tuning-cp-{1000,5000,10000}_trial{1..5}/` and cache-off/cache-size ablation dirs

- [ ] **Step 1:** Extend the tuning matrix to **three intervals** (add 5000 ms to the existing 1000/10000) and **five trials** per interval per engine, run at sustained scale (reuse the 6000-input/100-per-sec protocol already in `run-w3-tuning.sh`). Randomize order.
- [ ] **Step 2:** Run the Kafka Streams interval sweep a second time with **caching disabled** (`run-ablation-kafka-cache.sh`) and independently vary cache size, so the mechanism claim is separable (§C.12 boxes 6–7).
- [ ] **Step 3:** Re-summarize with per-run CIs and the `T_e` split; show whether the interval effect persists in `engine_compute (T2-T_e)` after removing `semantic_wait` (§C.12 last box).
- [ ] **Step 4:** Append manifest rows; commit.

**Definition of done:** ≥3 intervals × 5 trials sustained, with cache ablation, reported with CIs and split by `T_e`; the paper can say which knob moves which *engine-compute* stage. (Closes §C.12; strengthens §A.1 "does the KS–Flink difference remain after semantic waiting is removed".)

---

## Phase 3 — Direct fault-recovery milestones (RQ3)

Addresses `review_plan.md` §A.5. Choose the **preferred path** (direct measurement) — the harness already has five fault trials per mode and a recovery-timeline extractor, so direct milestones are within reach.

### Task 3.1: Extract recovery milestones from existing + new fault trials

**Files:**
- Modify: `scripts/extract-recovery-timeline.py`
- Use: `scripts/run-all-failure-tests.sh`, `scripts/run-failure-test.sh`
- Output: `experiments/results/recovery_milestones.csv`

**Interfaces:**
- Produces per (engine, workload, failure_mode, trial): `t_inject`, `t_restart`, `t_rebalance_done`, `t_state_restore_start`, `t_state_restore_done`, `t_first_post_recovery_record`, `t_first_correct_output`, `t_backlog_zero`, `final_correct` (bool), `censored` (bool), `timeout_sec`.

- [ ] **Step 1:** Extend `extract-recovery-timeline.py` to parse each milestone from `docker-compose.log` (failure injection marker, container restart, consumer-group rebalance / task assignment lines, Flink checkpoint restore start/complete, Kafka Streams changelog restore, first processed record, first committed output) and from `lag.csv` (backlog-reaches-zero). Emit one row per trial to `recovery_milestones.csv`.
- [ ] **Step 2:** Run it over the existing `*_failure_*_trial{1..5}` directories for W1. Verify milestones populate for completed Flink trials and that Kafka Streams censored trials are flagged `censored=true` with the exact `timeout_sec` (read the timeout from the run script, not guessed).
- [ ] **Step 3:** Run the **stateful** fault matrix (W3, W4) for both engines × {jvm_kill, broker_kill, node_loss}, five trials each, if not already present (`run-all-failure-tests.sh`). This tests the rebalance-vs-state-restore prediction that §A.5 and the paper's own "Highest-Priority Next Gaps" call the #1 gap. Record state size at failure, changelog end offsets (KS), checkpoint size/age/location (Flink), and restore bytes/records/throughput.
- [ ] **Step 4:** Diagnose each censored Kafka Streams trial from broker+engine logs and classify it (slow restore / repeated rebalance / transaction retry / identity error / genuine stall) — add a `censor_cause` column. **Do not** call any of these permanent data loss (§A.5 last box).
- [ ] **Step 5:** Append manifest rows; commit.

**Definition of done:** `recovery_milestones.csv` reports the §A.5 milestones and state/restore sizes for five trials per condition, with censored trials flagged, timed out at the stated timeout, and cause-classified. RQ3 is rewritten around milestones, not output-record p99. (Closes §A.5 preferred path.)

### Task 3.2: Recovery-timeline figure and RQ3 rewrite

**Files:**
- Modify: `src/stream_state_bench/make_figures.py`
- Modify: `paper/final_paper.md` (RQ3 section, §5.6)

- [ ] **Step 1:** Add a recovery-timeline figure (milestone Gantt per engine/mode) to `make_figures.py`.
- [ ] **Step 2:** Rewrite the RQ3 prose so it asks exactly what the milestones answer; report recovery time as `t_backlog_zero - t_inject` with CIs across trials; keep censored trials as right-censored observations with the stated timeout. Remove "recovery cost"/"data loss" wording unless a trial is still missing output after quiescence.
- [ ] **Step 3:** Commit.

**Definition of done:** RQ3 matches measured recovery information; figure present. (Closes §A.5 "definition of done" and §D.13 recovery-timeline figure.)

---

## Phase 4 — Resource accounting per workload (RQ2)

Addresses `review_plan.md` §A.6. The sampler `src/stream_state_bench/resource_monitor.py` exists; the gap is coverage (per-workload, broker, changelog/repartition bytes, checkpoint I/O, RocksDB state size, idle baseline) and reporting.

### Task 4.1: Sample full resource set per engine × W1–W4 (+ idle baseline)

**Files:**
- Modify: `src/stream_state_bench/resource_monitor.py`
- Create: `scripts/run-resource-matrix.sh`
- Output: `experiments/results/{engine}_w{1..4}_resource_trial{1..5}/`, `experiments/results/resource_summary.csv`

- [ ] **Step 1:** Confirm `resource_monitor.py` samples worker CPU (CPU-seconds), memory (mean/p95/peak), network bytes, and block-I/O bytes for **every** container (worker, JobManager, broker) — extend it if any are missing. Add collection of Kafka changelog-topic bytes and repartition-topic bytes (via `kafka-log-dirs`/topic size), Flink checkpoint size+duration and checkpoint-storage read/write bytes, and RocksDB state-dir size for both engines.
- [ ] **Step 2:** `run-resource-matrix.sh`: for each engine × {w1..w4} × 5 trials, run the sustained workload with the sampler attached; also run one **idle/no-workload baseline** per engine (§A.6 box "idle baselines").
- [ ] **Step 3:** Aggregate into `resource_summary.csv` **per workload** (not one blended mean — the paper's current blended Flink mean is a §A.6 defect), normalized per input, per output, and per wall-clock second, with both total-system and per-container costs, plus across-run variability.
- [ ] **Step 4:** Append manifest rows; commit.

**Definition of done:** Per-workload worker+broker+JobManager CPU/mem/net/block-I/O, changelog/repartition/checkpoint bytes, RocksDB state size, and idle baselines are reported with variability. If any measurement proves infeasible, instead narrow RQ2/title to "container-level observations" (§A.6 alternative) and record that decision. (Closes §A.6.)

### Task 4.2: Resource-utilization figures

**Files:** Modify `src/stream_state_bench/make_figures.py`; output `experiments/results/figures/resource_*.pdf`.

- [ ] **Step 1:** Add per-workload resource bar/line figures (worker vs broker vs storage split — the title's "where does state cost go" story).
- [ ] **Step 2:** Commit.

**Definition of done:** Resource figures exist for the results section. (Closes §D.13 resource-utilization figure.)

---

## Phase 5 — Document comparison fairness

Addresses `review_plan.md` §A.7. This is mostly a documentation-collection task.

### Task 5.1: Emit and record the full fairness configuration

**Files:**
- Create: `scripts/collect-environment.sh`
- Create: `docs/fairness_config.md`
- Output: `experiments/results/environment.json`

- [ ] **Step 1:** `collect-environment.sh` records host CPU model, physical/logical cores, RAM, disk type, filesystem, OS/kernel, Docker and JVM versions, and each container's CPU/memory limits, into `environment.json`.
- [ ] **Step 2:** From the compose files and app code, transcribe into `docs/fairness_config.md`: all topic partition counts and replication factors; Kafka Streams thread/task counts and Flink source/operator/sink parallelism; Flink state backend + RocksDB config + checkpoint-storage config + incremental-checkpoint flag; Kafka Streams cache size, suppression-buffer, window grace/retention; watermark generation + idle-partition handling; producer `linger`/batch/acks/compression/retries; consumer fetch/poll; transaction settings; restart policies. Demonstrate equivalent event-time semantics and state matched resource budgets (or explain differences).
- [ ] **Step 3:** Remove from the paper the assertion that shared-host contention affects both engines equally; replace with the host-level utilization measured in Phase 4 so interference is assessed, not assumed (§A.7 last two boxes).
- [ ] **Step 4:** Append `environment.json` to manifest; commit.

**Definition of done:** Another expert can reconstruct the deployment and judge comparability from `docs/fairness_config.md` + `environment.json`. (Closes §A.7.)

---

## Phase 6 — Scope decisions: W5, fixed-load framing, title

Addresses `review_plan.md` §B.9, §B.10, §B.11. These are decisions plus edits; recommended defaults below (the effort to make W5 and a full saturation envelope comparable is large and the memory already flags W5 long-duration as infeasible at this scale).

### Task 6.1: Narrow W5 out of the headline claims (recommended reduced scope)

**Files:** Modify `paper/final_paper.md`, `proposal.md`, `docs/claim_evidence_map.md`.

- [ ] **Step 1:** State the main findings cover stateless transforms and final-only windowed aggregations; retain W5 only as a benchmark-design/correctness case study; remove W5 from headline comparative latency/recovery claims; add the documented W5 output-cardinality-blowup limitation (from MEMORY 2026-07-17). (§B.9 reduced-scope path.)
- [ ] **Step 2:** Commit.

**Definition of done:** W5 is explicitly outside the principal evidence. (Closes §B.9.)

### Task 6.2: Keep the study fixed-load and consistent

**Files:** Modify `paper/final_paper.md`.

- [ ] **Step 1:** Keep RQ1 explicitly limited to 100 events/s on the documented host; remove any saturation/sustainable-throughput/capacity-envelope implication; state neither engine was tested near its limit; do not read the 10/20/40-events/s finite runs as scaling; keep conclusions conditional on workload/rate/state size/hardware. (Use the partial saturation sweeps only as an appendix "not a saturation study" note, or complete the full sweep per §B.10 alternative if time allows — recommended: keep fixed-load.) (§B.10.)
- [ ] **Step 2:** Commit.

**Definition of done:** The paper is consistently fixed-load. (Closes §B.10.)

### Task 6.3: Align the title with completed evidence

**Files:** Modify `paper/final_paper.md`, `paper/final_paper.tex`, `README.md`, `proposal.md`.

- [ ] **Step 1:** Keep "Where Does Streaming State Cost Go?" **only if** Phases 1–4 all landed (T_e isolation + worker/broker resource accounting + changelog/checkpoint measurements + direct recovery + repeated sustained evidence). Otherwise adopt: *"A Reproducible Fixed-Load Comparison of Flink and Kafka Streams"*. Decide based on which phases actually completed and record the decision in `.agents/MEMORY.md`. (§B.11.)
- [ ] **Step 2:** Commit.

**Definition of done:** Title claims nothing broader than the measurements. (Closes §B.11.)

---

## Phase 7 — Immutable, reviewable artifact

Addresses `review_plan.md` §A.8. Depends on all data existing.

### Task 7.1: Pin, script, and manifest the artifact

**Files:**
- Modify: all `experiments/**/docker-compose.yml` (pin image digests)
- Modify: `scripts/reproduce-all.sh`
- Create: `docs/ARTIFACT.md`
- Finalize: `docs/results_manifest.csv`

- [ ] **Step 1:** Pin every container image by `@sha256:` digest in all compose files; record digests in `docs/ARTIFACT.md`.
- [ ] **Step 2:** Ensure `scripts/reproduce-all.sh` gives one-command environment setup and one command per central experiment, and includes the stale-state guards (topics, checkpoints, volumes, offsets, application identities) — the checkpoint-clear + offset-reset checks documented in the project log.
- [ ] **Step 3:** Complete `docs/results_manifest.csv` so **every** table/figure in the paper maps to a result dir, raw files (`T0,T1,T_e,T2,T3` samples, lag traces, resource samples, fault logs — including successful, failed, and censored trials), and the exact command that regenerates it.
- [ ] **Step 4:** State required runtime/storage/CPU/memory in `docs/ARTIFACT.md`; add the repo URL and pin the immutable commit hash; create an archival release/DOI (e.g. Zenodo) and record it.
- [ ] **Step 5:** Demonstrate a clean-machine reproduction of at least one headline result (fresh clone or VM) and note it in `docs/ARTIFACT.md`.
- [ ] **Step 6:** Commit and tag: `git tag -a v-edbt-artifact -m "immutable artifact for submission"`.

**Definition of done:** A reviewer can regenerate every headline result without undocumented intervention; the paper carries repo URL, commit hash, and DOI. (Closes §A.8.)

---

## Phase 8 — Rebuild the paper from the result manifest

Addresses `review_plan.md` §D.13 (presentation), §D.14 (related work/novelty), and re-synchronizes every claim.

### Task 8.1: Regenerate all tables and figures from the manifest

**Files:** Create `src/stream_state_bench/build_paper_tables.py`; modify `paper/final_paper.md`.

- [ ] **Step 1:** `build_paper_tables.py` reads `docs/results_manifest.csv` + summary CSVs and emits every results table (latency by `T_e` component, tuning, recovery milestones, resource) with **units in headings, sample sizes stated, and CI/median-of-runs labeling** (§D.13 boxes). No hand-typed numbers.
- [ ] **Step 2:** Insert the Phase 2/3/4 figures (ECDFs, lag-over-time, recovery timeline, resource utilization). Number every caption.
- [ ] **Step 3:** Commit.

**Definition of done:** Every table/figure is generated, numbered, unit-labeled, sample-sized, and CI-labeled. (Closes §D.13 figure/table boxes.)

### Task 8.2: Fix notation, typesetting, and causal-language consistency

**Files:** Modify `paper/final_paper.md`, `paper/final_paper.tex`.

- [ ] **Step 1:** Fix LaTeX artifacts (`extttT2-T1`, `extttT_e`); use one notation `T0,T1,T_e,T2,T3` throughout; replace every remaining `T2-T1` "processing" label with `engine_compute (T2-T_e)` where `T_e` now exists, and with an accurate combined-hop label elsewhere.
- [ ] **Step 2:** Remove causal words ("because", "controls", "gates", "recovery cost") wherever not directly measured; ensure the abstract reports only fully-supported results; keep terminology consistent across RQs/methods/results/discussion/conclusion; avoid generalizing beyond the one shared-host deployment.
- [ ] **Step 3:** Rebuild: `pandoc ... -o paper/final_paper.tex && pdflatex` ×2; visually verify every page renders (no overfull boxes, legible tables).
- [ ] **Step 4:** Commit.

**Definition of done:** Notation and typesetting are clean; no over-claim language survives; PDF renders. (Closes §D.13.)

### Task 8.3: Strengthen related work and pin the novelty claim

**Files:** Modify `paper/final_paper.md` (§2 Related Work).

- [ ] **Step 1:** Compare directly against prior latency-attribution studies and against exactly-once-overhead, checkpoint/transactional-sink-latency, and state-restoration/recovery-benchmarking literature (extend the existing Nexmark/Yahoo/ESPBench/Theodolite/DSPBench set). Every added citation verified (venue/year/DOI).
- [ ] **Step 2:** State precisely what `T0–T_e–T3` instrumentation adds beyond prior benchmarks; distinguish workload-suite novelty from measurement-methodology novelty; name the single principal contribution (benchmark artifact / latency methodology / resource-cost analysis / recovery analysis).
- [ ] **Step 3:** Commit.

**Definition of done:** Novelty is precise and positioned against the closest methodology. (Closes §D.14.)

### Task 8.4: Re-score against the rubric and sync all supporting docs

**Files:** Modify `docs/rubric_assessment.md`, `docs/claim_evidence_map.md`, `docs/reproducibility.md`, `README.md`, `proposal.md`, `.agents/MEMORY.md`.

- [ ] **Step 1:** Re-run the §Final go/no-go test (all 17 questions must be "yes") and re-score `docs/rubric_assessment.md`; the target is ≥82/100 with no critical category below 4/5.
- [ ] **Step 2:** Sync claim-evidence map, reproducibility guide, README, proposal to the final evidence; add an `[OUTCOMES]` entry to `.agents/MEMORY.md`.
- [ ] **Step 3:** Commit; open the PR / finish the branch via `superpowers:finishing-a-development-branch`.

**Definition of done:** Every go/no-go answer is yes; rubric ≥82/100; all docs synchronized. (Closes the plan.)

---

## Self-review — spec coverage map

| review_plan.md gate | Task(s) |
|---|---|
| §A.1 T_e semantic separation | 1.1, 1.2, 2.4 |
| §A.2 latency attribution | 1.1 (docs/latency_attribution.md) |
| §A.3 replicate sustained W1–W4 | 2.1, 2.2 |
| §A.4 tail decomposition | 2.3 |
| §A.5 RQ3 direct recovery | 3.1, 3.2 |
| §A.6 RQ2 resource metrics | 4.1, 4.2 |
| §A.7 fairness documentation | 5.1 |
| §A.8 immutable artifact | 0.1, 7.1 |
| §B.9 W5 scope | 6.1 |
| §B.10 fixed-load framing | 6.2 |
| §B.11 title alignment | 6.3 |
| §C.12 tuning strengthening | 2.4 |
| §D.13 presentation/typesetting | 4.2, 3.2, 8.1, 8.2 |
| §D.14 related work/novelty | 8.3 |
| Final go/no-go + rescore | 8.4 |

## Recommended execution order (matches "most direct path", review_plan.md §Most direct path)

1. Phase 1 (T_e + attribution) — unblocks all re-reporting.
2. Phase 2 (five-trial sustained + tuning) — the central evidence.
3. Phase 3 (recovery milestones + censoring diagnosis).
4. Phase 4 (resource accounting).
5. Phase 5 + Phase 7 (freeze configs + immutable artifact).
6. Phase 6 (narrow W5, RQ1, title to completed evidence).
7. Phase 8 (rebuild tables/figures/abstract/conclusion from the manifest, rescore).

Phases 1→4 are sequential (each re-reports against `T_e`); Phases 4, 5 can overlap; Phase 6 depends on knowing which of 1–4 completed; Phase 8 is last.
