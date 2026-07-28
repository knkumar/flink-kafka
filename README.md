# Streaming State Cost Benchmark Artifact

A reproducible benchmark comparing Apache Flink and Kafka Streams on Apache Kafka (KRaft mode). This benchmark evaluates both engines across five workloads:

1. **W1:** Stateless identity transform
2. **W2:** Stateless filter/map
3. **W3:** Tumbling-count window
4. **W4:** Sliding-sum window
5. **W5:** Stream-stream join

Both engines run against the same generated input, the same multiset verifier, and the same four-point (T0-T3) latency decomposition.

**Start with `paper/final_paper.md`** for the current findings, what is and is not measured, and where each number comes from.

---

## 🚀 Onboarding Guide

Welcome to the project! If you're new here, this section will help you get set up and understand the structure of the repository.

### Prerequisites

To run the benchmark, you'll need:
- Docker and Docker Compose (for running isolated workloads)
- `make` (for executing benchmark targets)
- Python 3 (if running locally without Docker)

### Repository Structure

- **`src/stream_state_bench/`**: Core workload definitions, verification logic, experiment runner, and analyzer.
- **`experiments/`**: Docker Compose workflows and configuration for Kafka Streams and Flink. This is also where benchmark results are generated (`experiments/results/`).
- **`tests/`**: Unit tests for workload and verifier behavior.
- **`paper/`**: The final paper draft (tracked).
- **`docs/`**: Local, untracked working notes (logs, rubric assessments, evidence maps) generated while producing the paper. Not part of a fresh clone.

---

## 📖 User Guide

### Running the Checks

To verify your environment is set up correctly, run the local checks.

**Run locally:**
```bash
make check
# Or run the script directly: ./scripts/run-local-check.sh
```

**Run in a container:**
```bash
docker build -t stream-state-bench .
docker run --rm stream-state-bench
```

### Running Specific Workloads

You can run individual workloads for either Flink or Kafka Streams using `make`. For example, to run the W1 Identity workload:

- **Kafka Streams:** `make kafka-streams-w1`
- **Flink:** `make flink-w1`

*Substitute `w1` with `w2`, `w3`, `w4`, or `w5` to run other workloads.*

**Outputs:**
When you run a workload, the results are generated in `experiments/results/<engine>_<workload>/`. You will typically find files like:
- `input.tsv` (and `producer_input.tsv` for some workloads)
- `actual.jsonl`
- `verification.json`
- `run_metadata.json`
- `docker-compose.log`

### Running Latency Probes & Sweeps

To evaluate latency, you can run fixed-rate probes or full latency sweeps.

**Single Latency Probe:**
```bash
make kafka-streams-w1-latency
make flink-w1-latency
```

**Full Latency Sweep (runs rates 10, 40 and repeat):**
```bash
make w1-latency-sweep
```
*(Available for all workloads: `w1-latency-sweep` through `w5-latency-sweep`)*

**Outputs for Latency Runs:**
Latency runs produce additional summaries in their respective directories (`latency_summary.json` and `latency_samples.csv`). Sweep summaries are aggregated in `experiments/results/latency_summary.csv` and `.md`.

### Running the Full Matrix

To run the repeated correctness matrix and generate an engine summary:
```bash
make repeat-correctness
make engine-summary
```
This will output `experiments/results/engine_correctness_summary.csv` and `.md`.

---

## 📚 Documentation & Status

- **`paper/final_paper.md`**: Evidence-limited paper draft with full results and their scope. Tracked in git.
