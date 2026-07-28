import re

with open('paper/final_paper.md', 'r') as f:
    content = f.read()

# 1. Gate A & C: Change "engine-processing hop" to "semantic wait & computation"
content = content.replace('engine-processing hop', 'semantic wait and computation hop')
content = content.replace('engine processing hop', 'semantic wait and computation hop')
content = content.replace('processing hop (`t2-t1`)', 'semantic wait and computation hop (`t2-t1`)')
content = content.replace('processing hop barely moves', 'computation hop barely moves')

# Update table headers for latency
content = content.replace('| `t1-t0` | `t2-t1` | `t3-t2` |', '| `t1-t0` (Ingest) | `t2-t1` (Wait+Compute) | `t3-t2` (Visibility) |')

# 2. Gate C: Add explicit disclaimer in methodology about T2-T1 and Te
disclaimer = """
**Latency Attribution and Semantic Wait ($T_e$).** For windowed workloads, the time between a record's ingestion ($T_1$) and the result's emission ($T_2$) includes both the engine's processing time and the *semantic wait* ($T_e - T_1$) until the watermark advances past the window's close. Because our probe measures wall-clock $T_2 - T_1$ comprehensively, this span is labeled "semantic wait and computation" rather than purely "engine processing."
"""
if "### 3.2 Measuring visibility latency" in content:
    content = content.replace("### 3.2 Measuring visibility latency\n", "### 3.2 Measuring visibility latency\n" + disclaimer)

# 3. Gate D & I: Re-state the tuning study limitations or remove them.
# The reviewer said the tuning study had only 71 outputs. We should explicitly state this.
content = content.replace('Both engines matched 71/71 expected records', 'Both engines matched 71/71 expected records (a bounded tuning sample)')

# 4. Gate E: Saturation boundary (Narrow RQ1)
content = content.replace('asks where visibility latency, backlog, and recovery time actually land.', 'asks where visibility latency, backlog, and recovery time actually land under fixed, sustained load.')
if "RQ1, sustained baseline performance." in content:
    content = content.replace('RQ1, sustained baseline performance.', 'RQ1, fixed-load baseline performance.')
content = content.replace('a 10/20/40 records/sec sweep', 'a bounded 10/20/40 records/sec scaling sweep')

# 5. Gate G: Infrastructure cost
# Narrow RQ2 to remove "total infrastructure" and just state worker/broker.
content = content.replace('asks where the infrastructure cost of exactly-once streaming state is placed', 'asks how the baseline memory and CPU cost of exactly-once streaming state is distributed')

# 6. Gate I: Failure definitions (Timeout vs Data loss)
# Reviewer: "A timeout is not called data loss... distinguish timeout from data loss"
content = content.replace('apparent data loss from the perspective of downstream', 'a complete harness timeout (apparent data loss from the perspective of a rigidly timed downstream consumer, though the engine may still be recovering)')
content = content.replace('resulting in dropped data from the perspective of the harness.', 'resulting in a harness timeout (dropped data from the perspective of this benchmark\'s strict observation window, not necessarily permanent corruption).')

# 7. Gate J: KRaft failover
content = content.replace('KRaft failover', 'controller termination')
content = content.replace('KRaft Kafka', 'Kafka')

# 8. Gate K: W5 Comparable
content = content.replace('without the correctness probe completing', 'without the correctness probe completing. We treat this as an out-of-scope saturation failure for W5 under these parameters.')

with open('paper/final_paper.md', 'w') as f:
    f.write(content)
print("Paper rewritten to meet EDBT rubric Gates A, C, E, G, I, J, K.")
