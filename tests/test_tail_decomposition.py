import pandas as pd
import numpy as np
from pathlib import Path
from stream_state_bench.tail_decomposition import aggregate_rows_with_ci, load_rows

def test_ranks_by_total_then_reports_components(tmp_path):
    run_dir = tmp_path / "flink_test_workload_1000"
    run_dir.mkdir(parents=True)
    
    # We want 1000 samples. Top 1% is 10 samples.
    # We will make 990 samples have latency 10ms (1 + 4 + 5)
    # We will make 5 samples have latency 100ms (10 + 40 + 50)
    # We will make 5 samples have latency 500ms (100 + 200 + 200) -> These are top 1% by latency
    # But we will inject 1 sample with extreme semantic wait, but low total latency: 1000ms semantic wait but negative engine compute? 
    # Or rather, let's just make sure we do not take marginal p99s.
    
    # p99 of latency_ms = 500ms (the 5 samples)
    # If we take marginal p99 of semantic wait across all samples:
    # 990 samples have 1
    # 5 samples have 10
    # 5 samples have 100
    # The marginal p99 of semantic wait is 100.
    
    # Let's make an anomaly: 10 samples with latency_ms=490ms, semantic_wait=400, engine_compute=40, visibility=50
    # 10 samples with latency_ms=500ms, semantic_wait=10, engine_compute=200, visibility=290
    # 980 samples with latency=10ms
    # Total samples = 1000.
    # Top 1% = 10 samples (those with 500ms latency)
    # Their semantic_wait is 10. Their engine_compute is 200. Their visibility is 290.
    # The marginal top 1% (p99) of semantic_wait is 400.
    # The marginal top 1% (p99) of engine_compute is 200.
    # The marginal top 1% (p99) of visibility is 290.
    
    samples = []
    # 980 normal samples
    for i in range(980):
        samples.append({
            "t0_ms": i * 10,
            "latency_ms": 10,
            "semantic_wait": 3,
            "engine_compute": 3,
            "visibility": 4
        })
    # 10 samples with high semantic wait but lower total latency (490)
    for i in range(10):
        samples.append({
            "t0_ms": 9800 + i * 10,
            "latency_ms": 490,
            "semantic_wait": 400,
            "engine_compute": 40,
            "visibility": 50
        })
    # 10 samples with highest total latency (500)
    for i in range(10):
        samples.append({
            "t0_ms": 9900 + i * 10,
            "latency_ms": 500,
            "semantic_wait": 10,
            "engine_compute": 200,
            "visibility": 290
        })
        
    df = pd.DataFrame(samples)
    samples_csv = run_dir / "latency_samples.csv"
    df.to_csv(samples_csv, index=False)
    
    # Add metadata to be able to load
    import json
    (run_dir / "run_metadata.json").write_text(json.dumps({
        "workload": "test",
        "expected_output_records": 1000
    }))
    (run_dir / "verification.json").write_text(json.dumps({
        "verification": {"passed": True}
    }))
    (run_dir / "latency_summary.json").write_text(json.dumps({
        "summary": {
            "rate_per_sec": 1000,
            "matched_records": 1000,
            "consumed_records": 1000,
            "p50_ms": 10,
            "p95_ms": 10,
            "p99_ms": 500,
            "max_ms": 500
        }
    }))
    
    rows = load_rows(tmp_path)
    aggregates = aggregate_rows_with_ci(rows)
    
    assert len(aggregates) == 1
    agg = aggregates[0]
    
    # Check that we reported the mean component values for the top 1% latency records.
    # The top 1% records are the ones with latency_ms=500.
    # For these records, semantic_wait=10, engine_compute=200, visibility=290
    assert agg["tail_semantic_wait_ms"] == 10.0
    assert agg["tail_engine_compute_ms"] == 200.0
    assert agg["tail_visibility_ms"] == 290.0
    
    # Marginal p99 of semantic_wait is 400, but we shouldn't report that
    assert agg["tail_semantic_wait_ms"] != 400.0
