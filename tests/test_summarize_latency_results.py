import json
import tempfile
import unittest
from pathlib import Path

from stream_state_bench.summarize_latency_results import aggregate_rows, load_rows


class SummarizeLatencyResultsTests(unittest.TestCase):
    def test_loads_and_aggregates_latency_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "kafka_streams_w1_latency_repeat1"
            run_dir.mkdir()
            (run_dir / "run_metadata.json").write_text(
                json.dumps(
                    {
                        "workload": "identity",
                        "run_label": "repeat1",
                    }
                ),
                encoding="utf-8",
            )
            (run_dir / "verification.json").write_text(
                json.dumps(
                    {
                        "verification": {
                            "passed": True,
                        }
                    }
                ),
                encoding="utf-8",
            )
            (run_dir / "latency_summary.json").write_text(
                json.dumps(
                    {
                        "summary": {
                            "rate_per_sec": 20.0,
                            "produced_records": 100,
                            "expected_output_records": 100,
                            "matched_records": 100,
                            "consumed_records": 100,
                            "p50_ms": 10.0,
                            "p95_ms": 20.0,
                            "p99_ms": 30.0,
                            "max_ms": 40.0,
                        }
                    }
                ),
                encoding="utf-8",
            )

            rows = load_rows(Path(tmpdir))
            aggregates = aggregate_rows(rows)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["engine"], "kafka_streams")
        self.assertEqual(rows[0]["workload_id"], "w1")
        self.assertEqual(rows[0]["run_label"], "repeat1")
        self.assertEqual(aggregates[0]["runs"], 1)
        self.assertEqual(aggregates[0]["mean_p99_ms"], 30.0)


if __name__ == "__main__":
    unittest.main()
