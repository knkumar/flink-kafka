import json
import tempfile
import unittest
from pathlib import Path

from stream_state_bench.kafka_latency_probe import (
    input_event_id,
    load_input_records,
    load_expected_output_ids,
    load_expected_outputs,
    output_event_id,
    output_record_id_and_sources,
    percentile_nearest_rank,
    summarize_samples,
    write_latency_outputs,
)


class KafkaLatencyProbeTests(unittest.TestCase):
    def test_parses_input_and_output_event_ids(self):
        self.assertEqual(input_event_id("e-1\t3\t42\t100\n"), "e-1")
        self.assertEqual(output_event_id('{"output_id":"e-1","source_event_ids":["e-1"]}'), "e-1")
        self.assertEqual(output_event_id('{"output_id":"fm-e-1","source_event_ids":["e-1"]}'), "fm-e-1")
        self.assertEqual(output_event_id('{"source_event_ids":["e-2"]}'), "e-2")
        record_id, source_ids = output_record_id_and_sources(
            '{"output_id":"tc-1-0","source_event_ids":["e-1","e-2"]}'
        )
        self.assertEqual(record_id, "tc-1-0")
        self.assertEqual(source_ids, ("e-1", "e-2"))

    def test_loads_expected_output_ids_from_source_event_ids(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "expected.jsonl"
            path.write_text(
                '{"output_id":"fm-e-1","source_event_ids":["e-1"]}\n'
                '{"output_id":"fm-e-3","source_event_ids":["e-3"]}\n',
                encoding="utf-8",
            )

            ids = load_expected_output_ids(path)

        self.assertEqual(ids, {"fm-e-1", "fm-e-3"})

    def test_loads_expected_outputs_with_multiple_source_ids(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "expected.jsonl"
            path.write_text(
                '{"output_id":"tc-1-0","source_event_ids":["e-1","e-3"]}\n',
                encoding="utf-8",
            )

            outputs = load_expected_outputs(path)

        self.assertEqual(outputs, {"tc-1-0": ("e-1", "e-3")})

    def test_loads_input_records_with_topics(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            left = Path(tmpdir) / "left.tsv"
            right = Path(tmpdir) / "right.tsv"
            left.write_text("l-1\t1\t10\t100\n", encoding="utf-8")
            right.write_text("r-1\t1\t11\t120\n", encoding="utf-8")

            records = load_input_records([(left, "left-topic"), (right, "right-topic")])

        self.assertEqual(
            records,
            [
                ("left-topic", "l-1\t1\t10\t100"),
                ("right-topic", "r-1\t1\t11\t120"),
            ],
        )

    def test_percentile_uses_nearest_rank(self):
        values = [1.0, 2.0, 3.0, 4.0, 5.0]

        self.assertEqual(percentile_nearest_rank(values, 50), 3.0)
        self.assertEqual(percentile_nearest_rank(values, 95), 5.0)
        self.assertEqual(percentile_nearest_rank([], 99), None)

    def test_summarizes_and_writes_latency_outputs(self):
        samples = [
            {
                "event_id": "e-0",
                "t0_ms": 100.0,
                "t1_ms": 150.0,
                "t2_ms": 180.0,
                "t3_ms": 200.0,
                "t1_t0_ms": 50.0,
                "t2_t1_ms": 30.0,
                "t3_t2_ms": 20.0,
                "latency_ms": 1.0,
            },
            {
                "event_id": "e-1",
                "t0_ms": 200.0,
                "t1_ms": 350.0,
                "t2_ms": 420.0,
                "t3_ms": 500.0,
                "t1_t0_ms": 150.0,
                "t2_t1_ms": 70.0,
                "t3_t2_ms": 80.0,
                "latency_ms": 3.0,
            },
        ]

        summary = summarize_samples(samples, rate_per_sec=20.0)

        self.assertEqual(summary["matched_records"], 2)
        self.assertEqual(summary["p99_ms"], 3.0)
        with tempfile.TemporaryDirectory() as tmpdir:
            latency_json = Path(tmpdir) / "latency.json"
            latency_csv = Path(tmpdir) / "latency.csv"

            write_latency_outputs(
                samples=samples,
                summary=summary,
                latency_json=latency_json,
                latency_csv=latency_csv,
            )

            payload = json.loads(latency_json.read_text(encoding="utf-8"))
            csv_lines = latency_csv.read_text(encoding="utf-8").splitlines()

        self.assertEqual(payload["summary"]["measurement"], "host_write_to_read_committed_visibility_delay_proxy")
        self.assertEqual(
            csv_lines[0],
            "event_id,t0_ms,t1_ms,t2_ms,t3_ms,t1_t0_ms,t2_t1_ms,t3_t2_ms,latency_ms",
        )
        self.assertEqual(len(csv_lines), 3)


if __name__ == "__main__":
    unittest.main()
