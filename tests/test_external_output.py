import json
import tempfile
import unittest
from pathlib import Path

from stream_state_bench.reference import reference_outputs
from stream_state_bench.verify_external_output import load_jsonl_records, verify_external_output


class ExternalOutputVerifierTests(unittest.TestCase):
    def test_loads_jsonl_output_records(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "actual.jsonl"
            path.write_text(
                json.dumps(
                    {
                        "output_id": "a",
                        "key": 1,
                        "window_start_ms": None,
                        "window_end_ms": None,
                        "value": 10,
                        "source_event_ids": ["a"],
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            records = load_jsonl_records(path)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].source_event_ids, ("a",))

    def test_verifies_external_output_against_reference(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "identity.jsonl"
            records = reference_outputs("identity", events=3, keys=2, seed=7)
            path.write_text(
                "".join(
                    json.dumps(
                        {
                            "output_id": record.output_id,
                            "key": record.key,
                            "window_start_ms": record.window_start_ms,
                            "window_end_ms": record.window_end_ms,
                            "value": record.value,
                            "source_event_ids": list(record.source_event_ids),
                        }
                    )
                    + "\n"
                    for record in records
                ),
                encoding="utf-8",
            )

            result = verify_external_output(workload="identity", actual_jsonl=path, events=3, keys=2, seed=7)

        self.assertTrue(result["verification"]["passed"])
        self.assertEqual(result["verification"]["actual_count"], 3)


if __name__ == "__main__":
    unittest.main()
