import tempfile
import unittest
from pathlib import Path

from stream_state_bench.generate_input import write_identity_inputs, write_inputs


class GenerateInputTests(unittest.TestCase):
    def test_writes_identity_input_and_expected_output(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_tsv = Path(tmpdir) / "input.tsv"
            expected_jsonl = Path(tmpdir) / "expected.jsonl"

            write_identity_inputs(events=2, keys=10, seed=7, input_tsv=input_tsv, expected_jsonl=expected_jsonl)

            input_lines = input_tsv.read_text(encoding="utf-8").splitlines()
            expected_lines = expected_jsonl.read_text(encoding="utf-8").splitlines()

        self.assertEqual(len(input_lines), 2)
        self.assertEqual(len(expected_lines), 2)
        self.assertEqual(input_lines[0].count("\t"), 3)
        self.assertIn('"output_id":"e-0"', expected_lines[0])

    def test_writes_filter_map_expected_output(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_tsv = Path(tmpdir) / "input.tsv"
            expected_jsonl = Path(tmpdir) / "expected.jsonl"

            write_inputs(workload="filter_map", events=10, keys=10, seed=7, input_tsv=input_tsv, expected_jsonl=expected_jsonl)

            input_lines = input_tsv.read_text(encoding="utf-8").splitlines()
            expected_lines = expected_jsonl.read_text(encoding="utf-8").splitlines()

        self.assertEqual(len(input_lines), 10)
        self.assertTrue(expected_lines)
        self.assertTrue(all('"output_id":"fm-' in line for line in expected_lines))

    def test_writes_tumbling_count_expected_output(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_tsv = Path(tmpdir) / "input.tsv"
            expected_jsonl = Path(tmpdir) / "expected.jsonl"

            write_inputs(workload="tumbling_count", events=1000, keys=100, seed=7, input_tsv=input_tsv, expected_jsonl=expected_jsonl)

            input_lines = input_tsv.read_text(encoding="utf-8").splitlines()
            expected_lines = expected_jsonl.read_text(encoding="utf-8").splitlines()

        self.assertEqual(len(input_lines), 1000)
        self.assertEqual(len(expected_lines), 199)
        self.assertTrue(all('"output_id":"tc-' in line for line in expected_lines))

    def test_writes_sliding_sum_with_nonzero_start(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_tsv = Path(tmpdir) / "input.tsv"
            expected_jsonl = Path(tmpdir) / "expected.jsonl"

            write_inputs(
                workload="sliding_sum",
                events=1000,
                keys=100,
                seed=7,
                start_ms=600_000,
                input_tsv=input_tsv,
                expected_jsonl=expected_jsonl,
            )

            input_lines = input_tsv.read_text(encoding="utf-8").splitlines()
            expected_lines = expected_jsonl.read_text(encoding="utf-8").splitlines()

        self.assertEqual(len(input_lines), 1000)
        self.assertEqual(len(expected_lines), 1099)
        self.assertIn("\t600000", input_lines[0])
        self.assertTrue(all('"output_id":"ss-' in line for line in expected_lines))

    def test_writes_stream_join_left_and_right_inputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_tsv = Path(tmpdir) / "input.tsv"
            left_input_tsv = Path(tmpdir) / "left.tsv"
            right_input_tsv = Path(tmpdir) / "right.tsv"
            expected_jsonl = Path(tmpdir) / "expected.jsonl"

            write_inputs(
                workload="stream_stream_join",
                events=10,
                keys=3,
                seed=7,
                input_tsv=input_tsv,
                left_input_tsv=left_input_tsv,
                right_input_tsv=right_input_tsv,
                expected_jsonl=expected_jsonl,
            )

            input_lines = input_tsv.read_text(encoding="utf-8").splitlines()
            left_lines = left_input_tsv.read_text(encoding="utf-8").splitlines()
            right_lines = right_input_tsv.read_text(encoding="utf-8").splitlines()
            expected_lines = expected_jsonl.read_text(encoding="utf-8").splitlines()

        self.assertEqual(len(input_lines), 20)
        self.assertEqual(len(left_lines), 10)
        self.assertEqual(len(right_lines), 10)
        self.assertTrue(left_lines[0].startswith("l-0\t"))
        self.assertTrue(right_lines[0].startswith("r-0\t"))
        self.assertTrue(all('"output_id":"join-' in line for line in expected_lines))


if __name__ == "__main__":
    unittest.main()
