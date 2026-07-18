import unittest

from stream_state_bench.events import Event
from stream_state_bench.verify import verify_records
from stream_state_bench.workloads import filter_map, identity, sliding_sum, stream_stream_join, tumbling_count


class WorkloadTests(unittest.TestCase):
    def test_identity_outputs_one_record_per_input(self):
        events = [
            Event("a", 1, 10, 0),
            Event("b", 2, 20, 100),
        ]

        records = identity(events)

        self.assertEqual(len(records), 2)
        self.assertEqual([record.output_id for record in records], ["a", "b"])

    def test_filter_map_keeps_even_payloads_and_doubles_value(self):
        events = [
            Event("a", 1, 3, 0),
            Event("b", 1, 4, 100),
        ]

        records = filter_map(events)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].output_id, "fm-b")
        self.assertEqual(records[0].value, 8)

    def test_tumbling_count_groups_by_key_and_window(self):
        events = [
            Event("a", 1, 10, 0),
            Event("b", 1, 10, 59_999),
            Event("c", 1, 10, 60_000),
            Event("d", 2, 10, 0),
        ]

        records = tumbling_count(events, window_ms=60_000)

        values = {(record.key, record.window_start_ms): record.value for record in records}
        self.assertEqual(values[(1, 0)], 2)
        self.assertEqual(values[(1, 60_000)], 1)
        self.assertEqual(values[(2, 0)], 1)

    def test_sliding_sum_assigns_event_to_overlapping_windows(self):
        events = [Event("a", 1, 5, 120_000)]

        records = sliding_sum(events, window_ms=180_000, slide_ms=60_000)

        starts = [record.window_start_ms for record in records]
        self.assertEqual(starts, [0, 60_000, 120_000])
        self.assertTrue(all(record.value == 5 for record in records))

    def test_stream_stream_join_matches_key_within_window(self):
        left = [
            Event("l1", 1, 10, 1_000, "left"),
            Event("l2", 2, 20, 1_000, "left"),
        ]
        right = [
            Event("r1", 1, 2, 1_500, "right"),
            Event("r2", 1, 2, 10_000, "right"),
        ]

        records = stream_stream_join(left, right, join_window_ms=1_000)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].source_event_ids, ("l1", "r1"))
        self.assertEqual(records[0].value, 12)

    def test_verifier_detects_missing_and_duplicate_records(self):
        expected = identity([Event("a", 1, 10, 0), Event("b", 1, 20, 100)])
        actual = [expected[0], expected[0]]

        report = verify_records(expected, actual)

        self.assertFalse(report.passed)
        self.assertEqual(report.missing_count, 1)
        self.assertEqual(report.unexpected_count, 1)
        self.assertEqual(report.duplicate_count, 1)


if __name__ == "__main__":
    unittest.main()
