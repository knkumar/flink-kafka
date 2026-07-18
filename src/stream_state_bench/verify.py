from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass

from .workloads import OutputRecord


@dataclass(frozen=True)
class VerificationReport:
    expected_count: int
    actual_count: int
    missing_count: int
    unexpected_count: int
    duplicate_count: int

    @property
    def passed(self) -> bool:
        return self.missing_count == 0 and self.unexpected_count == 0 and self.duplicate_count == 0

    def to_dict(self) -> dict[str, int | bool]:
        data = asdict(self)
        data["passed"] = self.passed
        return data


def _canonical(record: OutputRecord) -> tuple[object, ...]:
    return (
        record.output_id,
        record.key,
        record.window_start_ms,
        record.window_end_ms,
        record.value,
        record.source_event_ids,
    )


def verify_records(expected: list[OutputRecord], actual: list[OutputRecord]) -> VerificationReport:
    expected_counter = Counter(_canonical(record) for record in expected)
    actual_counter = Counter(_canonical(record) for record in actual)

    missing = expected_counter - actual_counter
    unexpected = actual_counter - expected_counter
    duplicate_count = sum(count - 1 for count in actual_counter.values() if count > 1)

    return VerificationReport(
        expected_count=len(expected),
        actual_count=len(actual),
        missing_count=sum(missing.values()),
        unexpected_count=sum(unexpected.values()),
        duplicate_count=duplicate_count,
    )
