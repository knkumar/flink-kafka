from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable

from .events import Event


@dataclass(frozen=True, order=True)
class OutputRecord:
    output_id: str
    key: int
    window_start_ms: int | None
    window_end_ms: int | None
    value: int
    source_event_ids: tuple[str, ...]


def identity(events: Iterable[Event]) -> list[OutputRecord]:
    return [
        OutputRecord(event.event_id, event.key, None, None, event.payload, (event.event_id,))
        for event in events
    ]


def filter_map(events: Iterable[Event]) -> list[OutputRecord]:
    records: list[OutputRecord] = []
    for event in events:
        if event.payload % 2 == 0:
            records.append(
                OutputRecord(
                    output_id=f"fm-{event.event_id}",
                    key=event.key,
                    window_start_ms=None,
                    window_end_ms=None,
                    value=event.payload * 2,
                    source_event_ids=(event.event_id,),
                )
            )
    return records


def tumbling_count(events: Iterable[Event], *, window_ms: int = 60_000) -> list[OutputRecord]:
    counts: dict[tuple[int, int], int] = defaultdict(int)
    event_ids: dict[tuple[int, int], list[str]] = defaultdict(list)
    for event in events:
        window_start = (event.event_time_ms // window_ms) * window_ms
        bucket = (event.key, window_start)
        counts[bucket] += 1
        event_ids[bucket].append(event.event_id)

    return [
        OutputRecord(
            output_id=f"tc-{key}-{window_start}",
            key=key,
            window_start_ms=window_start,
            window_end_ms=window_start + window_ms,
            value=count,
            source_event_ids=tuple(sorted(event_ids[(key, window_start)])),
        )
        for (key, window_start), count in sorted(counts.items())
    ]


def sliding_sum(
    events: Iterable[Event],
    *,
    window_ms: int = 600_000,
    slide_ms: int = 60_000,
) -> list[OutputRecord]:
    sums: dict[tuple[int, int], int] = defaultdict(int)
    event_ids: dict[tuple[int, int], list[str]] = defaultdict(list)
    for event in events:
        latest_start = (event.event_time_ms // slide_ms) * slide_ms
        earliest_start = latest_start - window_ms + slide_ms
        for window_start in range(earliest_start, latest_start + slide_ms, slide_ms):
            if window_start <= event.event_time_ms < window_start + window_ms:
                bucket = (event.key, window_start)
                sums[bucket] += event.payload
                event_ids[bucket].append(event.event_id)

    return [
        OutputRecord(
            output_id=f"ss-{key}-{window_start}",
            key=key,
            window_start_ms=window_start,
            window_end_ms=window_start + window_ms,
            value=value,
            source_event_ids=tuple(sorted(event_ids[(key, window_start)])),
        )
        for (key, window_start), value in sorted(sums.items())
    ]


def stream_stream_join(
    left_events: Iterable[Event],
    right_events: Iterable[Event],
    *,
    join_window_ms: int = 600_000,
) -> list[OutputRecord]:
    records: list[OutputRecord] = []
    right_by_key: dict[int, list[Event]] = defaultdict(list)
    for event in right_events:
        right_by_key[event.key].append(event)

    for left in left_events:
        for right in right_by_key.get(left.key, []):
            if abs(left.event_time_ms - right.event_time_ms) <= join_window_ms:
                first, second = sorted((left.event_id, right.event_id))
                records.append(
                    OutputRecord(
                        output_id=f"join-{first}-{second}",
                        key=left.key,
                        window_start_ms=min(left.event_time_ms, right.event_time_ms),
                        window_end_ms=max(left.event_time_ms, right.event_time_ms),
                        value=left.payload + right.payload,
                        source_event_ids=(left.event_id, right.event_id),
                    )
                )
    return sorted(records)
