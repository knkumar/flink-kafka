from __future__ import annotations

from dataclasses import dataclass
from random import Random
from typing import Iterable, Literal


Side = Literal["left", "right", "single"]


@dataclass(frozen=True, order=True)
class Event:
    event_id: str
    key: int
    payload: int
    event_time_ms: int
    side: Side = "single"


def uniform_events(count: int, *, key_count: int, seed: int, start_ms: int = 0, step_ms: int = 100) -> list[Event]:
    rng = Random(seed)
    return [
        Event(
            event_id=f"e-{i}",
            key=rng.randrange(key_count),
            payload=rng.randrange(1_000),
            event_time_ms=start_ms + i * step_ms,
        )
        for i in range(count)
    ]


def paired_join_events(
    count: int,
    *,
    key_count: int,
    seed: int,
    start_ms: int = 0,
    step_ms: int = 100,
    max_skew_ms: int = 1_000,
) -> tuple[list[Event], list[Event]]:
    rng = Random(seed)
    left: list[Event] = []
    right: list[Event] = []
    for i in range(count):
        key = rng.randrange(key_count)
        base_time = start_ms + i * step_ms
        skew = rng.randrange(-max_skew_ms, max_skew_ms + 1)
        payload = rng.randrange(1_000)
        left.append(Event(f"l-{i}", key, payload, base_time, "left"))
        right.append(Event(f"r-{i}", key, payload + 1, base_time + skew, "right"))
    return left, right


def event_count(events: Iterable[Event]) -> int:
    return sum(1 for _ in events)
