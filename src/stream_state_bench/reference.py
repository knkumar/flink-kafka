from __future__ import annotations

from .events import paired_join_events, uniform_events
from .workloads import OutputRecord, filter_map, identity, sliding_sum, stream_stream_join, tumbling_count


WORKLOADS = ("identity", "filter_map", "tumbling_count", "sliding_sum", "stream_stream_join")


def reference_outputs(name: str, *, events: int, keys: int, seed: int, start_ms: int = 0) -> list[OutputRecord]:
    if name == "stream_stream_join":
        left, right = paired_join_events(events, key_count=keys, seed=seed, start_ms=start_ms)
        return stream_stream_join(left, right)

    generated = uniform_events(events, key_count=keys, seed=seed, start_ms=start_ms)
    if name == "identity":
        return identity(generated)
    if name == "filter_map":
        return filter_map(generated)
    if name == "tumbling_count":
        return tumbling_count(generated)
    if name == "sliding_sum":
        return sliding_sum(generated)
    raise ValueError(f"Unknown workload: {name}")
