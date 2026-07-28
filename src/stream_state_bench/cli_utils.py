"""Small helpers shared by the package's CLI entry points and result scripts."""

from __future__ import annotations

import re


def parse_scale(value: str) -> int:
    v = value.lower()
    if v.endswith("k"):
        return int(float(v[:-1]) * 1000)
    elif v.endswith("m"):
        return int(float(v[:-1]) * 1000000)
    elif v.endswith("b"):
        return int(float(v[:-1]) * 1000000000)
    return int(v)


def get_base_experiment_and_trial(dirname: str) -> tuple[str, int]:
    match = re.search(r'^(.*)_trial(\d+)$', dirname)
    if match:
        return match.group(1), int(match.group(2))
    return dirname, 1
