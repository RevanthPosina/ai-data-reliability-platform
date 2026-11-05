"""Consistent-cut helpers (design S10): has a Spark query's checkpoint reached the topic end offsets?

Spark's Kafka source stores, per batch, `offsets/N` (the next offset to read per partition, the same meaning as a
high watermark) and `commits/N` once the batch is done. The newest committed N names the offsets file to read."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping

Offsets = dict[str, dict[int, int]]


def parse_offsets_file(text: str) -> Offsets:
    """The Kafka source offsets from `offsets/N`: the last line, `{"topic": {"partition": next_offset}}`.
    (Line 1 is the version, line 2 the batch metadata.) Raises ValueError if the last line is not that shape."""
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        raise ValueError("empty offsets file")
    try:
        obj = json.loads(lines[-1])
    except json.JSONDecodeError as e:
        raise ValueError(f"offsets file last line is not JSON: {lines[-1][:40]!r}") from e
    if not isinstance(obj, dict):
        raise ValueError("offsets file last line is not an object")
    out: Offsets = {}
    for topic, parts in obj.items():
        if not isinstance(parts, dict) or not all(
            isinstance(v, int) and not isinstance(v, bool) for v in parts.values()
        ):
            raise ValueError("offsets file last line holds no per-partition offsets")
        out[topic] = {int(p): int(o) for p, o in parts.items()}
    return out


def latest_committed(names: Iterable[str]) -> int | None:
    """The highest batch number among `commits/` entries; `.N.crc`, `N.tmp` and other names are ignored."""
    nums = [int(n) for n in names if n.isascii() and n.isdigit()]
    return max(nums) if nums else None


def uncovered(
    committed: Mapping[str, Mapping[int, int]], end: Mapping[str, Mapping[int, int]]
) -> dict[str, dict[int, int]]:
    """Partitions whose committed offset is behind the end offset: {topic: {partition: lag}}. A partition missing
    from `committed` has lag `end`; an empty partition (end 0) is covered. `{}` means covered."""
    out: dict[str, dict[int, int]] = {}
    for topic, parts in end.items():
        have = committed.get(topic, {})
        for p, e in parts.items():
            lag = e - have.get(p, 0)
            if lag > 0:
                out.setdefault(topic, {})[p] = lag
    return out
