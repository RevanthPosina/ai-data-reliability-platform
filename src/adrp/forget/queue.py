"""File queue between Dagster and the host executor (G8). Pure stdlib, no raw ids.

The Dagster container has no Postgres client, no docker socket and no pyarrow, so it cannot run the forget workflow.
Its jobs `forget_logical` / `forget_physical` therefore only *enqueue*: they write one JSON file with a phase and
request ids (uuids from privacy.forget_requests) into a directory the host also sees (the event-log mount). The
executor, `scripts/forget.py run`, reads the queue, looks the requests up in Postgres and runs the workflow.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

PHASES = ("logical", "physical")
QUEUE_SUBDIR = "forget_queue"


def clean_ids(request_ids: list[str]) -> list[str]:
    """Normalised uuid strings, sorted and de-duplicated. Anything that is not a uuid is refused, so a raw customer
    id can never travel through a job's run config by accident."""
    out = set()
    for r in request_ids:
        try:
            out.add(str(uuid.UUID(str(r))))
        except ValueError as e:
            raise ValueError("request_ids must be uuids from privacy.forget_requests") from e
    return sorted(out)


def entry(phase: str, request_ids: list[str], now: float | None = None) -> dict[str, Any]:
    if phase not in PHASES:
        raise ValueError(f"unknown phase {phase!r}")
    ids = clean_ids(request_ids)
    if not ids:
        raise ValueError("no request ids")
    return {"phase": phase, "request_ids": ids, "queued_at": time.time() if now is None else now}


def write_entry(directory: Path, phase: str, request_ids: list[str]) -> Path:
    """Atomically write one queue file; returns its path."""
    e = entry(phase, request_ids)
    directory.mkdir(parents=True, exist_ok=True)
    name = f"{int(e['queued_at'] * 1000):013d}-{phase}-{uuid.uuid4().hex[:8]}.json"
    tmp = directory / f".{name}.tmp"
    tmp.write_text(json.dumps(e))
    os.replace(tmp, directory / name)
    return directory / name


def read_entries(directory: Path) -> list[tuple[Path, dict[str, Any]]]:
    """Queued entries oldest first. A malformed file is skipped (and left for a human to look at)."""
    out: list[tuple[Path, dict[str, Any]]] = []
    for p in sorted(directory.glob("*.json")) if directory.is_dir() else []:
        try:
            e = json.loads(p.read_text())
            entry(e["phase"], e["request_ids"], e.get("queued_at"))
        except (ValueError, KeyError, TypeError):
            continue
        out.append((p, e))
    return out


def pending_request_ids(directory: Path, phase: str) -> list[str]:
    return clean_ids([r for _, e in read_entries(directory) if e["phase"] == phase for r in e["request_ids"]])


def done(path: Path) -> None:
    path.unlink(missing_ok=True)
