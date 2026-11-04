"""Atomic in-place rewrites of the pipeline's own files so they stop naming a forgotten subject (G8).

Every rewrite writes a temp file next to the original, fsyncs it, and renames it over the original, so a reader
sees the old file or the new one and a crash leaves at most a temp file (which holds only already-rewritten
content). Unlinking the replaced inode is what releases the old bytes. A file with nothing to change is not
touched. The maps are entity id -> replacement, with entity ids spelled as the pipeline spells them
(`support_tickets:1000000000123`); build the replacements with adrp.forget.ids.pseudonym / pseudonym_int.

Callers stop the writer first (embedder drained, spark-streams stopped): nothing here locks against appends.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

TMP_PREFIX = "_forget-"  # leading underscore: Spark and pyarrow dataset readers skip such files


@dataclass
class Stats:
    files_scanned: int = 0
    files_rewritten: int = 0
    items_changed: int = 0  # JSONL lines, state keys, silver changes entries, ground-truth rows

    def add(self, other: Stats) -> None:
        self.files_scanned += other.files_scanned
        self.files_rewritten += other.files_rewritten
        self.items_changed += other.items_changed


def _tmp_for(path: Path) -> Path:
    return path.with_name(f"{TMP_PREFIX}{path.name}.tmp")


def _replace_bytes(path: Path, data: bytes) -> None:
    tmp = _tmp_for(path)
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


# ---- JSONL and state json ---------------------------------------------------------------------------


def rewrite_jsonl(path: Path, mapping: Mapping[str, str], field: str = "entity") -> Stats:
    """Replace record[field] where it is a key of `mapping` (embedder actions.jsonl). Other lines stay byte-exact."""
    st = Stats(files_scanned=1)
    if not path.exists():
        st.files_scanned = 0
        return st
    out: list[str] = []
    for line in path.read_text().splitlines(keepends=True):
        try:
            rec = json.loads(line)
        except ValueError:
            rec = None
        if isinstance(rec, dict) and rec.get(field) in mapping:
            rec[field] = mapping[rec[field]]
            line = json.dumps(rec) + ("\n" if line.endswith("\n") else "")
            st.items_changed += 1
        out.append(line)
    if st.items_changed:
        _replace_bytes(path, "".join(out).encode())
        st.files_rewritten = 1
    return st


def _rename_keys(obj: Any, mapping: Mapping[str, str], counter: list[int]) -> Any:
    if isinstance(obj, dict):
        renamed = {}
        for k, v in obj.items():
            if k in mapping:
                counter[0] += 1
                k = mapping[k]
            renamed[k] = _rename_keys(v, mapping, counter)
        return renamed
    if isinstance(obj, list):
        return [_rename_keys(v, mapping, counter) for v in obj]
    return obj


def rewrite_state_json(path: Path, mapping: Mapping[str, str]) -> Stats:
    """Rename dict keys found in `mapping`, at any depth (embedder.json: `pending` and `since` are keyed by entity)."""
    st = Stats(files_scanned=1)
    if not path.exists():
        st.files_scanned = 0
        return st
    counter = [0]
    state = _rename_keys(json.loads(path.read_text()), mapping, counter)
    if counter[0]:
        _replace_bytes(path, json.dumps(state).encode())
        st.files_rewritten = 1
        st.items_changed = counter[0]
    return st


# ---- Parquet directories ----------------------------------------------------------------------------


def rewrite_parquet_dir(directory: Path, transform: Callable[[pa.Table], tuple[pa.Table, int]]) -> Stats:
    """Apply transform(table) -> (table, n_changed) to every data file; rewrite only files where n_changed > 0.

    The schema, including Spark's embedded row metadata, is kept as read. Spark's `.<name>.crc` sidecar would no
    longer match the rewritten file (and Spark's local filesystem verifies it), so it is removed.
    """
    st = Stats()
    if not directory.is_dir():
        return st
    for stale in directory.glob(f"{TMP_PREFIX}*.tmp"):
        stale.unlink()
    for path in sorted(directory.glob("*.parquet")):
        st.files_scanned += 1
        table, n = transform(pq.read_table(path))
        if not n:
            continue
        tmp = _tmp_for(path)
        pq.write_table(table, tmp)
        with open(tmp, "rb") as fh:
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        path.with_name(f".{path.name}.crc").unlink(missing_ok=True)
        st.files_rewritten += 1
        st.items_changed += n
    return st


def silver_entity(table: str, pk: str | None) -> str | None:
    """Entity id of a silver batch log `changes.pk` (canonical JSON of the key record, e.g. '{"id":7}')."""
    try:
        key = json.loads(pk) if pk is not None else None
    except ValueError:
        return None
    if isinstance(key, dict) and "id" in key:
        return f"{table}:{key['id']}"
    return None


def rewrite_silver_batches(directory: Path, mapping: Mapping[str, str]) -> Stats:
    """silver_batches (streams.EVENT_LOG_SCHEMA): replace `changes[].pk` of forgotten entities, keep their lsn."""

    def transform(t: pa.Table) -> tuple[pa.Table, int]:
        n = 0
        new: list[Any] = []
        for table_name, changes in zip(t["table"].to_pylist(), t["changes"].to_pylist(), strict=True):
            if changes:
                for c in changes:
                    ent = silver_entity(table_name, c["pk"])
                    if ent is not None and ent in mapping:
                        c["pk"] = mapping[ent]
                        n += 1
            new.append(changes)
        if not n:
            return t, 0
        idx = t.schema.get_field_index("changes")
        col = pa.array(new, type=t.schema.field("changes").type)
        return t.set_column(idx, t.schema.field("changes"), col), n

    return rewrite_parquet_dir(directory, transform)


def rewrite_ground_truth(directory: Path, mapping: Mapping[str, int]) -> Stats:
    """Generator ground truth (runner.GROUND_TRUTH_SCHEMA): int64 `pk` -> pseudonym int, `row_json` -> NULL.

    `mapping` is keyed by entity id `<table>:<pk>`. A row that names a forgotten subject without being one of its
    entities (a transaction's customer_id inside row_json) is only caught if that row's own entity is in the map.
    """

    def transform(t: pa.Table) -> tuple[pa.Table, int]:
        pks = t["pk"].to_pylist()
        rows = t["row_json"].to_pylist()
        n = 0
        for i, (table_name, pk) in enumerate(zip(t["table"].to_pylist(), pks, strict=True)):
            repl = mapping.get(f"{table_name}:{pk}")
            if repl is not None:
                pks[i] = repl
                rows[i] = None
                n += 1
        if not n:
            return t, 0
        t = t.set_column(
            t.schema.get_field_index("pk"), t.schema.field("pk"), pa.array(pks, type=t.schema.field("pk").type)
        )
        t = t.set_column(
            t.schema.get_field_index("row_json"),
            t.schema.field("row_json"),
            pa.array(rows, type=t.schema.field("row_json").type),
        )
        return t, n

    return rewrite_parquet_dir(directory, transform)


def ground_truth_dirs(runs_data: Path) -> list[Path]:
    """Every run's ground_truth directory under runs/data (each run keeps its own)."""
    return sorted(p for p in runs_data.glob("*/ground_truth") if p.is_dir())
