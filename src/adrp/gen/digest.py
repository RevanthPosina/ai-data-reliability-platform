"""Digest tools for the G1 determinism check.

python -m adrp.gen.digest stream --seed 42 --start 0 --count 5000   # no database
python -m adrp.gen.digest parquet --dir /runs/data/<run_id>/ground_truth
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pyarrow.parquet as pq

from adrp.gen.model import stream_digest


def parquet_digest(directory: Path) -> tuple[str, int]:
    """Recompute the stream digest from a ground-truth log (same identity encoding as Op.identity)."""
    import hashlib

    h = hashlib.sha256()
    n = 0
    table = pq.read_table(directory).sort_by("seq")
    for r in table.to_pylist():
        row = json.loads(r["row_json"]) if r["row_json"] is not None else None
        ident = json.dumps([r["seq"], r["op"], r["table"], r["pk"], row], sort_keys=True, separators=(",", ":"))
        h.update(ident.encode())
        h.update(b"\n")
        n += 1
    return h.hexdigest(), n


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("stream")
    s.add_argument("--seed", type=int, required=True)
    s.add_argument("--start", type=int, default=0)
    s.add_argument("--count", type=int, required=True)
    p = sub.add_parser("parquet")
    p.add_argument("--dir", type=Path, required=True)
    args = ap.parse_args()
    if args.cmd == "stream":
        digest, ops = stream_digest(args.seed, args.start, args.count)
    else:
        digest, ops = parquet_digest(args.dir)
    print("DIGEST " + json.dumps({"sha256": digest, "ops": ops}))


if __name__ == "__main__":
    main()
