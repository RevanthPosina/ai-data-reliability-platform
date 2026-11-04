"""Object-store scanner for the forget workflow (G8): lists every object of a store, decodes what is compressed
or encoded, and counts occurrences of a subject's PII with adrp.forget.ids.Matcher.

Why decoding: raw bytes are only a partial check. Parquet pages are compressed, Iceberg manifests and manifest
lists are Avro (compressed, and their lower/upper bounds are binary: a removed row survives there as the bounds
of a DELETED entry), `*.gz.metadata.json` is gzip. So per object kind:

  parquet   raw bytes; every string / binary / integer / nested value of every column; footer min/max statistics
            and footer key-value metadata                              (needs pyarrow: the app image or the host venv)
  avro      raw bytes; header metadata and every record, walked recursively (fastavro); 4- and 8-byte binary values
            are also read as little-endian integers, because that is how Iceberg stores integer bounds
  json      gunzipped when it starts with the gzip magic, then text-matched (table metadata.json)
  other     raw bytes only

Manifest bounds are truncated to 16 characters: build the Matcher with canary_prefixes(canaries) to see them.

Results are counts only: never the matched text (they end up in manifests and check details). Matched object keys
are listed (a key is an Iceberg file path, not data). Without pyarrow, parquet falls back to the raw check and the
result says so (`undecoded_parquet`): such a result is a weaker statement and `clean` stays False unless the
caller accepts it with `clean(allow_undecoded=True)`.

CLI (exit 0 clean, 1 matches found, 2 scan incomplete):
  python -m adrp.forget.scan --s3 [--bucket lake] --canaries a,b --ids 1,2 --schema-ids 3
  python -m adrp.forget.scan --local DIR --any-canary
S3 settings come from S3_ENDPOINT (default http://localhost:8333), S3_ACCESS_KEY, S3_SECRET_KEY, AWS_REGION.
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import sys
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse

from adrp.forget.ids import Hits, Matcher

ZERO = Hits()
MAX_MATCHED_KEYS = 50
GZIP_MAGIC = b"\x1f\x8b"


BOUND_LENGTH = 16  # Iceberg truncates string lower/upper bounds in manifests to 16 characters (default metrics mode)


def canary_prefixes(canaries: Iterable[str], length: int = BOUND_LENGTH) -> list[str]:
    """Canary patterns for a Matcher that must also see truncated bounds: the first `length` characters of each
    token. Distinct tokens keep distinct prefixes (`g8c-` + 12 hex digits), and a prefix also matches the full token."""
    return sorted({c[:length] for c in canaries})


def add(a: Hits, b: Hits) -> Hits:
    return Hits(a.canary + b.canary, a.subject_id + b.subject_id, a.key_bytes + b.key_bytes)


def hits_dict(h: Hits) -> dict[str, int]:
    return {"canary": h.canary, "subject_id": h.subject_id, "key_bytes": h.key_bytes, "total": h.total}


# ---- stores ------------------------------------------------------------------------------------------


class Store(Protocol):
    def keys(self) -> Iterator[str]: ...

    def read(self, key: str) -> bytes: ...


class LocalStore:
    """A directory tree (tests, a Hadoop-catalog warehouse, a copy of the bucket)."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)

    def keys(self) -> Iterator[str]:
        for dirpath, _, names in os.walk(self.root):
            for n in sorted(names):
                yield (Path(dirpath) / n).relative_to(self.root).as_posix()

    def read(self, key: str) -> bytes:
        return (self.root / key).read_bytes()


class ArrowS3Store:
    """An S3 bucket through pyarrow.fs (path-style addressing, which SeaweedFS needs). No boto3 dependency."""

    def __init__(self, fs: Any, bucket: str, prefix: str = "") -> None:
        self.fs = fs
        self.bucket = bucket
        self.prefix = prefix.strip("/")

    def keys(self) -> Iterator[str]:
        from pyarrow import fs as pafs

        base = f"{self.bucket}/{self.prefix}" if self.prefix else self.bucket
        for info in self.fs.get_file_info(pafs.FileSelector(base, recursive=True)):
            if info.type == pafs.FileType.File:
                yield info.path[len(self.bucket) + 1 :]

    def read(self, key: str) -> bytes:
        with self.fs.open_input_stream(f"{self.bucket}/{key}") as f:
            return bytes(f.read())


def s3_store(env: Mapping[str, str] | None = None, bucket: str = "lake", prefix: str = "") -> ArrowS3Store:
    from pyarrow import fs as pafs

    e = env if env is not None else os.environ
    url = urlparse(e.get("S3_ENDPOINT", "http://localhost:8333"))
    fs = pafs.S3FileSystem(
        access_key=e["S3_ACCESS_KEY"],
        secret_key=e["S3_SECRET_KEY"],
        endpoint_override=url.netloc,
        scheme=url.scheme or "http",
        region=e.get("AWS_REGION", "us-east-1"),
    )
    return ArrowS3Store(fs, bucket, prefix)


# ---- decoding ----------------------------------------------------------------------------------------


def bytes_hits(m: Matcher, b: bytes) -> Hits:
    h = m.hits(b)
    if len(b) in (4, 8):  # Iceberg bounds / Parquet plain integers: little-endian two's complement
        h = add(h, m.hits(str(int.from_bytes(b, "little", signed=True))))
    return h


def walk(m: Matcher, obj: Any) -> Hits:
    """Match every string, bytes and integer found in a decoded structure (dict keys included)."""
    if obj is None or isinstance(obj, (bool, float)):
        return ZERO
    if isinstance(obj, str):
        return m.hits(obj)
    if isinstance(obj, (bytes, bytearray, memoryview)):
        return bytes_hits(m, bytes(obj))
    if isinstance(obj, int):
        return m.hits(str(obj))
    if isinstance(obj, Mapping):
        total = ZERO
        for k, v in obj.items():
            total = add(add(total, walk(m, k)), walk(m, v))
        return total
    if isinstance(obj, (list, tuple, set)):
        total = ZERO
        for v in obj:
            total = add(total, walk(m, v))
        return total
    return ZERO  # dates, timestamps, decimals: no text to hold a subject's PII


def parquet_hits(m: Matcher, data: bytes) -> Hits:
    import pyarrow as pa
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(io.BytesIO(data))
    md = pf.metadata
    total = walk(m, md.metadata or {})
    for rg in range(md.num_row_groups):
        for c in range(md.num_columns):
            st = md.row_group(rg).column(c).statistics
            if st is not None and st.has_min_max:
                total = add(total, add(walk(m, st.min), walk(m, st.max)))
    for batch in pf.iter_batches(batch_size=8192):
        for col in batch.columns:
            t = col.type
            if pa.types.is_floating(t) or pa.types.is_boolean(t) or pa.types.is_temporal(t) or pa.types.is_null(t):
                continue
            if pa.types.is_decimal(t):
                continue
            total = add(total, walk(m, col.to_pylist()))
    return total


def avro_hits(m: Matcher, data: bytes) -> Hits:
    import fastavro

    reader = fastavro.reader(io.BytesIO(data))
    total = walk(m, dict(reader.metadata))
    for rec in reader:
        total = add(total, walk(m, rec))
    return total


def pyarrow_available() -> bool:
    try:
        import pyarrow.parquet  # noqa: F401
    except ImportError:
        return False
    return True


# ---- scan --------------------------------------------------------------------------------------------


def kind_of(key: str) -> str:
    name = key.rsplit("/", 1)[-1]
    if name.endswith(".parquet"):
        return "parquet"
    if name.endswith(".avro"):
        return "avro"
    if name.endswith(".json") or name.endswith(".json.gz") or name.endswith(".gz"):
        return "json"
    return "other"


@dataclass
class KindStats:
    files: int = 0
    bytes: int = 0
    files_with_hits: int = 0
    raw: Hits = ZERO
    decoded: Hits = ZERO
    errors: int = 0
    undecoded: int = 0


@dataclass
class ScanResult:
    kinds: dict[str, KindStats] = field(default_factory=dict)
    matched_keys: list[str] = field(default_factory=list)
    matched_files: int = 0
    error_keys: list[str] = field(default_factory=list)
    parquet_decoded: bool = True

    @property
    def files(self) -> int:
        return sum(k.files for k in self.kinds.values())

    @property
    def total_hits(self) -> int:
        return sum(k.raw.total + k.decoded.total for k in self.kinds.values())

    @property
    def errors(self) -> int:
        return sum(k.errors for k in self.kinds.values())

    @property
    def undecoded_parquet(self) -> int:
        return sum(k.undecoded for k in self.kinds.values())

    def clean(self, allow_undecoded: bool = False) -> bool:
        """No match, every object read and decoded (parquet raw-only counts as incomplete unless allowed)."""
        return self.total_hits == 0 and self.errors == 0 and (allow_undecoded or self.undecoded_parquet == 0)

    def to_dict(self) -> dict[str, Any]:
        return {
            "files": self.files,
            "bytes": sum(k.bytes for k in self.kinds.values()),
            "total_hits": self.total_hits,
            "matched_files": self.matched_files,
            "matched_keys": self.matched_keys,
            "errors": self.errors,
            "error_keys": self.error_keys,
            "undecoded_parquet": self.undecoded_parquet,
            "parquet_decoded": self.parquet_decoded,
            "kinds": {
                n: {
                    "files": k.files, "bytes": k.bytes, "files_with_hits": k.files_with_hits,
                    "raw": hits_dict(k.raw), "decoded": hits_dict(k.decoded),
                    "errors": k.errors, "undecoded": k.undecoded,
                }
                for n, k in sorted(self.kinds.items())
            },
        }  # fmt: skip


def scan_object(m: Matcher, key: str, data: bytes, decode_parquet: bool) -> tuple[Hits, Hits, bool, bool]:
    """(raw hits, decoded hits, decode_error, parquet_left_undecoded) for one object."""
    kind = kind_of(key)
    if kind == "json":
        if data[:2] == GZIP_MAGIC:
            try:
                return ZERO, m.hits(gzip.decompress(data)), False, False
            except (OSError, EOFError):
                return m.hits(data), ZERO, True, False
        return m.hits(data), ZERO, False, False
    raw = m.hits(data)
    try:
        if kind == "parquet":
            if not decode_parquet:
                return raw, ZERO, False, True
            return raw, parquet_hits(m, data), False, False
        if kind == "avro":
            return raw, avro_hits(m, data), False, False
    except Exception:  # noqa: BLE001 - pyarrow / fastavro raise their own families; any failure is "not verified"
        return raw, ZERO, True, False
    return raw, ZERO, False, False


def scan(store: Store, matcher: Matcher, keys: Iterable[str] | None = None) -> ScanResult:
    res = ScanResult(parquet_decoded=pyarrow_available())
    for key in keys if keys is not None else store.keys():
        data = store.read(key)
        raw, decoded, error, undecoded = scan_object(matcher, key, data, res.parquet_decoded)
        ks = res.kinds.setdefault(kind_of(key), KindStats())
        ks.files += 1
        ks.bytes += len(data)
        ks.raw = add(ks.raw, raw)
        ks.decoded = add(ks.decoded, decoded)
        ks.errors += int(error)
        ks.undecoded += int(undecoded)
        if raw or decoded:
            ks.files_with_hits += 1
            res.matched_files += 1
            if len(res.matched_keys) < MAX_MATCHED_KEYS:
                res.matched_keys.append(key)
        if error and len(res.error_keys) < MAX_MATCHED_KEYS:
            res.error_keys.append(key)
    return res


# ---- cli ---------------------------------------------------------------------------------------------


def split_ints(text: str) -> list[int]:
    return [int(x) for x in text.split(",") if x.strip()]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    where = ap.add_mutually_exclusive_group(required=True)
    where.add_argument("--local", help="directory to scan")
    where.add_argument("--s3", action="store_true", help="scan the whole bucket")
    ap.add_argument("--bucket", default="lake")
    ap.add_argument("--prefix", default="")
    ap.add_argument("--canaries", default="", help="comma list of exact canary tokens")
    ap.add_argument("--any-canary", action="store_true", help="match any g8c- token")
    ap.add_argument("--exact-canaries", action="store_true", help="do not also match 16-char truncated bounds")
    ap.add_argument("--ids", default="", help="comma list of subject ids (decimal text, 13-digit ids)")
    ap.add_argument("--schema-ids", default="", help="registry schema ids, for Confluent-framed key bytes")
    ap.add_argument("--allow-undecoded", action="store_true")
    a = ap.parse_args(argv)
    canaries = [c for c in a.canaries.split(",") if c]
    if not (canaries or a.any_canary or a.ids):
        ap.error("give --canaries, --any-canary or --ids")
    matcher = Matcher(
        subject_ids=split_ints(a.ids),
        canaries=None if a.any_canary else (canaries if a.exact_canaries else canary_prefixes(canaries)),
        schema_ids=split_ints(a.schema_ids),
    )
    store: Store = LocalStore(a.local) if a.local else s3_store(bucket=a.bucket, prefix=a.prefix)
    res = scan(store, matcher)
    print("RESULT " + json.dumps(res.to_dict()), flush=True)
    if res.total_hits:
        return 1
    return 0 if res.clean(a.allow_undecoded) else 2


if __name__ == "__main__":
    sys.exit(main())
