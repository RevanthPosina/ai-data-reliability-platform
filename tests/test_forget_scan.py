import gzip
import io
import json
import struct
from pathlib import Path
from typing import Any

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from adrp.forget import ids, scan

CANARY = "g8c-" + "ab" * 16
OTHER = "g8c-" + "cd" * 16
SUBJECT = 2_000_000_000_123
SCHEMA_ID = 7


def matcher() -> ids.Matcher:
    return ids.Matcher(canaries=[CANARY], subject_ids=[SUBJECT], schema_ids=[SCHEMA_ID])


def write_parquet(path: Path, table: pa.Table, **kw: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, **kw)


def avro_bytes(schema: dict[str, Any], records: list[dict[str, Any]], codec: str = "deflate") -> bytes:
    buf = io.BytesIO()
    fastavro.writer(buf, schema, records, codec=codec)
    return buf.getvalue()


def test_clean_tree_is_clean_and_counted(tmp_path: Path) -> None:
    write_parquet(tmp_path / "data/a.parquet", pa.table({"s": ["x", "y"], "n": [1, 2]}))
    (tmp_path / "meta").mkdir()
    (tmp_path / "meta/v1.metadata.json").write_text(json.dumps({"schema": "none"}))
    res = scan.scan(scan.LocalStore(tmp_path), matcher())
    assert res.files == 2 and res.total_hits == 0 and res.errors == 0
    assert res.clean(allow_undecoded=False)
    assert res.matched_files == 0 and res.matched_keys == []


def test_parquet_string_binary_nested_and_int_columns(tmp_path: Path) -> None:
    # gzip pages + no statistics: the raw check cannot see the values, only decoding can
    t = pa.table(
        {
            "s": ["plain", f"mail {CANARY} end"],
            "b": [b"\x00\x01", b"xx" + CANARY.encode()],
            "nested": [["a"], ["b", CANARY]],
            "cid": [1, SUBJECT],
        }
    )
    write_parquet(tmp_path / "t.parquet", t, compression="gzip", write_statistics=False)
    res = scan.scan(scan.LocalStore(tmp_path), matcher())
    dec = res.kinds["parquet"].decoded
    assert dec.canary == 3 and dec.subject_id == 1
    assert res.matched_keys == ["t.parquet"] and not res.clean()
    # counts only: the serialized result never carries the matched text or the id
    text = json.dumps(res.to_dict())
    assert CANARY not in text and str(SUBJECT) not in text


def test_parquet_key_bytes_in_binary_column(tmp_path: Path) -> None:
    key = ids.confluent_key_bytes(SCHEMA_ID, SUBJECT)
    write_parquet(tmp_path / "k.parquet", pa.table({"key_bytes": [b"zz", key]}), compression="gzip")
    res = scan.scan(scan.LocalStore(tmp_path), matcher())
    assert res.kinds["parquet"].decoded.key_bytes >= 1


def test_parquet_footer_statistics_are_read(tmp_path: Path) -> None:
    # min of the column is the canary: the footer holds it even when only the stats are looked at
    t = pa.table({"s": [CANARY]})
    write_parquet(tmp_path / "s.parquet", t, compression="gzip")
    data = (tmp_path / "s.parquet").read_bytes()
    md = pq.ParquetFile(io.BytesIO(data)).metadata
    assert md.row_group(0).column(0).statistics.min == CANARY
    assert scan.parquet_hits(matcher(), data).canary >= 3  # value + min + max


def test_avro_manifest_bounds_binary_and_little_endian_ints(tmp_path: Path) -> None:
    schema: dict[str, Any] = {
        "type": "record",
        "name": "entry",
        "fields": [
            {"name": "status", "type": "int"},
            {
                "name": "lower_bounds",
                "type": {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "k12_v12",
                        "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": "bytes"}],
                    },
                },
            },
        ],
    }
    recs = [
        {"status": 2, "lower_bounds": [{"key": 3, "value": CANARY.encode()[:16] + b"x"}]},
        {"status": 2, "lower_bounds": [{"key": 1, "value": struct.pack("<q", SUBJECT)}]},
        {"status": 1, "lower_bounds": [{"key": 1, "value": struct.pack("<q", 5)}]},
    ]
    p = tmp_path / "metadata/snap-1-m0.avro"
    p.parent.mkdir()
    p.write_bytes(avro_bytes(schema, recs))
    # the canary prefix is only 16 chars (Iceberg truncates bounds): match the prefix as a custom canary
    m = ids.Matcher(canaries=[CANARY[:16]], subject_ids=[SUBJECT])
    res = scan.scan(scan.LocalStore(tmp_path), m)
    k = res.kinds["avro"]
    assert k.decoded.canary == 1 and k.decoded.subject_id == 1 and k.raw.total == 0  # raw is blind: deflate
    assert res.matched_keys == ["metadata/snap-1-m0.avro"]


def test_metadata_json_gzip_and_plain(tmp_path: Path) -> None:
    (tmp_path / "a.gz.metadata.json").write_bytes(gzip.compress(json.dumps({"x": CANARY}).encode()))
    (tmp_path / "b.metadata.json").write_text(json.dumps({"x": "nothing"}))
    res = scan.scan(scan.LocalStore(tmp_path), matcher())
    assert res.kinds["json"].files == 2 and res.kinds["json"].decoded.canary == 1
    assert res.kinds["json"].raw.total == 0  # gz: raw is blind
    assert res.matched_keys == ["a.gz.metadata.json"]


def test_other_files_raw_and_corrupt_files_are_errors(tmp_path: Path) -> None:
    (tmp_path / "blob.bin").write_bytes(b"\x00" + CANARY.encode() + b"\x00")
    (tmp_path / "bad.parquet").write_bytes(b"not parquet")
    (tmp_path / "bad.avro").write_bytes(b"not avro")
    res = scan.scan(scan.LocalStore(tmp_path), matcher())
    assert res.kinds["other"].raw.canary == 1
    assert res.errors == 2 and sorted(res.error_keys) == ["bad.avro", "bad.parquet"]
    assert not res.clean()


def test_other_subjects_do_not_match(tmp_path: Path) -> None:
    write_parquet(tmp_path / "t.parquet", pa.table({"s": [OTHER, "2000000000124", "12000000000123"]}))
    assert scan.scan(scan.LocalStore(tmp_path), matcher()).total_hits == 0


def test_without_pyarrow_parquet_is_raw_only_and_not_clean(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_parquet(tmp_path / "t.parquet", pa.table({"s": ["x"]}))
    monkeypatch.setattr(scan, "pyarrow_available", lambda: False)
    res = scan.scan(scan.LocalStore(tmp_path), matcher())
    assert res.undecoded_parquet == 1 and not res.parquet_decoded
    assert not res.clean() and res.clean(allow_undecoded=True)


def test_matched_keys_are_capped(tmp_path: Path) -> None:
    for i in range(scan.MAX_MATCHED_KEYS + 5):
        (tmp_path / f"f{i}.bin").write_bytes(CANARY.encode())
    res = scan.scan(scan.LocalStore(tmp_path), matcher())
    assert res.matched_files == scan.MAX_MATCHED_KEYS + 5 and len(res.matched_keys) == scan.MAX_MATCHED_KEYS


def test_cli_exit_codes_and_output(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    write_parquet(tmp_path / "t.parquet", pa.table({"s": ["x"]}))
    assert scan.main(["--local", str(tmp_path), "--canaries", CANARY]) == 0
    out = capsys.readouterr().out
    assert out.startswith("RESULT ") and json.loads(out[len("RESULT ") :])["total_hits"] == 0
    (tmp_path / "leak.txt").write_text(CANARY)
    assert scan.main(["--local", str(tmp_path), "--any-canary"]) == 1
    assert CANARY not in capsys.readouterr().out
