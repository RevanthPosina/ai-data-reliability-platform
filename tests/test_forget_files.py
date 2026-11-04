import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from adrp.forget import files, ids

KEY = "forget_key_for_tests"
TICKET = "support_tickets:1000000000123"
OTHER = "support_tickets:1000000000124"
P = ids.pseudonym(KEY, TICKET)


def test_jsonl_rewrites_only_matching_lines_and_is_idempotent(tmp_path: Path) -> None:
    f = tmp_path / "actions.jsonl"
    keep = json.dumps({"batch": 1, "entity": OTHER, "lsn": 5})
    f.write_text(
        json.dumps({"batch": 1, "entity": TICKET, "lsn": 4})
        + "\n"
        + keep
        + "\nnot json\n"
        + json.dumps({"entity": TICKET})
    )
    st = files.rewrite_jsonl(f, {TICKET: P})
    assert (st.files_rewritten, st.items_changed) == (1, 2)
    lines = f.read_text().split("\n")
    assert json.loads(lines[0]) == {"batch": 1, "entity": P, "lsn": 4}
    assert lines[1] == keep and lines[2] == "not json"
    assert json.loads(lines[3]) == {"entity": P} and not f.read_text().endswith("\n")
    assert TICKET not in f.read_text()
    assert files.rewrite_jsonl(f, {TICKET: P}).files_rewritten == 0
    assert not list(tmp_path.glob("_forget-*"))


def test_missing_files_are_skipped(tmp_path: Path) -> None:
    assert files.rewrite_jsonl(tmp_path / "nope.jsonl", {TICKET: P}) == files.Stats()
    assert files.rewrite_state_json(tmp_path / "nope.json", {TICKET: P}) == files.Stats()
    assert files.rewrite_silver_batches(tmp_path / "nope", {TICKET: P}) == files.Stats()


def test_state_json_renames_keys_and_keeps_the_rest(tmp_path: Path) -> None:
    f = tmp_path / "embedder.json"
    state = {"snapshot": 99, "pending": {TICKET: 7, OTHER: 8}, "since": {TICKET: 1.5}, "batch": 3}
    f.write_text(json.dumps(state))
    st = files.rewrite_state_json(f, {TICKET: P})
    assert (st.files_rewritten, st.items_changed) == (1, 2)
    assert json.loads(f.read_text()) == {"snapshot": 99, "pending": {P: 7, OTHER: 8}, "since": {P: 1.5}, "batch": 3}
    assert files.rewrite_state_json(f, {TICKET: P}).files_rewritten == 0


SILVER_SCHEMA = pa.schema(
    [
        ("batch_id", pa.int64()),
        ("table", pa.string()),
        ("max_lsn", pa.int64()),
        ("changes", pa.list_(pa.struct([("pk", pa.string()), ("lsn", pa.int64())]))),
        ("written_at", pa.timestamp("us", tz="UTC")),
    ]
)


def write_silver(d: Path, name: str, rows: list[dict[str, object]]) -> Path:
    d.mkdir(exist_ok=True)
    p = d / name
    pq.write_table(pa.Table.from_pylist(rows, schema=SILVER_SCHEMA), p)
    return p


def silver_row(batch: int, table: str, pks: list[int]) -> dict[str, object]:
    return {
        "batch_id": batch, "table": table, "max_lsn": 10,
        "changes": [{"pk": json.dumps({"id": i}, separators=(",", ":")), "lsn": 10 + i % 3} for i in pks],
        "written_at": datetime(2026, 10, 1, tzinfo=timezone.utc),
    }  # fmt: skip


def test_silver_batches_replace_pk_keep_lsn_and_untouched_files(tmp_path: Path) -> None:
    d = tmp_path / "silver_batches"
    hit = write_silver(d, "part-0.snappy.parquet", [silver_row(1, "support_tickets", [1000000000123, 1000000000124]),
                                                    silver_row(1, "customers", [1000000000123])])  # fmt: skip
    clean = write_silver(d, "part-1.snappy.parquet", [silver_row(2, "support_tickets", [1000000000124])])
    clean_bytes = clean.read_bytes()
    crc = d / f".{hit.name}.crc"
    crc.write_bytes(b"stale")
    (d / "_SUCCESS").write_text("")

    st = files.rewrite_silver_batches(d, {TICKET: P})
    assert (st.files_scanned, st.files_rewritten, st.items_changed) == (2, 1, 1)
    assert clean.read_bytes() == clean_bytes and not crc.exists()
    t = pq.read_table(hit)
    assert t.schema.equals(SILVER_SCHEMA)
    assert [c["pk"] for c in t["changes"].to_pylist()[0]] == [P, '{"id":1000000000124}']
    assert t["changes"].to_pylist()[0][0]["lsn"] == 10 + 1000000000123 % 3
    assert t["changes"].to_pylist()[1][0]["pk"] == '{"id":1000000000123}'  # same id, other table: not forgotten
    assert b"1000000000123" not in hit.read_bytes().replace(b'{"id":1000000000123}', b"")
    assert files.rewrite_silver_batches(d, {TICKET: P}).files_rewritten == 0
    assert sorted(x.name for x in d.iterdir() if x.name != crc.name) == [
        "_SUCCESS",
        "part-0.snappy.parquet",
        "part-1.snappy.parquet",
    ]


def test_silver_batches_ignore_unparsable_and_keyless_pks(tmp_path: Path) -> None:
    d = tmp_path / "sb"
    row = silver_row(1, "support_tickets", [])
    row["changes"] = [{"pk": "garbage", "lsn": 1}, {"pk": '{"other":1}', "lsn": 2}, {"pk": None, "lsn": 3}]
    write_silver(d, "part-0.parquet", [row, {**row, "changes": None}])
    assert files.rewrite_silver_batches(d, {TICKET: P}).files_rewritten == 0


GT_SCHEMA = pa.schema(
    [
        ("seq", pa.int64()),
        ("txn_no", pa.int64()),
        ("op", pa.string()),
        ("table", pa.string()),
        ("pk", pa.int64()),
        ("event_seq", pa.int64()),
        ("row_json", pa.string()),
    ]
)


def gt_row(seq: int, txn: int, op: str, table: str, pk: int, row_json: str | None) -> dict[str, object]:
    return {"seq": seq, "txn_no": txn, "op": op, "table": table, "pk": pk, "event_seq": 0, "row_json": row_json}


def test_ground_truth_pk_becomes_negative_int_and_row_json_is_nulled(tmp_path: Path) -> None:
    d = tmp_path / "20261001T000000Z" / "ground_truth"
    d.mkdir(parents=True)
    rows = [
        gt_row(1, 1, "c", "customers", 17, '{"id":17,"email":"a@b"}'),
        gt_row(2, 1, "u", "customers", 17, '{"id":17,"email":"c@d"}'),
        gt_row(3, 2, "d", "support_tickets", 17, None),
        gt_row(4, 2, "c", "customers", 18, '{"id":18}'),
    ]
    f = d / "part-00000.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=GT_SCHEMA), f)
    repl = ids.pseudonym_int(KEY, "customers:17")
    st = files.rewrite_ground_truth(d, {"customers:17": repl})
    assert (st.files_rewritten, st.items_changed) == (1, 2)
    t = pq.read_table(f)
    assert t.schema.equals(GT_SCHEMA)
    got = t.to_pylist()
    assert [(r["pk"], r["row_json"]) for r in got] == [(repl, None), (repl, None), (17, None), (18, '{"id":18}')]
    assert repl < 0 and [r["seq"] for r in got] == [1, 2, 3, 4]
    assert files.rewrite_ground_truth(d, {"customers:17": repl}).files_rewritten == 0
    assert files.ground_truth_dirs(tmp_path) == [d]


def test_stale_temp_files_are_removed(tmp_path: Path) -> None:
    d = tmp_path / "gt"
    d.mkdir()
    (d / "_forget-part-0.parquet.tmp").write_bytes(b"x")
    files.rewrite_ground_truth(d, {})
    assert not list(d.iterdir())
