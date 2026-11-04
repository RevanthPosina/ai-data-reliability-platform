import re

import pytest

from adrp.forget import postgres as pg

CID = 2_000_000_000_123
RID = "0b6f5f0e-5b0e-4a53-9f3a-2a5a3f1f3d11"


def test_forget_script_embeds_the_id_once_and_orders_the_statements() -> None:
    s = pg.forget_script(CID)
    assert s.count(str(CID)) == 1 and s.startswith(f"\\set cid {CID}\n")
    pos = [s.index(k) for k in ("INSERT INTO customers", "UPDATE accounts", "UPDATE transactions",
                                "DELETE FROM support_tickets", "DELETE FROM customers", "COMMIT;")]  # fmt: skip
    assert pos == sorted(pos)
    assert s.index("FOR UPDATE") < pos[0] and "\\if :found" in s and "ROLLBACK;" in s


def test_forget_script_placeholder_pii_is_the_redacted_constant() -> None:
    s = pg.forget_script(CID)
    for v in pg.REDACTED.values():
        assert f"'{v}'" in s
    assert not re.search(r"INSERT INTO customers \(id", s)  # the placeholder takes the next identity value


def test_forget_script_rejects_a_non_integer() -> None:
    with pytest.raises(ValueError):
        pg.forget_script("1; DROP TABLE x")  # type: ignore[arg-type]


def test_parse_forget_output_found() -> None:
    lines = ["xid32|12178", "begin_lsn|0/134ACEA0", "placeholder|1000000000007", "accounts|1"]
    lines += ["transactions|3", "support_tickets|2", "customers|1", "end_lsn|0/134AD940"]
    out = "\n".join(lines)
    t = pg.parse_forget_output(out)
    assert t.found and t.xid32 == 12178 and (t.begin_lsn, t.end_lsn) == ("0/134ACEA0", "0/134AD940")
    assert t.placeholder_id == 1_000_000_000_007
    assert dict(t.counts) == {"accounts": 1, "transactions": 3, "support_tickets": 2, "customers": 1}


def test_parse_forget_output_not_found_and_truncated() -> None:
    assert not pg.parse_forget_output("not_found|1").found
    assert not pg.parse_forget_output("xid32|1\nbegin_lsn|0/1").found  # no end_lsn: the commit did not happen


def test_lit_and_ident_and_uuid_validation() -> None:
    assert pg.lit("it's") == "'it''s'"
    assert pg.ident("privacy.forget_requests") == "privacy.forget_requests"
    for bad in ("x; drop", "A.b", "a.b.c", "1x", "a b"):
        with pytest.raises(ValueError):
            pg.ident(bad)
    assert pg._uuid(RID.upper()) == RID
    with pytest.raises(ValueError):
        pg._uuid("x'; drop")


def test_scrub_hides_long_digit_runs() -> None:
    err = "ERROR: duplicate key\nDETAIL:  Key (id)=(2000000000999) already exists. xid 123"
    out = pg.scrub(err)
    assert "2000000000999" not in out and "xid 123" in out


def test_set_status_rejects_unknown_status() -> None:
    with pytest.raises(ValueError):
        pg.set_status(RID, "done")


def test_xid_after_is_wraparound_safe() -> None:
    assert pg.xid_after(100, 100) and pg.xid_after(101, 100) and not pg.xid_after(99, 100)
    assert pg.xid_after(5, 4_294_967_290)  # the counter wrapped past the marker
    assert not pg.xid_after(4_294_967_290, 5)


def test_vacuum_script_vacuums_toast_after_each_table() -> None:
    s = pg.vacuum_script(["public.customers", "privacy.forget_requests"], {"public.customers": "pg_toast.pg_toast_1"})
    assert s.splitlines() == [
        "VACUUM (FULL, VERBOSE) public.customers;",
        "VACUUM (FULL, VERBOSE) pg_toast.pg_toast_1;",
        "VACUUM (FULL, VERBOSE) privacy.forget_requests;",
    ]
    with pytest.raises(ValueError):
        pg.vacuum_script(["x; drop table y"], {})


def test_parse_vacuum_blocked_sums_the_cannot_be_removed_lines() -> None:
    err = (
        'INFO:  vacuuming "public.customers"\n'
        'INFO:  "customers": found 3 removable, 7 nonremovable row versions in 1 pages\n'
        "DETAIL:  2 dead row versions cannot be removed yet.\n"
        "DETAIL:  0 dead row versions cannot be removed yet.\n"
        "DETAIL:  5 dead row versions cannot be removed yet.\n"
    )
    assert pg.parse_vacuum_blocked(err) == 7
    assert pg.parse_vacuum_blocked("") == 0


def test_wal_bound_statement_distinguishes_removed_from_present() -> None:
    gone = pg.wal_bound_statement(80, 16, 64 * 2**20, 7, True)
    still = pg.wal_bound_statement(80, 16, None, 40, False)
    assert "absent from pg_wal after 7" in gone and "5 x 16 MiB" in gone and "64.0 MiB" in gone
    assert "still present" in still and "not removed" in still and "unknown" in still
