from decimal import Decimal

from adrp.gen.model import Generator, stream_digest


def test_same_seed_same_stream() -> None:
    assert stream_digest(42, 0, 1500) == stream_digest(42, 0, 1500)


def test_different_seed_different_stream() -> None:
    assert stream_digest(42, 0, 500)[0] != stream_digest(43, 0, 500)[0]


def test_resume_from_offset_matches_continuous_run() -> None:
    # A run that resumes at txn 700 must emit exactly what an uninterrupted run emits there.
    whole = Generator(7)
    ops_whole = [op for t in whole.txns(1000) for op in t.ops]
    resumed = Generator(7)
    resumed.skip(700)
    ops_resumed = [op for t in resumed.txns(300) for op in t.ops]
    assert ops_resumed == [op for op in ops_whole if op.txn_no >= 700]


def test_invariants_over_a_long_stream() -> None:
    gen = Generator(11)
    seen: dict[tuple[str, int], int] = {}
    deleted: set[tuple[str, int]] = set()
    last_seq = -1
    for txn in gen.txns(4000):
        for op in txn.ops:
            key = (op.table, op.pk)
            assert op.seq == last_seq + 1
            last_seq = op.seq
            assert key not in deleted, "no op after a delete"
            assert op.event_seq == seen.get(key, 0) + 1, "per-entity event_seq is gap-free"
            assert (op.op == "I") == (op.event_seq == 1)
            seen[key] = op.event_seq
            if op.op == "D":
                deleted.add(key)
    # Balances are maintained transactionally and never go negative for customers.
    for acct in gen.s.accounts.values():
        if acct["owner_type"] == "customer":
            assert acct["balance"] >= Decimal("0.00")


def test_skew_some_customers_far_more_active() -> None:
    gen = Generator(3)
    counts: dict[int, int] = {}
    for txn in gen.txns(6000):
        for op in txn.ops:
            if op.table == "transactions" and op.row is not None:
                counts[op.row["customer_id"]] = counts.get(op.row["customer_id"], 0) + 1
    ranked = sorted(counts.values(), reverse=True)
    top_decile = sum(ranked[: max(1, len(ranked) // 10)])
    assert top_decile > 0.5 * sum(ranked)


def test_some_articles_large_enough_for_toast() -> None:
    gen = Generator(5)
    sizes = [
        len(op.row["body"])
        for t in gen.txns(400)
        for op in t.ops
        if op.table == "help_articles" and op.op == "I" and op.row
    ]
    assert max(sizes) > 8000
