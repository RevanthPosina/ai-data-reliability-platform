"""Property test of the merge logic (PLAN G3): random per-key event sequences with shuffles, duplicates,
replays, arbitrary batch boundaries and crash re-application must always end at the oracle state."""

import itertools

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from adrp.merge_model import Event, Row, apply_batch, dedupe, oracle

OPS = st.sampled_from(["r", "c", "u", "d"])


@st.composite
def histories(draw: st.DrawFn) -> list[Event]:
    """Events with globally unique, strictly increasing LSNs, over a few keys, any op order
    (including delete followed by re-insert of the same key)."""
    n = draw(st.integers(min_value=1, max_value=40))
    keys = draw(st.lists(st.integers(min_value=1, max_value=6), min_size=n, max_size=n))
    ops = draw(st.lists(OPS, min_size=n, max_size=n))
    gaps = draw(st.lists(st.integers(min_value=1, max_value=5), min_size=n, max_size=n))
    out, lsn = [], 100
    for i, (k, op, gap) in enumerate(zip(keys, ops, gaps, strict=True)):
        lsn += gap
        out.append(Event(k, lsn, op, None if op == "d" else {"v": i}, offset=i))
    return out


@st.composite
def deliveries(draw: st.DrawFn) -> tuple[list[Event], list[list[Event]]]:
    """(history, batches): the history shuffled, with duplicates, cut into batches, with some batches replayed."""
    history = draw(histories())
    extra = draw(st.lists(st.sampled_from(history), max_size=len(history)))  # duplicates / replays
    stream = draw(st.permutations(history + extra))
    cuts = sorted(set(draw(st.lists(st.integers(0, len(stream)), max_size=8))))
    bounds = [0, *cuts, len(stream)]
    batches = [list(stream[a:b]) for a, b in itertools.pairwise(bounds) if b > a]
    replays = draw(st.lists(st.integers(0, max(len(batches) - 1, 0)), max_size=4))
    for r in replays:  # a crash re-applies a batch after later ones
        if batches:
            batches.append(list(batches[r]))
    return history, batches


@settings(max_examples=1000, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(deliveries())
def test_any_delivery_converges_to_oracle(data: tuple[list[Event], list[list[Event]]]) -> None:
    history, batches = data
    state: dict[int, Row] = {}
    for b in batches:
        state = apply_batch(state, b)
    assert state == oracle(history)


@settings(max_examples=300, deadline=None)
@given(deliveries())
def test_applying_a_batch_twice_changes_nothing(data: tuple[list[Event], list[list[Event]]]) -> None:
    _, batches = data
    state: dict[int, Row] = {}
    for b in batches:
        once = apply_batch(state, b)
        assert apply_batch(once, b) == once
        state = once


@settings(max_examples=300, deadline=None)
@given(deliveries())
def test_deleted_keys_stay_deleted_and_carry_no_values(data: tuple[list[Event], list[list[Event]]]) -> None:
    history, batches = data
    state: dict[int, Row] = {}
    for b in batches:
        state = apply_batch(state, b)
    for key, row in state.items():
        last = max((e for e in history if e.key == key), key=lambda e: e.lsn)
        assert row.is_deleted == (last.op == "d")  # a later insert may re-create, but never an older one
        assert (row.values is None) == row.is_deleted


def test_dedupe_keeps_highest_lsn_then_offset() -> None:
    evs = [Event(1, 10, "r", {"v": 1}, 5), Event(1, 10, "u", {"v": 2}, 6), Event(1, 9, "u", {"v": 0}, 99)]
    assert dedupe(evs) == [Event(1, 10, "u", {"v": 2}, 6)]


def test_old_insert_cannot_resurrect_a_tombstone() -> None:
    state = apply_batch({}, [Event(1, 20, "d", None)])
    assert apply_batch(state, [Event(1, 10, "c", {"v": 1})])[1].is_deleted
