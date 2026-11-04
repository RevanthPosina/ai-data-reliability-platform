from pathlib import Path

import pytest

from adrp.forget import storage as st
from adrp.forget.rawscan import Subject

BIG = 2_000_000_000_123
CANARY = "g8c-" + "ef" * 16
SUBJ = Subject({"customers": [BIG, 5]}, canaries=(CANARY,))


def _vol(i: int, size: int, coll: str, files: int, dels: int, delbytes: int) -> str:
    return (
        f"          volume Id:{i}, Size:{size}, ReplicaPlacement:000, Collection:{coll}, Version:3, Ttl:, "
        f"FileCount:{files}, DeleteCount:{dels}, DeletedByteCount:{delbytes}, ReadOnly:false, ModifiedAtSecond:1"
    )


VOLUME_LIST = "\n".join(
    [
        "Topology volumeSizeLimit:256 MB hdd(volume:14/129 active:14 free:115 remote:0)",
        "        Disk hdd(volume:14/129 active:14 free:115 remote:0) id:0",
        _vol(1, 3386800, "lake", 327, 4, 11868),
        _vol(2, 2620704, "lake", 328, 5, 5385),
        _vol(8, 834216, "", 6, 0, 0),
        "        Disk hdd {Size:24874208 FileCount:2323 DeletedFileCount:41 DeletedBytes:72141}",
        "total size:24874208 file_count:2323 deleted_file:41 deleted_bytes:72141",
    ]
)


def test_parse_volume_list() -> None:
    vs = st.parse_volume_list(VOLUME_LIST)
    assert [(v.id, v.collection, v.size, v.delete_count, v.deleted_bytes) for v in vs] == [
        (1, "lake", 3386800, 4, 11868),
        (2, "lake", 2620704, 5, 5385),
        (8, "", 834216, 0, 0),
    ]


def test_garbage_ratio_is_per_collection() -> None:
    vs = st.parse_volume_list(VOLUME_LIST)
    g = st.garbage_of(vs, "lake")
    assert g.volumes == 2 and g.size == 3386800 + 2620704 and g.deleted_bytes == 11868 + 5385 and g.deleted_files == 9
    assert g.ratio == pytest.approx((11868 + 5385) / (3386800 + 2620704))
    none = st.garbage_of(vs, "absent")
    assert none.volumes == 0 and none.ratio == 0.0


def test_vacuum_script_locks_scopes_to_the_collection_and_unlocks() -> None:
    assert st.vacuum_script("lake", 0.0001) == "lock; volume.vacuum -garbageThreshold=0.0001 -collection lake; unlock"
    with pytest.raises(ValueError):
        st.vacuum_script("lake; volume.delete", 0.1)


def test_hit_counts_ignore_small_ids_and_never_return_text() -> None:
    text = f"user {BIG} wrote {CANARY} twice {CANARY}; also 5 and 12345678901234"
    assert st.hit_counts(text, SUBJ) == {"canary": 2, "id": 1}
    assert st.hit_counts(text.encode(), SUBJ) == {"canary": 2, "id": 1}
    assert st.hit_counts("nothing here", SUBJ) == {"canary": 0, "id": 0}


def test_scan_tree_counts_files_and_hits(tmp_path: Path) -> None:
    (tmp_path / "a.json").write_text(f'{{"x": "{CANARY}"}}')
    (tmp_path / "b.json").write_text('{"x": "clean"}')
    (tmp_path / "c.txt").write_text(f"{BIG} {BIG}")
    r = st.scan_tree(tmp_path, SUBJ)
    assert r == {"files": 3, "files_with_hits": 2, "hits": 3, "skipped": 0}
    assert st.manifests_scan(SUBJ, tmp_path) == {"files": 2, "files_with_hits": 1, "hits": 1, "skipped": 0}


def test_scan_tree_missing_directory_is_empty(tmp_path: Path) -> None:
    assert st.scan_tree(tmp_path / "nope", SUBJ)["files"] == 0
