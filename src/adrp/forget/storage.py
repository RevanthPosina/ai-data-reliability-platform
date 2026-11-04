"""Object-store, checkpoint and log sides of the forget workflow (G8). Counts and booleans only.

SeaweedFS keeps an object as a "needle" in a volume `.dat` file. Deleting the object (Iceberg expire / orphan
removal does) only marks the needle deleted; the bytes stay until the volume is vacuumed. garbage_ratio() reads
the deleted-bytes share per collection and vacuum() compacts it. Content that compresses well is stored
gzip'd, so a raw grep of /data can miss it: a clean grep there is weaker evidence than for the other sinks, and
parquet/avro are verified by decoding, not by this module.

The log and metrics functions answer one question: does the subject appear (canary text, or an id >= 10**12 as
decimal text) in anything the platform wrote while running? Container logs cannot be edited in place, only
recreated with their container, so a hit there is reported as a count and the caller decides.
"""

from __future__ import annotations

import json
import re
import time
import urllib.request
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from adrp.forget import ids as fids
from adrp.forget.rawscan import (
    BUSYBOX_GREP,
    COMPOSE,
    ROOT,
    GrepCount,
    Subject,
    compose_exec,
    grep_files,
    parse_grep_result,
    pattern_blob,
    run,
    safe_path,
    service_running,
    wait_until,
)

SEAWEED_MASTER = "localhost:9333"
LAKE_COLLECTION = "lake"
SEAWEED_DATA = "/data"
CHECKPOINT_DIR = "/checkpoints"
DAGSTER_HOME = "/opt/dagster_home"
EXPORTER_METRICS = "http://127.0.0.1:9108/metrics"
MANIFESTS = ROOT / "runs/manifests"
MAX_FILE_BYTES = 256 * 1024 * 1024

SPILL_BOUND = (
    "spark spill: shuffle/sort spill files and blockmgr directories live in the Spark container's /tmp until the "
    "JVM exits and the container is recreated; this system does not scrub them. Bounded, not removed."
)
CONTAINER_LOG_BOUND = (
    "container logs: docker keeps each container's stdout/stderr in its json log file until the container is "
    "recreated; they cannot be edited in place. Bounded, not removed."
)


# ---- SeaweedFS -------------------------------------------------------------------------------------------

_VOLUME = re.compile(
    r"volume Id:(?P<id>\d+), Size:(?P<size>\d+), .*?Collection:(?P<coll>[^,]*), .*?FileCount:(?P<files>\d+), "
    r"DeleteCount:(?P<dels>\d+), DeletedByteCount:(?P<delbytes>\d+)"
)


@dataclass(frozen=True)
class VolumeInfo:
    id: int
    collection: str
    size: int
    file_count: int
    delete_count: int
    deleted_bytes: int


def parse_volume_list(text: str) -> list[VolumeInfo]:
    """Parse `weed shell` `volume.list` output (one `volume Id:..., Size:..., Collection:..., ...` line each)."""
    out = []
    for line in text.splitlines():
        m = _VOLUME.search(line)
        if m:
            out.append(
                VolumeInfo(
                    int(m["id"]),
                    m["coll"].strip(),
                    int(m["size"]),
                    int(m["files"]),
                    int(m["dels"]),
                    int(m["delbytes"]),
                )
            )
    return out


@dataclass(frozen=True)
class Garbage:
    volumes: int
    size: int
    deleted_bytes: int
    deleted_files: int

    @property
    def ratio(self) -> float:
        return self.deleted_bytes / self.size if self.size else 0.0


def garbage_of(volumes: Iterable[VolumeInfo], collection: str = LAKE_COLLECTION) -> Garbage:
    """Garbage ratio (deleted bytes / size) summed over the volumes of one collection (= bucket name)."""
    vs = [v for v in volumes if v.collection == collection]
    return Garbage(len(vs), sum(v.size for v in vs), sum(v.deleted_bytes for v in vs), sum(v.delete_count for v in vs))


def weed_shell(commands: str, timeout: float = 600) -> tuple[int, str]:
    """Run `weed shell` commands (`;`-separated, as `echo ... | weed shell`) in the seaweedfs container."""
    r = compose_exec(
        "seaweedfs", ["weed", "shell", f"-master={SEAWEED_MASTER}"], stdin=(commands + "\n").encode(), timeout=timeout
    )
    return r.rc, r.text + r.err_text


def garbage_ratio(collection: str = LAKE_COLLECTION) -> Garbage:
    """Current garbage in a collection. `volume.list` lags the volume servers by a few seconds."""
    rc, out = weed_shell("volume.list")
    if rc != 0:
        raise RuntimeError(f"weed shell volume.list failed (rc {rc})")
    return garbage_of(parse_volume_list(out), collection)


def vacuum_script(collection: str, threshold: float) -> str:
    """The shell script of the vacuum. `lock` is mandatory; -collection limits the rewrite to one bucket."""
    if not re.fullmatch(r"[A-Za-z0-9_-]+", collection):
        raise ValueError(f"bad collection: {collection!r}")
    return f"lock; volume.vacuum -garbageThreshold={threshold} -collection {collection}; unlock"


@dataclass(frozen=True)
class VacuumResult:
    ok: bool  # the shell ran and the garbage ratio fell to <= settle_ratio
    before: Garbage
    after: Garbage
    elapsed_s: float


def vacuum(
    collection: str = LAKE_COLLECTION, threshold: float = 0.0001, settle_ratio: float = 0.001, timeout_s: float = 120
) -> VacuumResult:
    """Compact the collection's volumes (rewrites `.dat` files without the deleted needles), then poll
    volume.list until the garbage ratio is <= settle_ratio. Rewrites every live volume of the collection: run in
    the maintenance window with writers stopped (writes to a volume being compacted are paused or retried)."""
    t0 = time.monotonic()
    before = garbage_ratio(collection)
    rc, _ = weed_shell(vacuum_script(collection, threshold))
    after = [before]

    def settled() -> bool:
        after[0] = garbage_ratio(collection)
        return after[0].ratio <= settle_ratio

    ok, _ = wait_until(settled, timeout_s, 3.0)
    return VacuumResult(rc == 0 and ok, before, after[0], time.monotonic() - t0)


def seaweed_raw_grep(subject: Subject) -> GrepCount:
    """grep -rlaF over the volume `.dat` files in /data for the subject's canaries and ids as text. Weak evidence
    (see the module docstring): compressible needles are stored gzip'd and are invisible to it."""
    return grep_files("seaweedfs", [SEAWEED_DATA], subject.needles().text())


# ---- greps in containers that may be stopped ----------------------------------------------------------------


def grep_service_files(service: str, paths: Sequence[str], patterns: Iterable[bytes]) -> GrepCount:
    """grep -rlaF in `service`. If the container is stopped (spark-streams during the maintenance window), run the
    same grep in a throwaway container of the same image that mounts the stopped container's volumes read-only."""
    pats = list(patterns)
    if service_running(service):
        return grep_files(service, paths, pats)
    cid = run([*COMPOSE, "ps", "-a", "-q", service], timeout=60).text.strip()
    if not cid:
        return GrepCount(errors=1)
    image = run(["docker", "inspect", "--format", "{{.Image}}", cid], timeout=60).text.strip()
    blob, approx, dropped = pattern_blob(pats, cut_at_nul=service in BUSYBOX_GREP)
    if not blob:
        return GrepCount(approximated=approx, dropped=dropped)
    script = "exec grep -rlaF -f - " + " ".join(safe_path(p) for p in paths)
    r = run(
        ["docker", "run", "--rm", "-i", "--volumes-from", f"{cid}:ro", "--entrypoint", "sh", image, "-c", script],
        stdin=blob,
    )
    return parse_grep_result(r.rc, r.text, approx, dropped)


def spark_checkpoint_grep(subject: Subject, service: str = "spark-streams") -> GrepCount:
    """Files in the Spark checkpoints volume (/checkpoints: offsets, commits, state) containing the subject.
    Kafka offsets and Iceberg progress carry no row data, so zero is expected; a hit is a finding."""
    return grep_service_files(service, [CHECKPOINT_DIR], subject.needles().text())


def spark_spill_grep(subject: Subject, service: str = "spark-streams") -> GrepCount:
    """Files under the Spark container's /tmp (blockmgr, spill) containing the subject. Reported for the record;
    the matching bound is SPILL_BOUND."""
    return grep_service_files(service, ["/tmp"], subject.needles().text())


def dagster_storage_grep(subject: Subject, service: str = "dagster") -> GrepCount:
    """Files under DAGSTER_HOME (run storage, event logs, compute logs) containing the subject."""
    return grep_service_files(service, [DAGSTER_HOME], subject.needles().text())


# ---- text scans: logs, metrics, manifests ----------------------------------------------------------------


def hit_counts(data: bytes | str, subject: Subject) -> dict[str, int]:
    """Counts of canary tokens and of subject ids (only ids >= 10**12: a short id would match unrelated numbers)
    in text. Never the matched text."""
    big = [i for i in subject.customer_ids if i >= 10**12]
    h = fids.Matcher(subject_ids=big, canaries=subject.canaries).hits(data)
    return {"canary": h.canary, "id": h.subject_id}


def services() -> list[str]:
    """Compose services that have a container (running or not)."""
    r = run([*COMPOSE, "ps", "-a", "--format", "json"], timeout=60)
    text = r.text.strip()
    if not text:
        return []
    try:
        rows = json.loads(text)
        rows = rows if isinstance(rows, list) else [rows]
    except ValueError:  # one JSON object per line on older compose versions
        rows = [json.loads(ln) for ln in text.splitlines() if ln.strip()]
    return sorted({str(r["Service"]) for r in rows})


def container_log(service: str) -> bytes:
    """stdout and stderr of the service's container since it started."""
    r = run([*COMPOSE, "logs", "--no-color", "--no-log-prefix", service], timeout=300)
    return r.out + r.err


def container_logs_scan(subject: Subject, only: Sequence[str] | None = None) -> dict[str, int]:
    """{service: hits} over `docker compose logs` of every service (or `only`). Hits = canaries + ids."""
    out: dict[str, int] = {}
    for svc in only or services():
        c = hit_counts(container_log(svc), subject)
        out[svc] = c["canary"] + c["id"]
    return out


def metrics_scan(subject: Subject, url: str = EXPORTER_METRICS) -> int:
    """Hits in the exporter's /metrics page (-1 if it cannot be fetched)."""
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            c = hit_counts(r.read(), subject)
    except OSError:
        return -1
    return c["canary"] + c["id"]


def scan_tree(root: Path, subject: Subject, pattern: str = "**/*") -> dict[str, int]:
    """Host-side scan of files under `root` (default every file): {'files': scanned, 'files_with_hits': n,
    'hits': total}. Files over MAX_FILE_BYTES are skipped and counted in 'skipped'. For runs/manifests/*.json,
    runs/data/eventlog/openlineage and similar plain-text outputs; parquet is compressed, use files.py."""
    scanned = with_hits = hits = skipped = 0
    for p in sorted(root.glob(pattern)):
        if not p.is_file():
            continue
        if p.stat().st_size > MAX_FILE_BYTES:
            skipped += 1
            continue
        scanned += 1
        c = hit_counts(p.read_bytes(), subject)
        n = c["canary"] + c["id"]
        hits += n
        with_hits += n > 0
    return {"files": scanned, "files_with_hits": with_hits, "hits": hits, "skipped": skipped}


def manifests_scan(subject: Subject, directory: Path = MANIFESTS) -> dict[str, int]:
    """runs/manifests/*.json: no run manifest may carry a canary or a subject id."""
    return scan_tree(directory, subject, "*.json")


def logs_report(subject: Subject, eventlog: Path | None = None) -> Mapping[str, int]:
    """Everything above that is a text/log source, as {source: hits}: one key per compose service
    (`log:<service>`), `metrics`, `dagster_storage`, `manifests`, and `eventlog` when a directory is given
    (OpenLineage events, maintenance run records). A single call for the workflow's final sweep."""
    rep: dict[str, int] = {f"log:{k}": v for k, v in container_logs_scan(subject).items()}
    rep["metrics"] = metrics_scan(subject)
    rep["dagster_storage"] = dagster_storage_grep(subject).files
    rep["manifests"] = manifests_scan(subject)["hits"]
    if eventlog is not None:
        rep["eventlog"] = scan_tree(eventlog, subject)["hits"]
    return rep
