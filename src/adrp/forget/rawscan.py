"""Host-side helpers shared by the G8 physical-removal modules: docker compose exec, and raw-byte grep counts.

Nothing here returns or logs the bytes it searches for. A needle (canary token, subject id, key bytes) travels to
the container on STDIN (`grep -f -`), never on a command line, so it is not visible in `ps`, in the docker daemon's
exec records, or in a CalledProcessError message. Results are counts of matching FILES (plus file paths, which
name storage locations such as `base/16384/2619` and never contain subject data).

grep cannot match a needle that contains a newline byte (patterns are line separated). A needle with one is cut
at the newline and its longest piece is searched instead; that over-approximates (it can only add hits) and is
reported in `GrepCount.approximated`. Pieces shorter than MIN_PIECE bytes would match random data, so such a
needle is dropped and counted in `GrepCount.dropped` instead of being searched. The same holds for NUL bytes in
a needle on the busybox-grep services (postgres, seaweedfs), cut at the first NUL.
"""

from __future__ import annotations

import re
import struct
import subprocess
import time
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from adrp.forget import ids as fids

ROOT = Path(__file__).resolve().parents[3]
COMPOSE = ["docker", "compose", "--env-file", str(ROOT / ".run/compose.env")]
MIN_PIECE = 4  # bytes; shorter needle pieces would hit unrelated data
MAX_LOCATIONS = 20
RETAINED_TABLES = ("accounts", "transactions")  # ledger rows: re-keyed to the placeholder customer, never deleted
BUSYBOX_GREP = frozenset({"postgres", "seaweedfs"})  # services whose grep is busybox (no NUL in patterns)
_SAFE_PATH = re.compile(r"[A-Za-z0-9_./*-]+")


# ---- docker -----------------------------------------------------------------------------------------


@dataclass
class Proc:
    rc: int
    out: bytes
    err: bytes

    @property
    def text(self) -> str:
        return self.out.decode(errors="replace")

    @property
    def err_text(self) -> str:
        return self.err.decode(errors="replace")


def run(argv: Sequence[str], stdin: bytes | None = None, timeout: float = 600) -> Proc:
    """Run a host command from the repo root. Errors are returned as rc, never raised, and never echo stdin."""
    try:
        r = subprocess.run(list(argv), input=stdin, capture_output=True, check=False, timeout=timeout, cwd=ROOT)
    except subprocess.TimeoutExpired:
        return Proc(124, b"", b"timed out")
    return Proc(r.returncode, r.stdout, r.stderr)


def compose_exec(service: str, argv: Sequence[str], stdin: bytes | None = None, timeout: float = 600) -> Proc:
    """`docker compose exec -T <service> <argv...>`. Do not put secrets in argv; use stdin."""
    return run([*COMPOSE, "exec", "-T", service, *argv], stdin=stdin, timeout=timeout)


def service_running(service: str) -> bool:
    r = run([*COMPOSE, "ps", "--status", "running", "-q", service], timeout=60)
    return r.rc == 0 and bool(r.out.strip())


def wait_until(fn: Callable[[], bool], timeout_s: float, every_s: float = 5.0) -> tuple[bool, float]:
    """Poll fn until it returns True. Returns (reached, elapsed seconds). Always evaluates fn at least once."""
    t0 = time.monotonic()
    while True:
        if fn():
            return True, time.monotonic() - t0
        if time.monotonic() - t0 >= timeout_s:
            return False, time.monotonic() - t0
        time.sleep(every_s)


# ---- needles ----------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Needles:
    """What to look for in raw bytes. canaries: exact tokens. ids: subject / entity ids.

    text()    canary tokens and, for ids >= 10**12 only, their decimal text (a short id would match anything).
    pg_int8() 8-byte little-endian int8 of each id (how Postgres stores a bigint in heap and index pages) with the
      trailing zero bytes removed. busybox grep (the postgres and seaweedfs images) cuts a pattern at its first
      NUL byte, so a pattern ending in NULs would not behave as written; an id >= 10**12 keeps 5-6 significant
      low bytes, which no other int8 in this database shares, so the stripped form is as specific as the whole.
    """

    canaries: tuple[str, ...] = ()
    ids: tuple[int, ...] = ()

    def text(self) -> list[bytes]:
        out = [c.encode() for c in self.canaries]
        out += [str(i).encode() for i in self.ids if i >= 10**12]
        return out

    def pg_int8(self) -> list[bytes]:
        return [struct.pack("<q", i).rstrip(b"\0") for i in self.ids if i >= 10**12]

    def __bool__(self) -> bool:
        return bool(self.canaries or self.ids)


@dataclass(frozen=True)
class Subject:
    """What one forget request is looking for, in the vocabulary of the sinks.

    entities: table name -> pks the subject owned (`customers`, `support_tickets`, `accounts`, `transactions`),
      i.e. the footprint. canaries: planted tokens (g8-check only; empty for real requests, which are found by
      id). Holds raw ids: keep it in process memory, never print it or put it in a manifest."""

    entities: Mapping[str, Collection[int]]
    canaries: tuple[str, ...] = ()

    @property
    def customer_ids(self) -> tuple[int, ...]:
        return tuple(sorted(self.entities.get("customers", ())))

    def removed_ids(self, table: str) -> tuple[int, ...]:
        """pks of `table` that the forget removes. accounts and transactions are RETAINED_TABLES: re-keyed to a
        placeholder, so their own pks stay in every store and are not evidence about the subject."""
        return () if table in RETAINED_TABLES else tuple(sorted(self.entities.get(table, ())))

    def entity_strings(self, tables: Iterable[str] | None = None) -> list[str]:
        """Pipeline entity ids (`support_tickets:123`), as the embedder and Qdrant payloads spell them."""
        names = list(self.entities) if tables is None else list(tables)
        return [f"{t}:{i}" for t in names for i in sorted(self.entities.get(t, ()))]

    def needles(self) -> Needles:
        """Canaries plus the CUSTOMER ids, for text-and-binary scans of stores that hold row data. Ticket,
        account and transaction pks are not searched as numbers: every table's identity column starts at 10**12,
        so a subject's ticket id is also another table's row id (a false hit in every store), and the ledger pks
        stay by design. Tickets are searched by their pipeline entity string and by canary/PII text instead."""
        return Needles(self.canaries, self.customer_ids)


def kafka_key_patterns(
    entity_ids: Iterable[int], schema_ids: Iterable[int], bare_ids: Iterable[int] = ()
) -> list[bytes]:
    """Confluent-framed message keys of `entity_ids` under each key schema id, plus the bare zigzag varint of
    every id in `entity_ids` + `bare_ids` that is >= 10**12 (a varint of a small id appears everywhere by chance;
    framed keys, with the magic byte and schema id in front, are specific at any size)."""
    ents = sorted(set(entity_ids))
    out = [fids.confluent_key_bytes(s, i) for s in sorted(set(schema_ids)) for i in ents]
    out += [fids.zigzag_varint(i) for i in sorted({*ents, *bare_ids}) if i >= 10**12]
    return sorted(set(out))


def pattern_blob(patterns: Iterable[bytes], cut_at_nul: bool = False) -> tuple[bytes, int, int]:
    """(stdin for `grep -F -f -`, approximated, dropped). See the module docstring for the newline rule.

    cut_at_nul: the target grep is busybox, which truncates a pattern at the first NUL byte; do it explicitly so
    the result is counted as approximated (a prefix match, never a miss) and a too-short prefix is dropped."""
    lines: list[bytes] = []
    approximated = dropped = 0
    for p in sorted(set(patterns)):
        if cut_at_nul and b"\0" in p:
            p = p.split(b"\0", 1)[0]
            if len(p) < MIN_PIECE:
                dropped += 1
                continue
            approximated += 1
        if not p:
            continue
        if b"\n" in p:
            piece = max(p.split(b"\n"), key=len)
            if len(piece) < MIN_PIECE:
                dropped += 1
                continue
            approximated += 1
            p = piece
        lines.append(p)
    return b"".join(x + b"\n" for x in sorted(set(lines))), approximated, dropped


# ---- grep -------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class GrepCount:
    """Result of one raw grep: the set of matching files (a union across needle groups), never matched text."""

    paths: frozenset[str] = frozenset()
    approximated: int = 0  # needles searched by a piece only (newline or NUL byte inside)
    dropped: int = 0  # needles that could not be searched
    errors: int = 0  # grep returned rc >= 2 (unreadable/vanished file or the exec failed)

    @property
    def files(self) -> int:
        return len(self.paths)

    @property
    def locations(self) -> tuple[str, ...]:
        """Up to MAX_LOCATIONS matching file paths, sorted. Storage locations, never subject data."""
        return tuple(sorted(self.paths))[:MAX_LOCATIONS]

    def __bool__(self) -> bool:
        return bool(self.paths)

    def merge(self, other: GrepCount) -> GrepCount:
        return GrepCount(
            self.paths | other.paths,
            self.approximated + other.approximated,
            self.dropped + other.dropped,
            self.errors + other.errors,
        )


PERL_SERVICES = frozenset({"redpanda", "qdrant"})  # images with perl: exact search for needles containing a newline

# Reads patterns (one hex string per line) from the file named by the first argument, then reports every further
# argument (a file) that contains one. Chunked with overlap, so a 256 MB segment is never held in memory.
_PERL_SCAN = (
    'my $pf = shift; open(P, "<:raw", $pf) or die; my @p = map { chomp; pack("H*", $_) } <P>; close P; '
    "my $k = 0; for my $x (@p) { $k = length($x) if length($x) > $k } $k--; "
    'for my $fn (@ARGV) { open(F, "<:raw", $fn) or next; my $buf = ""; my $hit = 0; '
    "while (!$hit) { my $n = read(F, my $c, 8388608); last unless $n; $buf .= $c; "
    "for my $x (@p) { if (index($buf, $x) >= 0) { $hit = 1; last } } "
    '$buf = substr($buf, -$k) if $k > 0 && length($buf) > $k } close F; print "$fn\\n" if $hit }'
)


def grep_files(service: str, paths: Sequence[str], patterns: Iterable[bytes], timeout: float = 900) -> GrepCount:
    """Files under `paths` inside `service` that contain any pattern; the patterns go in on stdin.

    Normally `grep -rlaF -f -`. A pattern containing a newline cannot be a grep pattern; on the images with perl
    (redpanda, qdrant) those are searched exactly with a small perl scan, elsewhere they are approximated (module
    docstring). `paths` run through `sh -c`, so a glob such as /var/lib/redpanda/data/kafka/cdc.public.* is
    expanded in the container. Paths must be literal/glob text from the caller (see safe_path), not subject data.
    With no patterns the count is 0 (grep with an empty pattern file would match every file)."""
    pats = sorted({p for p in patterns if p})
    plain = [p for p in pats if b"\n" not in p]
    newline = [p for p in pats if b"\n" in p]
    spec = " ".join(safe_path(p) for p in paths)
    if newline and service in PERL_SERVICES:
        result = _perl_files(service, spec, newline, timeout)
    else:
        plain, result = plain + newline, GrepCount()
    blob, approx, dropped = pattern_blob(plain, cut_at_nul=service in BUSYBOX_GREP)
    if blob:
        r = compose_exec(service, ["sh", "-c", "exec grep -rlaF -f - " + spec], stdin=blob, timeout=timeout)
        result = result.merge(parse_grep_result(r.rc, r.text, approx, dropped))
    else:
        result = result.merge(GrepCount(approximated=approx, dropped=dropped))
    return result


def _perl_files(service: str, spec: str, patterns: Sequence[bytes], timeout: float) -> GrepCount:
    """Exact multi-pattern file search with perl. The pattern file is written inside the container under /tmp
    (mktemp, removed on exit; never a scanned directory), fed from stdin."""
    blob = b"".join(p.hex().encode() + b"\n" for p in patterns)
    script = (
        'f=$(mktemp) && trap \'rm -f "$f"\' EXIT && cat > "$f" && '
        f"find {spec} -type f -exec perl -e '{_PERL_SCAN}' \"$f\" {{}} +"
    )
    r = compose_exec(service, ["sh", "-c", script], stdin=blob, timeout=timeout)
    return parse_grep_result(0 if r.rc == 0 else r.rc, r.text)


def safe_path(p: str) -> str:
    """A path (or glob) that is safe to splice into `sh -c`: letters, digits and `_ . / * -` only."""
    if not _SAFE_PATH.fullmatch(p):
        raise ValueError(f"unsafe path for sh: {p!r}")
    return p


def parse_grep_result(rc: int, stdout: str, approximated: int = 0, dropped: int = 0) -> GrepCount:
    """rc 0 or 1 is a clean run; 2 and up means grep hit an error (a segment deleted mid-scan, bad exec)."""
    return GrepCount(
        frozenset(ln for ln in stdout.splitlines() if ln.strip()), approximated, dropped, 0 if rc in (0, 1) else 1
    )


def grep_counts_by_path(service: str, paths: Sequence[str], patterns: Iterable[bytes]) -> dict[str, int]:
    """One grep per path (a directory or a glob); {path: matching file count}. A path that errors counts as -1."""
    plist = list(patterns)
    out: dict[str, int] = {}
    for p in paths:
        g = grep_files(service, [p], plist)
        out[p] = -1 if g.errors and not g.files else g.files
    return out
