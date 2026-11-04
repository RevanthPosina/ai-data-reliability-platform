"""Pure identifiers for the forget workflow (G8): HMAC subject ids, pseudonyms, key byte patterns, PII matcher.

No I/O. The HMAC key comes in as an argument (callers read FORGET_HMAC_KEY from the environment), so the
raw customer id never has to be stored: the audit tables and the logs carry the HMAC or a pseudonym only.
"""

from __future__ import annotations

import hmac
import random
import re
import struct
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from hashlib import sha256

CANARY_PREFIX = "g8c-"
ADHOC_ID_MIN = 2 * 10**12  # ad-hoc subjects live above the identity default range (1e12 + a few thousand rows)
ADHOC_ID_MAX = 10**13 - 1  # 13 digits, so ID13 finds them in text
ID13 = re.compile(r"(?<!\d)\d{13}(?!\d)")
INT63_MAX = 2**63 - 1


def _digest(key: str, domain: str, value: str) -> bytes:
    if not key:
        raise ValueError("FORGET_HMAC_KEY is empty")
    return hmac.new(key.encode(), f"{domain}|{value}".encode(), sha256).digest()


def subject_hmac(key: str, customer_id: int | str) -> str:
    """Stable, non-reversible handle for a data subject (hex SHA-256 HMAC). This is what the audit tables store."""
    return _digest(key, "subject", str(customer_id)).hex()


def pseudonym(key: str, entity: str) -> str:
    """Replacement for an entity id or pk string in logs: `p:` + 16 hex chars of HMAC(entity)."""
    return "p:" + _digest(key, "pseudonym", entity).hex()[:16]


def pseudonym_int(key: str, entity: str) -> int:
    """Replacement for an int64 pk: a negative int63 (real pks are positive, so it never collides with one)."""
    n = int.from_bytes(_digest(key, "pseudonym-int", entity)[:8], "big")
    return -(1 + n % INT63_MAX)  # in [-(2**63 - 1), -1]


# ---- Avro key bytes ---------------------------------------------------------------------------------


def zigzag_varint(n: int) -> bytes:
    """Avro `long` encoding: zigzag, then 7 bits per byte, least significant group first."""
    z = ((n << 1) ^ (n >> 63)) & 0xFFFFFFFFFFFFFFFF
    out = bytearray()
    while z > 0x7F:
        out.append((z & 0x7F) | 0x80)
        z >>= 7
    out.append(z)
    return bytes(out)


def read_zigzag_varint(data: bytes, pos: int = 0) -> tuple[int, int]:
    """Inverse of zigzag_varint: (value, position after it)."""
    z = shift = 0
    while True:
        b = data[pos]
        pos += 1
        z |= (b & 0x7F) << shift
        if not b & 0x80:
            return (z >> 1) ^ -(z & 1), pos
        shift += 7


def confluent_key_bytes(schema_id: int, row_id: int) -> bytes:
    """Message key of a Debezium topic for key schema `{id: long}`: 0x00, 4-byte big-endian schema id, varint."""
    return b"\x00" + struct.pack(">I", schema_id) + zigzag_varint(row_id)


# ---- ad-hoc subjects and canaries --------------------------------------------------------------------


def new_canary(rng: random.Random | None = None) -> str:
    """`g8c-` + 32 hex chars. Seed `rng` for a reproducible token; the default is a random uuid4."""
    u = uuid.uuid4() if rng is None else uuid.UUID(int=rng.getrandbits(128), version=4)
    return CANARY_PREFIX + u.hex


def adhoc_subject_ids(n: int, rng: random.Random | None = None, taken: Iterable[int] = ()) -> list[int]:
    """n distinct 13-digit ids >= 10**12, none in `taken` (ids the generator or earlier subjects already use)."""
    rng = rng or random.Random()
    seen = set(taken)
    out: list[int] = []
    while len(out) < n:
        i = rng.randint(ADHOC_ID_MIN, ADHOC_ID_MAX)
        if i not in seen:
            seen.add(i)
            out.append(i)
    return out


# ---- matcher ----------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Hits:
    """Match counts only. Never the matched text: results end up in manifests and check details.

    key_bytes counts every key pattern separately, so a framed key and its bare varint tail count twice."""

    canary: int = 0
    subject_id: int = 0
    key_bytes: int = 0

    @property
    def total(self) -> int:
        return self.canary + self.subject_id + self.key_bytes

    def __bool__(self) -> bool:
        return self.total > 0


class Matcher:
    """Counts occurrences of subject PII in bytes or text.

    canaries: the exact tokens to look for; None matches any `g8c-` token.
    subject_ids: decimal text with digit boundaries on both sides, so a 14-digit number containing the id does
      not match. Meaningful for ids >= 10**12; a small id (a generator customer) would match unrelated numbers.
    schema_ids: for bytes input only, the Confluent framed key (schema id + varint) of each subject id; the
      bare varint is added for ids >= 10**12 only (a short varint of a small id appears everywhere by chance).
    """

    def __init__(
        self,
        subject_ids: Iterable[int] = (),
        canaries: Iterable[str] | None = None,
        schema_ids: Iterable[int] = (),
    ) -> None:
        ids = sorted({int(i) for i in subject_ids})
        canary_pat = (
            re.escape(CANARY_PREFIX) if canaries is None else "|".join(re.escape(c) for c in sorted(set(canaries)))
        )
        self._canary = re.compile(canary_pat) if canary_pat else None
        self._id_text = re.compile("(?<!\\d)(?:" + "|".join(str(i) for i in ids) + ")(?!\\d)") if ids else None
        keys = [confluent_key_bytes(s, i) for s in schema_ids for i in ids]
        keys += [zigzag_varint(i) for i in ids if i >= 10**12]
        self._keys = sorted(set(keys))
        self._canary_b = re.compile(self._canary.pattern.encode()) if self._canary else None
        self._id_b = re.compile(self._id_text.pattern.encode()) if self._id_text else None

    def hits(self, data: bytes | str) -> Hits:
        if isinstance(data, str):
            return Hits(
                canary=len(self._canary.findall(data)) if self._canary else 0,
                subject_id=len(self._id_text.findall(data)) if self._id_text else 0,
            )
        return Hits(
            canary=len(self._canary_b.findall(data)) if self._canary_b else 0,
            subject_id=len(self._id_b.findall(data)) if self._id_b else 0,
            key_bytes=sum(data.count(k) for k in self._keys),
        )
