"""Build a pip constraints file that holds every package to the version policy (ADR-0000).

For each `name==version` in a pip freeze, pick the newest stable release uploaded on or before
CUTOFF that is <= the frozen version. Usage: python scripts/pin_policy.py FREEZE > constraints.txt
"""

from __future__ import annotations

import json
import re
import sys
import urllib.request

CUTOFF = "2026-10-11"
SKIP = {"torch"}  # pinned explicitly, from the CPU wheel index


def parse(v: str) -> tuple[int, ...] | None:
    if not re.fullmatch(r"\d+(\.\d+)*(\.post\d+)?", v):
        return None  # pre-releases, dev builds, local versions
    return tuple(int(x) for x in re.findall(r"\d+", v))


def pick(name: str, frozen: str) -> str:
    with urllib.request.urlopen(f"https://pypi.org/pypi/{name}/json", timeout=30) as r:
        releases = json.load(r)["releases"]
    ceiling = parse(frozen.split("+")[0])
    best: tuple[tuple[int, ...], str] | None = None
    for v, files in releases.items():
        pv = parse(v)
        if pv is None or not files or (ceiling and pv > ceiling):
            continue
        if min(f["upload_time"][:10] for f in files) <= CUTOFF and (best is None or pv > best[0]):
            best = (pv, v)
    return best[1] if best else frozen


def main() -> None:
    for line in open(sys.argv[1]).read().splitlines():
        m = re.fullmatch(r"([A-Za-z0-9_.\-]+)==(\S+)", line.strip())
        if not m or m.group(1).lower() in SKIP:
            continue
        name, frozen = m.groups()
        chosen = pick(name, frozen)
        note = "" if chosen == frozen.split("+")[0] else f"  # resolved {frozen}, newer than policy"
        print(f"{name}=={chosen}{note}")


if __name__ == "__main__":
    main()
