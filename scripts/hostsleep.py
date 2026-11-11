"""Host sleep guard for the G4 run of record (PLAN A2): a run is invalid if the Mac slept during it.

Two signals. The wall clock keeps advancing across sleep, but the uptime clock (CLOCK_UPTIME_RAW on Darwin) does not, so
their difference is the drift. `pmset -g log` records the Sleep, Wake and DarkWake events. Only Darwin is checked; on
other platforms `stop()` reports checked=false.
"""

from __future__ import annotations

import re
import subprocess
import sys
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

DRIFT_THRESHOLD_S = 2.0
PMSET_TIME_FMT = "%Y-%m-%d %H:%M:%S %z"
_PMSET_LINE = re.compile(r"\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [+-]\d{4})\s+(Sleep|Wake|DarkWake)\b")


def parse_pmset(text: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
    """Sleep, Wake and DarkWake lines of `pmset -g log` text whose timestamp lies in [start, end].

    `start` and `end` must be timezone-aware. Other lines, and lines with an impossible timestamp, are skipped.
    """
    events: list[dict[str, Any]] = []
    for line in text.splitlines():
        m = _PMSET_LINE.match(line)
        if m is None:
            continue
        try:
            at = datetime.strptime(m.group(1), PMSET_TIME_FMT)
        except ValueError:
            continue
        if start <= at <= end:
            events.append({"at": at.astimezone(timezone.utc).isoformat(), "kind": m.group(2), "line": line.strip()})
    return events


def read_pmset_log() -> tuple[str, str | None]:
    """Output of `pmset -g log`, or ("", reason) if it could not be run. Never raises."""
    try:
        proc = subprocess.run(["pmset", "-g", "log"], capture_output=True, text=True, check=False, timeout=60)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return "", f"{type(exc).__name__}: {exc}"
    if proc.returncode != 0:
        return "", f"pmset exited {proc.returncode}: {proc.stderr.strip()[:200]}"
    return proc.stdout, None


def _uptime_raw() -> float:
    return time.clock_gettime(getattr(time, "CLOCK_UPTIME_RAW"))  # noqa: B009 - attribute exists only on Darwin


class SleepGuard:
    """Brackets a run: `start()` before it, `stop()` after it. `stop()` returns a JSON-serialisable dict."""

    def __init__(
        self,
        *,
        platform: str | None = None,
        wall: Callable[[], float] = time.time,
        uptime: Callable[[], float] = _uptime_raw,
        pmset: Callable[[], tuple[str, str | None]] = read_pmset_log,
    ) -> None:
        self.platform = sys.platform if platform is None else platform
        self.checked = self.platform == "darwin"
        self._wall = wall
        self._uptime = uptime
        self._pmset = pmset
        self._start: tuple[float, float] | None = None

    def start(self) -> None:
        if self.checked:
            self._start = (self._wall(), self._uptime())

    def stop(self) -> dict[str, Any]:
        if not self.checked:
            return {
                "platform": self.platform,
                "checked": False,
                "start": None,
                "end": None,
                "drift_s": None,
                "events": [],
                "pmset_error": None,
                "sleep_detected": None,
            }
        if self._start is None:
            raise RuntimeError("SleepGuard.stop() called before start()")
        wall0, up0 = self._start
        wall1, up1 = self._wall(), self._uptime()
        drift = (wall1 - wall0) - (up1 - up0)
        start_dt = datetime.fromtimestamp(wall0, timezone.utc)
        end_dt = datetime.fromtimestamp(wall1, timezone.utc)
        text, error = self._pmset()
        events = parse_pmset(text, start_dt, end_dt) if error is None else []
        return {
            "platform": self.platform,
            "checked": True,
            "start": start_dt.isoformat(),
            "end": end_dt.isoformat(),
            "drift_s": round(drift, 3),
            "events": events,
            "pmset_error": error,
            "sleep_detected": abs(drift) > DRIFT_THRESHOLD_S or bool(events),
        }
