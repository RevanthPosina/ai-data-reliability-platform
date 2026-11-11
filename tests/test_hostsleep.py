"""scripts/hostsleep.py: pmset parsing, drift and the non-Darwin path. No real pmset is run; clocks are injected."""

import json
import subprocess
import sys
from datetime import datetime
from typing import Any

import hostsleep as hs
import pytest

PMSET_SAMPLE = """\
Summary:
2026-11-10 08:30:00 -0800 Charge               \tUsing AC (Charge:100%)
2026-11-10 09:00:00 -0800 Sleep                \tEntering Sleep state due to 'Idle Sleep':TCPKeepAlive=active
2026-11-10 09:10:00 -0800 DarkWake             \tDarkWake from Normal Sleep [CDNVA] : due to RTC/Maintenance
2026-11-10 10:00:00 -0800 Wake                 \tWake from Normal Sleep [CDNVA] : due to UserActivity
2026-11-10 10:06:00 -0800 WakeRequests         \tNot a power event
   Sleep Service Back to Sleep
2026-13-40 10:07:00 -0800 Sleep                \tImpossible timestamp
2026-11-10 10:08:00 -0800 Display              \tDisplay is on
"""

START = datetime.strptime("2026-11-10 09:05:00 -0800", hs.PMSET_TIME_FMT)
END = datetime.strptime("2026-11-10 10:30:00 -0800", hs.PMSET_TIME_FMT)


class Clock:
    """A fake clock: call it for the time, or move `t` by hand."""

    def __init__(self, t: float) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


def _guard(
    platform: str = "darwin", pmset_text: str = "", pmset_error: str | None = None
) -> tuple[hs.SleepGuard, Clock, Clock]:
    wall, uptime = Clock(1_800_000_000.0), Clock(5000.0)
    guard = hs.SleepGuard(
        platform=platform,
        wall=wall,
        uptime=uptime,
        pmset=lambda: (pmset_text, pmset_error),
    )
    return guard, wall, uptime


# ---- parse_pmset ---------------------------------------------------------------------------------


def test_parse_keeps_only_sleep_wake_darkwake_lines_in_window() -> None:
    events = hs.parse_pmset(PMSET_SAMPLE, START, END)
    assert [e["kind"] for e in events] == ["DarkWake", "Wake"]


def test_parse_excludes_events_outside_window() -> None:
    # The 09:00 Sleep is before START; the 10:00 Wake is inside; a window ending at 09:30 keeps only the DarkWake.
    early_end = datetime.strptime("2026-11-10 09:30:00 -0800", hs.PMSET_TIME_FMT)
    assert [e["kind"] for e in hs.parse_pmset(PMSET_SAMPLE, START, early_end)] == ["DarkWake"]
    assert hs.parse_pmset(PMSET_SAMPLE, START, START) == []


def test_parse_reports_utc_timestamp_and_the_original_line() -> None:
    (dark, _wake) = hs.parse_pmset(PMSET_SAMPLE, START, END)
    assert dark["at"] == "2026-11-10T17:10:00+00:00"
    assert dark["line"].startswith("2026-11-10 09:10:00 -0800 DarkWake")


def test_parse_ignores_non_matching_text() -> None:
    assert hs.parse_pmset("", START, END) == []
    assert hs.parse_pmset("nothing here\nSleep without a timestamp\nWakeRequests 2026\n", START, END) == []


def test_parse_handles_positive_offsets() -> None:
    text = "2026-11-10 20:00:00 +0100 Sleep  \tEntering Sleep\n"
    start = datetime.strptime("2026-11-10 18:00:00 +0000", hs.PMSET_TIME_FMT)
    end = datetime.strptime("2026-11-10 20:00:00 +0000", hs.PMSET_TIME_FMT)
    (ev,) = hs.parse_pmset(text, start, end)
    assert ev["at"] == "2026-11-10T19:00:00+00:00"


# ---- drift and sleep_detected (injected clocks) --------------------------------------------------


def test_no_drift_and_no_events_means_no_sleep() -> None:
    guard, wall, uptime = _guard()
    guard.start()
    wall.t += 600
    uptime.t += 600
    result = guard.stop()
    assert result["checked"] is True
    assert result["drift_s"] == 0.0
    assert result["events"] == []
    assert result["sleep_detected"] is False


def test_wall_clock_running_ahead_of_uptime_is_sleep() -> None:
    guard, wall, uptime = _guard()
    guard.start()
    wall.t += 3600  # an hour passed on the wall clock...
    uptime.t += 60  # ...but only a minute of uptime, so the machine was asleep
    result = guard.stop()
    assert result["drift_s"] == 3540.0
    assert result["sleep_detected"] is True


def test_drift_threshold_is_two_seconds() -> None:
    guard, wall, uptime = _guard()
    guard.start()
    wall.t += 60 + 1.5
    uptime.t += 60
    assert guard.stop()["sleep_detected"] is False

    guard, wall, uptime = _guard()
    guard.start()
    wall.t += 60 + 2.5
    uptime.t += 60
    assert guard.stop()["sleep_detected"] is True


def test_negative_drift_also_counts() -> None:
    guard, wall, uptime = _guard()
    guard.start()
    wall.t += 60
    uptime.t += 3600  # wall clock stepped back or uptime ran ahead: the clocks disagree either way
    result = guard.stop()
    assert result["drift_s"] == -3540.0
    assert result["sleep_detected"] is True


def test_pmset_event_in_window_is_sleep_even_without_drift() -> None:
    text = "2026-11-10 09:00:00 +0000 Sleep  \tEntering Sleep\n"
    guard, wall, uptime = _guard(pmset_text=text)
    wall.t = datetime.strptime("2026-11-10 08:59:00 +0000", hs.PMSET_TIME_FMT).timestamp()
    guard.start()
    wall.t += 300
    uptime.t += 300
    result = guard.stop()
    assert result["drift_s"] == 0.0
    assert [e["kind"] for e in result["events"]] == ["Sleep"]
    assert result["sleep_detected"] is True


def test_stop_result_is_json_serialisable_and_has_the_keys() -> None:
    guard, wall, uptime = _guard(pmset_text=PMSET_SAMPLE)
    wall.t = START.timestamp()
    guard.start()
    wall.t = END.timestamp()
    uptime.t += wall.t - START.timestamp()
    result = guard.stop()
    assert {
        "platform",
        "checked",
        "start",
        "end",
        "drift_s",
        "events",
        "pmset_error",
        "sleep_detected",
    } <= result.keys()
    assert json.loads(json.dumps(result)) == result


def test_stop_before_start_raises() -> None:
    guard, _, _ = _guard()
    with pytest.raises(RuntimeError):
        guard.stop()


# ---- pmset failure is tolerated ------------------------------------------------------------------


def test_pmset_error_still_gives_a_result_from_drift() -> None:
    guard, wall, uptime = _guard(pmset_error="pmset exited 1: no log")
    guard.start()
    wall.t += 3600
    uptime.t += 1
    result = guard.stop()
    assert result["pmset_error"] == "pmset exited 1: no log"
    assert result["events"] == []
    assert result["sleep_detected"] is True


def test_read_pmset_log_runs_the_command(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[list[str]] = []

    def fake_run(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        seen.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout=PMSET_SAMPLE, stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    assert hs.read_pmset_log() == (PMSET_SAMPLE, None)
    assert seen == [["pmset", "-g", "log"]]


def test_read_pmset_log_tolerates_missing_binary(monkeypatch: pytest.MonkeyPatch) -> None:
    def missing(*_: Any, **__: Any) -> None:
        raise FileNotFoundError("pmset")

    monkeypatch.setattr(subprocess, "run", missing)
    text, error = hs.read_pmset_log()
    assert text == ""
    assert error is not None and "FileNotFoundError" in error


def test_read_pmset_log_tolerates_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    def slow(cmd: list[str], **_: Any) -> None:
        raise subprocess.TimeoutExpired(cmd, 60)

    monkeypatch.setattr(subprocess, "run", slow)
    text, error = hs.read_pmset_log()
    assert text == "" and error is not None and "TimeoutExpired" in error


def test_read_pmset_log_tolerates_nonzero_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    def failed(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="permission denied\n")

    monkeypatch.setattr(subprocess, "run", failed)
    text, error = hs.read_pmset_log()
    assert text == "" and error == "pmset exited 1: permission denied"


# ---- non-Darwin path -----------------------------------------------------------------------------


def _forbidden_pmset() -> tuple[str, str | None]:
    raise AssertionError("pmset must not run off Darwin")


def test_non_darwin_reports_unchecked_and_never_reads_clocks_or_pmset() -> None:
    def no_clock() -> float:
        raise AssertionError("clock must not be read off Darwin")

    guard = hs.SleepGuard(platform="linux", wall=no_clock, uptime=no_clock, pmset=_forbidden_pmset)
    guard.start()
    result = guard.stop()
    assert result == {
        "platform": "linux",
        "checked": False,
        "start": None,
        "end": None,
        "drift_s": None,
        "events": [],
        "pmset_error": None,
        "sleep_detected": None,
    }
    assert json.loads(json.dumps(result)) == result


@pytest.mark.skipif(sys.platform == "darwin", reason="the real-host check runs on Darwin")
def test_real_host_off_darwin_is_unchecked() -> None:
    result = hs.SleepGuard().stop()
    assert result["checked"] is False
    assert result["platform"] == sys.platform
