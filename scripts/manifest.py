"""Run manifest: git SHA, pinned versions, seed, profile, hardware fingerprint (PLAN G0).

Every result that may ever appear in the README must point at one of these files.
"""

from __future__ import annotations

import json
import platform
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from _env import ROOT, read_env


def _run(*cmd: str) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=30).stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        return ""


def hardware() -> dict[str, Any]:
    hw: dict[str, Any] = {"machine": platform.machine(), "system": platform.system(), "release": platform.release()}
    if platform.system() == "Darwin":
        hw["cpu"] = _run("sysctl", "-n", "machdep.cpu.brand_string")
        hw["ncpu"] = _run("sysctl", "-n", "hw.ncpu")
        hw["mem_bytes"] = _run("sysctl", "-n", "hw.memsize")
    docker = _run("docker", "info", "--format", "{{json .}}")
    if docker:
        d = json.loads(docker)
        hw["docker"] = {k: d.get(k) for k in ("ServerVersion", "NCPU", "MemTotal", "OperatingSystem", "Architecture")}
    return hw


def image_digests() -> dict[str, str]:
    out = _run("docker", "compose", "--env-file", str(ROOT / ".run/compose.env"), "images", "--format", "json")
    try:
        rows = json.loads(out) if out else []
    except json.JSONDecodeError:
        rows = []
    return {r.get("ContainerName", "?"): f"{r.get('Repository')}:{r.get('Tag')}@{r.get('ID')}" for r in rows}


def _dirty_paths() -> list[str]:
    out = _run("git", "-C", str(ROOT), "status", "--porcelain", "--", ".", ":(exclude)runs/manifests")
    return [line[3:] for line in out.splitlines() if line.strip()]


def write_manifest(kind: str, profile: str, seed: int | None, result: dict[str, Any]) -> Path:
    now = datetime.now(UTC)
    dirty = _dirty_paths()
    manifest = {
        "kind": kind,
        "started_at": result.pop("started_at", None),
        "written_at": now.isoformat(),
        "git_sha": _run("git", "-C", str(ROOT), "rev-parse", "--verify", "-q", "HEAD") or None,
        # Other run manifests do not make the code dirty; anything else does, and is listed.
        "git_dirty": bool(dirty),
        "git_dirty_paths": dirty,
        "versions": read_env(ROOT / "versions.env"),
        "seed": seed,
        "profile": profile,
        "hardware": hardware(),
        "images": image_digests(),
        "result": result,
    }
    path = ROOT / "runs/manifests" / f"{now:%Y%m%dT%H%M%SZ}-{kind}.json"
    path.write_text(json.dumps(manifest, indent=2, default=str) + "\n")
    return path
