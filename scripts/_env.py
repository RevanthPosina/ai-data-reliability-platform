"""Tiny dotenv reader shared by host scripts (stdlib only)."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def read_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def all_env() -> dict[str, str]:
    # .env.example is the floor: a .env copied before a variable existed (FORGET_HMAC_KEY) still renders.
    return {**read_env(ROOT / "versions.env"), **read_env(ROOT / ".env.example"), **read_env(ROOT / ".env")}
