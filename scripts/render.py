"""Render .run/ files from versions.env + .env (Compose 2.15 accepts only one --env-file)."""

from __future__ import annotations

import shutil
import string

from _env import ROOT, all_env


def main() -> None:
    if not (ROOT / ".env").exists():
        shutil.copy(ROOT / ".env.example", ROOT / ".env")
    env = all_env()
    run = ROOT / ".run"
    run.mkdir(exist_ok=True)
    (run / "compose.env").write_text("".join(f"{k}={v}\n" for k, v in env.items()))
    tmpl = (ROOT / "infra/seaweedfs/s3.json.tmpl").read_text()
    (run / "s3.json").write_text(string.Template(tmpl).substitute(env))


if __name__ == "__main__":
    main()
