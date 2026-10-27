"""Idempotent one-time setup after the core infra is healthy.

1. SeaweedFS bucket  2. Lakekeeper bootstrap + warehouse  3. Schema Registry compatibility
4. Debezium connector  5. wait until the connector task is RUNNING
"""

from __future__ import annotations

import json
import string
import subprocess
import sys
import time
import urllib.error
import urllib.request
from typing import Any

from _env import ROOT, all_env

COMPOSE = ["docker", "compose", "--env-file", str(ROOT / ".run/compose.env")]


def http(method: str, url: str, body: Any = None, ok: tuple[int, ...] = (200, 201, 204)) -> tuple[int, Any]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        payload = e.read().decode(errors="replace")
        if e.code in ok:
            return e.code, payload
        raise RuntimeError(f"{method} {url} -> {e.code}: {payload}") from None


def wait_http(url: str, timeout: float = 180) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(url, timeout=5)
            return
        except Exception:  # noqa: BLE001
            time.sleep(2)
    sys.exit(f"timed out waiting for {url}")


def step(msg: str) -> None:
    print(f"[bootstrap] {msg}", flush=True)


def main() -> None:
    env = all_env()

    step("bucket 'lake' on SeaweedFS")
    out = subprocess.run(
        [*COMPOSE, "exec", "-T", "seaweedfs", "sh", "-c", "echo 's3.bucket.create -name lake' | weed shell"],
        capture_output=True,
        text=True,
        check=False,
    )
    if out.returncode != 0 and "already exist" not in (out.stdout + out.stderr):
        sys.exit(f"bucket create failed: {out.stdout}{out.stderr}")

    step("Lakekeeper bootstrap + warehouse 'lake'")
    wait_http("http://localhost:8181/health")
    http(
        "POST",
        "http://localhost:8181/management/v1/bootstrap",
        {"accept-terms-of-use": True},
        ok=(200, 201, 204, 400, 409),
    )
    _, listing = http("GET", "http://localhost:8181/management/v1/warehouse")
    if not any(w["name"] == "lake" for w in listing.get("warehouses", [])):
        http(
            "POST",
            "http://localhost:8181/management/v1/warehouse",
            {
                "warehouse-name": "lake",
                "storage-profile": {
                    "type": "s3",
                    "bucket": "lake",
                    "key-prefix": "warehouse",
                    "region": "us-east-1",
                    "endpoint": "http://seaweedfs:8333",
                    "path-style-access": True,
                    "flavor": "s3-compat",
                    "sts-enabled": False,
                    "remote-signing-enabled": False,
                },
                "storage-credential": {
                    "type": "s3",
                    "credential-type": "access-key",
                    "aws-access-key-id": env["S3_ACCESS_KEY"],
                    "aws-secret-access-key": env["S3_SECRET_KEY"],
                },
                # Hard delete: dropped tables leave no data behind (matters for G8).
                "delete-profile": {"type": "hard"},
            },
        )

    step("Schema Registry global compatibility BACKWARD")
    wait_http("http://localhost:18081/subjects")
    http("PUT", "http://localhost:18081/config", {"compatibility": "BACKWARD"})

    if "--skip-connector" in sys.argv:
        step("skipping connector (--skip-connector)")
    else:
        ensure_connector(env)
    step("done")


def ensure_connector(env: dict[str, str], timeout: float = 120) -> None:
    """Create or update the Debezium connector and wait until connector and task are RUNNING."""
    step("Debezium connector pg-shop")
    wait_http("http://localhost:8083/connectors")
    cfg = json.loads(string.Template((ROOT / "infra/connect/pg-shop.json").read_text()).substitute(env))
    http("PUT", "http://localhost:8083/connectors/pg-shop/config", cfg)

    deadline = time.monotonic() + timeout
    state = None
    while time.monotonic() < deadline:
        try:
            _, st = http("GET", "http://localhost:8083/connectors/pg-shop/status")
            tasks = st.get("tasks", [])
            state = (st["connector"]["state"], tasks[0]["state"] if tasks else None)
            if state == ("RUNNING", "RUNNING"):
                return
            if "FAILED" in state:
                sys.exit(f"connector failed: {json.dumps(st)[:2000]}")
        except RuntimeError:
            pass
        time.sleep(2)
    sys.exit(f"connector not RUNNING: {state}")


if __name__ == "__main__":
    main()
