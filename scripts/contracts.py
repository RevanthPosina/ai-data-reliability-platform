"""Data-contract tooling (PLAN G2).

  contracts.py export                         write contracts/*.yaml + contracts/baseline/*.avsc from the live registry
  contracts.py check [--register-baseline]    judge every contract against the registry; exit 1 on incompatibility

`check` asks the registry itself (POST /compatibility/subjects/<subject>/versions/latest). With
--register-baseline it first registers the committed baseline schemas, so CI can run against a
throwaway registry that has never seen Debezium.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import yaml
from _env import ROOT

sys.path.insert(0, str(ROOT / "src"))
from adrp.contracts import apply_contract, canonical, columns_of, diff_columns

TABLES = ["customers", "merchants", "accounts", "transactions", "refunds", "support_tickets", "help_articles"]
PII: dict[str, set[str]] = {
    "customers": {"full_name", "email", "phone", "address", "date_of_birth"},
    "accounts": {"customer_id"},
    "transactions": {"customer_id"},
    "support_tickets": {"customer_id", "body"},
}
TOPIC_PREFIX = "cdc.public"
JSON_CT = {"Content-Type": "application/vnd.schemaregistry.v1+json"}


def registry_call(base: str, method: str, path: str, body: Any = None, ok: tuple[int, ...] = (200,)) -> Any:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{base}{path}", data=data, method=method, headers=JSON_CT)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        payload = e.read().decode(errors="replace")
        if e.code in ok:
            return json.loads(payload) if payload else None
        raise SystemExit(f"{method} {base}{path} -> {e.code}: {payload}") from None


def latest_schema(base: str, subject: str) -> dict[str, Any]:
    d = registry_call(base, "GET", f"/subjects/{subject}/versions/latest")
    return json.loads(d["schema"])


def contracts_dir() -> Path:
    return Path(os.environ.get("CONTRACTS_DIR", ROOT / "contracts"))


def cmd_export(a: argparse.Namespace) -> int:
    out = contracts_dir()
    (out / "baseline").mkdir(parents=True, exist_ok=True)
    for table in TABLES:
        topic = f"{TOPIC_PREFIX}.{table}"
        value = latest_schema(a.registry, f"{topic}-value")
        key = latest_schema(a.registry, f"{topic}-key")
        (out / "baseline" / f"{topic}-value.avsc").write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
        (out / "baseline" / f"{topic}-key.avsc").write_text(json.dumps(key, indent=2, sort_keys=True) + "\n")
        path = out / f"{table}.yaml"
        if path.exists() and not a.overwrite:
            print(f"kept existing contract {path.name} (use --overwrite to regenerate)")
            continue
        cols = [{**c, "pii": c["name"] in PII.get(table, set())} for c in columns_of(value)]
        doc = {
            "table": table,
            "topic": topic,
            "subject": f"{topic}-value",
            "key": [f["name"] for f in key["fields"]],
            "columns": cols,
        }
        path.write_text(yaml.safe_dump(doc, sort_keys=False, width=120))
        print(f"wrote {path.name}: {len(cols)} columns")
    return 0


def register_baselines(base: str) -> None:
    registry_call(base, "PUT", "/config", {"compatibility": "BACKWARD"})
    for f in sorted((contracts_dir() / "baseline").glob("*.avsc")):
        subject = f.stem
        existing = registry_call(base, "GET", f"/subjects/{subject}/versions", ok=(200, 404))
        if isinstance(existing, list) and existing:
            continue
        registry_call(base, "POST", f"/subjects/{subject}/versions", {"schema": f.read_text()})
        print(f"registered baseline {subject}")


def cmd_check(a: argparse.Namespace) -> int:
    if a.register_baseline:
        register_baselines(a.registry)
    failures = 0
    for path in sorted(contracts_dir().glob("*.yaml")):
        contract = yaml.safe_load(path.read_text())
        subject = contract["subject"]
        latest = latest_schema(a.registry, subject)
        candidate = apply_contract(latest, contract)
        drift = diff_columns(columns_of(latest), contract["columns"])
        if canonical(candidate) == canonical(latest):
            print(
                f"  PASS  {contract['table']}: contract matches registry v-latest ({len(contract['columns'])} columns)"
            )
            continue
        res = registry_call(
            a.registry,
            "POST",
            f"/compatibility/subjects/{subject}/versions/latest?verbose=true",
            {"schema": json.dumps(candidate)},
        )
        if res.get("is_compatible"):
            print(f"  PASS  {contract['table']}: contract differs from registry but is BACKWARD compatible {drift}")
        else:
            failures += 1
            print(f"  FAIL  {contract['table']}: INCOMPATIBLE with the registered schema {drift}")
            for m in res.get("messages", [])[:5]:
                print(f"          registry: {m}")
    print(f"{'FAILED' if failures else 'OK'}: {failures} incompatible contract(s)")
    return 1 if failures else 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--registry", default=os.environ.get("REGISTRY_URL", "http://localhost:18081"))
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("export")
    e.add_argument("--overwrite", action="store_true")
    c = sub.add_parser("check")
    c.add_argument("--register-baseline", action="store_true")
    a = ap.parse_args()
    return {"export": cmd_export, "check": cmd_check}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
