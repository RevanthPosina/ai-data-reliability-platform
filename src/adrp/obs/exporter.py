"""Prometheus-format exporter for CDC health (PLAN G2, design S13). Stdlib HTTP server + psycopg.

Metrics (computed on every scrape, so values are never stale):
  adrp_replication_slot_lag_bytes{slot}          current WAL position - confirmed_flush_lsn
  adrp_replication_slot_retained_wal_bytes{slot} current WAL position - restart_lsn (what Postgres must keep)
  adrp_replication_slot_active{slot}             1 if a consumer is attached
  adrp_connector_up{connector}                   1 iff connector and every task are RUNNING (and >= 1 task)
  adrp_connector_tasks{connector,state}          task count per state
  adrp_exporter_source_up{source}                1 if the source (postgres | connect) answered
No customer or entity ids are ever used as labels.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg

log = logging.getLogger("exporter")

SLOT_SQL = """
SELECT slot_name,
       active::int,
       COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::float8,
       COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::float8
FROM pg_replication_slots
WHERE slot_type = 'logical'
"""


@dataclass(frozen=True)
class Slot:
    name: str
    active: int
    lag_bytes: float
    retained_bytes: float


@dataclass(frozen=True)
class Connector:
    name: str
    state: str
    task_states: tuple[str, ...]

    @property
    def up(self) -> int:
        return int(self.state == "RUNNING" and bool(self.task_states) and all(t == "RUNNING" for t in self.task_states))


def collect_slots(dsn: str) -> list[Slot]:
    with psycopg.connect(dsn, connect_timeout=5, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(SLOT_SQL)
        return [Slot(str(n), int(a), float(lag), float(ret)) for n, a, lag, ret in cur.fetchall()]


def collect_connectors(connect_url: str) -> list[Connector]:
    with urllib.request.urlopen(f"{connect_url}/connectors?expand=status", timeout=5) as r:
        body = json.load(r)
    out = []
    for name, info in sorted(body.items()):
        st = info["status"]
        out.append(Connector(name, st["connector"]["state"], tuple(t["state"] for t in st.get("tasks", []))))
    return out


def _metric(lines: list[str], name: str, help_text: str, kind: str) -> None:
    lines.append(f"# HELP {name} {help_text}")
    lines.append(f"# TYPE {name} {kind}")


def render(slots: list[Slot] | None, connectors: list[Connector] | None) -> str:
    """slots / connectors are None when that source could not be read."""
    lines: list[str] = []
    _metric(lines, "adrp_exporter_source_up", "1 if the source answered on this scrape", "gauge")
    lines.append(f'adrp_exporter_source_up{{source="postgres"}} {int(slots is not None)}')
    lines.append(f'adrp_exporter_source_up{{source="connect"}} {int(connectors is not None)}')
    if slots is not None:
        _metric(
            lines, "adrp_replication_slot_lag_bytes", "WAL bytes between current position and confirmed flush", "gauge"
        )
        lines += [f'adrp_replication_slot_lag_bytes{{slot="{s.name}"}} {s.lag_bytes:.0f}' for s in slots]
        _metric(lines, "adrp_replication_slot_retained_wal_bytes", "WAL bytes retained for the slot", "gauge")
        lines += [f'adrp_replication_slot_retained_wal_bytes{{slot="{s.name}"}} {s.retained_bytes:.0f}' for s in slots]
        _metric(lines, "adrp_replication_slot_active", "1 if a consumer is attached to the slot", "gauge")
        lines += [f'adrp_replication_slot_active{{slot="{s.name}"}} {s.active}' for s in slots]
    if connectors is not None:
        _metric(lines, "adrp_connector_up", "1 if the connector and all its tasks are RUNNING", "gauge")
        lines += [f'adrp_connector_up{{connector="{c.name}"}} {c.up}' for c in connectors]
        _metric(lines, "adrp_connector_tasks", "Connector task count by state", "gauge")
        for c in connectors:
            for state in sorted(set(c.task_states)):
                lines.append(
                    f'adrp_connector_tasks{{connector="{c.name}",state="{state}"}} {c.task_states.count(state)}'
                )
    return "\n".join(lines) + "\n"


def scrape(dsn: str, connect_url: str) -> str:
    slots: list[Slot] | None
    connectors: list[Connector] | None
    try:
        slots = collect_slots(dsn)
    except Exception as e:  # noqa: BLE001 - report as source_up=0, keep serving
        log.warning("postgres scrape failed: %s", type(e).__name__)
        slots = None
    try:
        connectors = collect_connectors(connect_url)
    except Exception as e:  # noqa: BLE001
        log.warning("connect scrape failed: %s", type(e).__name__)
        connectors = None
    return render(slots, connectors)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    dsn = os.environ["SHOP_DSN"]
    connect_url = os.environ.get("CONNECT_URL", "http://connect:8083")
    port = int(os.environ.get("PORT", "9108"))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/healthz":
                body, ctype = b"ok\n", "text/plain"
            elif self.path == "/metrics":
                body, ctype = scrape(dsn, connect_url).encode(), "text/plain; version=0.0.4"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    log.info("exporter listening on :%d", port)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
