"""Write workloads and the heartbeat.

A workload is N client connections to the primary, each CALLing one of the
procedures in sql/labctl.sql. A few workloads add a side event -- an ALTER
TABLE that starts a few seconds into the run -- to show what DDL does to a
replica.

"""
from __future__ import annotations

import threading
import time

from . import db

DDL_SQL = ("ALTER TABLE lab.archive ADD COLUMN extra INT NOT NULL DEFAULT 0, "
           "ALGORITHM=COPY")

WORKLOADS = ("independent", "hotspot", "serial", "own_innodb", "own_myisam", "ddl", "ddl_domain")


def client_call(spec: dict, client: int) -> tuple[str, tuple]:
    """The CALL one client issues for a given run spec."""
    wl = spec["workload"]
    clients = effective_clients(spec)
    tx, rows, accounts = spec["tx_per_client"], spec["rows"], spec["accounts"]
    if wl in ("independent", "serial", "ddl", "ddl_domain"):
        return "CALL labctl.wl_independent(%s,%s,%s,%s,%s)", (client, clients, tx, rows, accounts)
    if wl == "hotspot":
        return ("CALL labctl.wl_hotspot(%s,%s,%s,%s,%s,%s)",
                (client, clients, tx, rows, accounts, spec.get("hot", 1)))
    if wl in ("own_innodb", "own_myisam"):
        return "CALL labctl.wl_own_table(%s,%s,%s,%s)", (client, tx, rows, wl.split("_")[1])
    raise ValueError(f"unknown workload {wl!r}; choose from {WORKLOADS}")


def effective_clients(spec: dict) -> int:
    return 1 if spec["workload"] == "serial" else spec["clients"]


class Heartbeat(threading.Thread):
    """UPDATE labctl.heartbeat SET ts = NOW(6) on the primary every `interval`
    seconds. Each update is one tiny autocommitted transaction in domain 0."""

    def __init__(self, interval: float = 0.1):
        super().__init__(daemon=True, name="heartbeat")
        self.interval = interval
        self.stop_event = threading.Event()

    def run(self) -> None:
        conn = db.connect("primary")
        while not self.stop_event.is_set():
            t = time.time()
            db.q(conn, "UPDATE labctl.heartbeat SET ts = NOW(6), seq = seq + 1 WHERE id = 1")
            self.stop_event.wait(max(0.0, self.interval - (time.time() - t)))
        conn.close()

    def stop(self) -> None:
        self.stop_event.set()
        self.join(timeout=5)


def run_load(spec: dict, t0: float, events: list) -> tuple[float, float]:
    """Run the workload to completion. Returns (start, end) relative to t0.
    Side events (DDL start/end) are appended to `events`."""
    n = effective_clients(spec)
    errors: list[BaseException] = []
    barrier = threading.Barrier(n + 1)

    def worker(client: int) -> None:
        try:
            conn = db.connect("primary")
            sql, args = client_call(spec, client)
            barrier.wait()
            db.q(conn, sql, args)
            conn.close()
        except BaseException as e:  # noqa: BLE001 -- reported below
            errors.append(e)
            try:
                barrier.abort()
            except threading.BrokenBarrierError:
                pass

    threads = [threading.Thread(target=worker, args=(c,), daemon=True) for c in range(n)]
    for th in threads:
        th.start()
    barrier.wait()           # all clients connected; go
    start = time.time() - t0

    ddl_thread = None
    if spec["workload"] in ("ddl", "ddl_domain"):
        def run_ddl():
            try:
                _ddl(spec, t0, events)
            except Exception as exc:
                errors.append(exc)

        ddl_thread = threading.Thread(target=run_ddl, daemon=True)
        ddl_thread.start()

    for th in threads:
        th.join()
    if ddl_thread:
        ddl_thread.join()
    end = time.time() - t0
    if errors:
        raise RuntimeError(f"workload failed: {errors[0]!r}")
    return start, end


def _ddl(spec: dict, t0: float, events: list) -> None:
    time.sleep(spec.get("ddl_at", 5))
    conn = db.connect("primary")
    if spec["workload"] == "ddl_domain":
        # This session writes only to the independent archive table.
        # A parallel replica may apply domain 1 independently of domain 0.
        db.q(conn, "SET SESSION gtid_domain_id = 1")
    events.append({"t": time.time() - t0, "event": "ddl_start", "sql": DDL_SQL})
    db.q(conn, DDL_SQL)
    events.append({"t": time.time() - t0, "event": "ddl_end_on_primary"})
    conn.close()
