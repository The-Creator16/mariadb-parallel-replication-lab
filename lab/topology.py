"""Replication topology: wiring replicas to the primary, and changing the
settings a run is about.

Every statement that matters for the tutorial is printed when `verbose` is
on, so `python -m lab setup` doubles as a worked example of setting up GTID
replication by hand.
"""
from __future__ import annotations

import os
import time

from . import db

DELAY_SECONDS = int(os.environ.get("LAB_DELAY", 30))


def _say(verbose: bool, server: str, sql: str) -> None:
    if verbose:
        print(f"  [{server}] {sql}")


def running_replicas() -> list[str]:
    return [s for s in ("replica", "delayed") if db.is_up(s)]


def setup(verbose: bool = True) -> None:
    """(Re)build replication from scratch:
    1. create the replication user on the primary,
    2. point every running replica at the primary's *current* GTID position,
    3. (re)install the labctl schema on the primary -- it replicates.
    Reinitialises the disposable lab schemas and replication coordinates."""
    db.wait_up("primary")
    replicas = running_replicas()
    if "replica" not in replicas:
        db.wait_up("replica")
        replicas = running_replicas()

    p = db.connect("primary")
    print(f"primary: {db.q1(p, 'SELECT VERSION()')}")
    for sql in (
        f"CREATE USER IF NOT EXISTS '{db.REPL_USER}'@'%' IDENTIFIED BY '{db.REPL_PASSWORD}'",
        f"GRANT REPLICATION SLAVE ON *.* TO '{db.REPL_USER}'@'%'",
    ):
        _say(verbose, "primary", sql)
        db.q(p, sql)
    start_pos = db.q1(p, "SELECT @@gtid_binlog_pos")

    for name in replicas:
        r = db.connect(name)
        delay = DELAY_SECONDS if name == "delayed" else 0
        stmts = [
            "STOP SLAVE",
            "RESET SLAVE ALL",
            # Skip pre-setup history. The disposable lab schemas are rebuilt below.
            f"SET GLOBAL gtid_slave_pos = '{start_pos}'",
            (f"CHANGE MASTER TO MASTER_HOST='{db.PRIMARY_INTERNAL_HOST}', MASTER_PORT=3306, "
             f"MASTER_USER='{db.REPL_USER}', MASTER_PASSWORD='{db.REPL_PASSWORD}', "
             f"MASTER_USE_GTID=slave_pos, MASTER_DELAY={delay}"),
            "START SLAVE",
        ]
        for sql in stmts:
            _say(verbose, name, sql)
            db.q(r, sql)
        _wait_threads_running(r, name)
        print(f"{name}: {db.q1(r, 'SELECT VERSION()')} replicating from gtid {start_pos!r}"
              + (f", MASTER_DELAY={delay}s" if delay else ""))
        r.close()

    # The control schema: dropped and recreated so primary and replicas can
    # never disagree about it.
    _say(verbose, "primary", "DROP DATABASE IF EXISTS lab; DROP DATABASE IF EXISTS labctl; SOURCE sql/labctl.sql")
    db.q(p, "DROP DATABASE IF EXISTS lab")
    db.q(p, "DROP DATABASE IF EXISTS labctl")
    db.run_sql_file(p, "labctl.sql")
    p.close()
    db.wait_caught_up("replica")
    print("setup done: labctl installed on the primary and replicated.")


def _wait_threads_running(conn, name: str, timeout: float = 30) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = db.slave_status(conn)
        if st.get("Slave_IO_Running") == "Yes" and st.get("Slave_SQL_Running") == "Yes":
            return
        if st.get("Last_IO_Error") or st.get("Last_SQL_Error"):
            raise RuntimeError(f"{name}: {st.get('Last_IO_Error') or st.get('Last_SQL_Error')}")
        time.sleep(0.5)
    raise TimeoutError(f"{name}: replication threads did not start")


# Settings a run may vary, and where they live.
REPLICA_SETTINGS = {
    # run-spec key        -> server variable
    "threads": "slave_parallel_threads",
    "mode": "slave_parallel_mode",
    "max_queued": "slave_parallel_max_queued",
    "domain_threads": "slave_domain_parallel_threads",
    # The replica's own durability. Not a parallel-replication setting, but
    # the control experiment: how much of the lag is the cost of fsync?
    "replica_sync_binlog": "sync_binlog",
    "replica_flush_log": "innodb_flush_log_at_trx_commit",
}
PRIMARY_SETTINGS = {
    "commit_wait_count": "binlog_commit_wait_count",
    "commit_wait_usec": "binlog_commit_wait_usec",
}


def configure_replica(spec: dict, name: str = "replica", verbose: bool = False) -> dict:
    """Apply the parallel-replication settings of a run. slave_parallel_threads
    can only change while replication is stopped, so stop, set, start."""
    r = db.connect(name)
    stmts = ["STOP SLAVE"]
    for key, var in REPLICA_SETTINGS.items():
        if key in spec and spec[key] is not None:
            val = spec[key]
            stmts.append(f"SET GLOBAL {var} = " + (f"'{val}'" if isinstance(val, str) else str(val)))
    stmts.append("START SLAVE")
    for sql in stmts:
        _say(verbose, name, sql)
        db.q(r, sql)
    _wait_threads_running(r, name)
    applied = db.global_vars(r, list(REPLICA_SETTINGS.values()))
    r.close()
    return applied


def configure_primary(spec: dict, verbose: bool = False) -> dict:
    p = db.connect("primary")
    for key, var in PRIMARY_SETTINGS.items():
        if key in spec and spec[key] is not None:
            sql = f"SET GLOBAL {var} = {int(spec[key])}"
            _say(verbose, "primary", sql)
            db.q(p, sql)
    applied = db.global_vars(p, list(PRIMARY_SETTINGS.values()))
    p.close()
    return applied


def purge_binlogs(name: str) -> None:
    """Keep disk use flat across dozens of runs: rotate the binlog and drop
    the old files. Only called when every replica has caught up."""
    c = db.connect(name)
    try:
        db.q(c, "FLUSH BINARY LOGS")
        current = db.q(c, "SHOW MASTER STATUS")[0][0]
        db.q(c, f"PURGE BINARY LOGS TO '{current}'")
    except Exception as e:  # a log still in use by a slow dump thread, etc.
        print(f"  (purge on {name} skipped: {e})")
    finally:
        c.close()


def reset_data(accounts: int, archive_rows: int, own_tables: int) -> None:
    """Recreate the `lab` schema with identical data, and wait until every
    replica that is part of the run has it."""
    db.wait_caught_up("replica")
    purge_binlogs("primary")
    purge_binlogs("replica")
    p = db.connect("primary")
    db.run_sql_file(p, "reset.sql", accounts=accounts, archive_rows=archive_rows,
                    own_tables=own_tables)
    p.close()
    db.wait_caught_up("replica")
