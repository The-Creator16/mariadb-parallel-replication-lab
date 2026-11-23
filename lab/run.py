"""One run = one workload under one set of replication settings.

    reset data -> apply settings -> start heartbeat + sampler
    -> run the workload on the primary -> wait until the replica has caught up
    -> write results/<experiment>/<run_id>/{meta.json, samples.csv, summary.json, groups.json}
"""
from __future__ import annotations

import json
import os
import platform
import subprocess
import time
from pathlib import Path
from contextlib import ExitStack

from . import analysis, binlog, db, topology
from .sampler import Sampler
from .workload import Heartbeat, effective_clients, run_load

DEFAULTS = {
    "workload": "independent",
    "clients": 32,           # concurrent client connections on the primary
    "tx_per_client": 1000,   # transactions each client commits
    "rows": 4,               # rows changed per transaction (x2 for independent/hotspot: update + insert)
    "accounts": 200_000,
    "hot": 1,                # hotspot: number of shared counter rows
    "archive_rows": 1_000,   # size of the table the DDL workloads rebuild (the ddl plan raises it)
    "own_tables": 32,
    "ddl_at": 5,             # seconds into the load when the ALTER starts
    # replica
    "threads": 0,            # slave_parallel_threads (0 = single-threaded SQL thread)
    "mode": "optimistic",    # slave_parallel_mode
    "max_queued": None,      # slave_parallel_max_queued (None = leave as is)
    "domain_threads": 0,     # slave_domain_parallel_threads (0 = no per-domain cap)
    "replica_sync_binlog": 1,  # replica durability (control experiment only)
    "replica_flush_log": 1,
    "replica_cpus": None,    # CPU budget of the replica container for this run (docker update --cpus)
    # catchup=False: "live" -- the replica applies while the load runs.
    # catchup=True: the replica's SQL thread is stopped during the load (the
    #   IO thread keeps fetching), then started: the drain measures the
    #   replica's pure apply capacity, with the primary idle.
    "catchup": False,
    # primary
    "commit_wait_count": 0,  # binlog_commit_wait_count
    "commit_wait_usec": 100000,
    # harness
    "sample_interval": 0.25,
    "settle_s": 3,
    "timeout_s": 1200,
}

PRIMARY_VARS = ["version", "binlog_format", "sync_binlog", "innodb_flush_log_at_trx_commit",
                "binlog_commit_wait_count", "binlog_commit_wait_usec", "binlog_checksum",
                "binlog_annotate_row_events", "innodb_buffer_pool_size", "gtid_domain_id"]
REPLICA_VARS = ["version", "slave_parallel_threads", "slave_parallel_mode",
                "slave_parallel_max_queued", "slave_domain_parallel_threads", "log_slave_updates",
                "sync_binlog", "innodb_flush_log_at_trx_commit", "slave_sql_verify_checksum",
                "replicate_annotate_row_events", "innodb_buffer_pool_size"]


def run_id_for(spec: dict, repeat: int) -> str:
    rid = f"{spec['workload']}-{spec['mode']}-t{spec['threads']:02d}"
    if spec.get("commit_wait_count"):
        rid += f"-gc{spec['commit_wait_count']}x{spec['commit_wait_usec']}"
    if spec["workload"] == "hotspot":
        rid += f"-hot{spec['hot']}"
    if spec.get("replica_sync_binlog") not in (None, 1) or spec.get("replica_flush_log") not in (None, 1):
        rid += f"-durab{spec['replica_sync_binlog']}{spec['replica_flush_log']}"
    if spec.get("replica_cpus"):
        rid += f"-cpu{spec['replica_cpus']:g}"
    if spec.get("catchup"):
        rid += "-catchup"
    return f"{rid}-r{repeat}"


def execute(spec: dict, out_dir: Path, replicas: list[str] | None = None,
            analyse_binlog: bool = True) -> dict:
    spec = {**DEFAULTS, **spec}
    replicas = replicas or ["replica"]
    out_dir.mkdir(parents=True, exist_ok=True)

    # A failed rerun must not retain a previous completion marker or histogram.
    for filename in ("summary.json", "groups.json", "meta.json"):
        (out_dir / filename).unlink(missing_ok=True)

    # Rebuild the workload tables.
    topology.reset_data(spec["accounts"], spec["archive_rows"], spec["own_tables"])
    topology.configure_primary(spec)
    topology.configure_replica(spec)
    with ExitStack() as cleanup:
        cpus = _set_replica_cpus(spec.get("replica_cpus"))
        if cpus is not None:
            cleanup.callback(_set_replica_cpus, os.environ.get("REPLICA_CPUS", "0"))
        db.wait_caught_up("replica")

        meta = {
            "spec": spec,
            "replicas": replicas,
            "n_tx": effective_clients(spec) * spec["tx_per_client"],
            "primary": _vars("primary", PRIMARY_VARS),
            "replica": _vars("replica", REPLICA_VARS),
            "host": _host_info(),
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        }
        p = db.connect("primary")
        cleanup.callback(p.close)
        bl_start = db.q(p, "SHOW MASTER STATUS")[0][:2]

        # Exclude the idle time since the previous heartbeat from the baseline.
        hb = Heartbeat()
        hb.start()
        cleanup.callback(hb.stop)
        _wait_fresh_heartbeat("replica")
        t0 = time.time()
        sampler = Sampler(out_dir / "samples.csv", replicas, t0, spec["sample_interval"])
        sampler.start()
        cleanup.callback(sampler.stop)
        time.sleep(1.5)                                   # a baseline before the load
        events: list[dict] = []
        r = db.connect("replica")
        cleanup.callback(r.close)
        if spec["catchup"]:
            db.q(r, "STOP SLAVE SQL_THREAD")
            restore_sql = cleanup.enter_context(ExitStack())
            restore_sql.callback(db.q, r, "START SLAVE SQL_THREAD")
            events.append({"t": time.time() - t0, "event": "sql_thread_stopped"})
        load_start, load_end = run_load(spec, t0, events)
        apply_from = load_start
        if spec["catchup"]:
            _wait_io_fetched(r, p)
            db.q(r, "START SLAVE SQL_THREAD")
            restore_sql.pop_all()
            apply_from = time.time() - t0
            events.append({"t": apply_from, "event": "sql_thread_started"})
        print(f"  load done in {load_end - load_start:.1f}s; waiting for the replica ...", flush=True)
        caught_up = _wait_drained(sampler, "replica", spec["timeout_s"], max(apply_from, load_end))
        time.sleep(spec["settle_s"])
        sampler.stop()
        if sampler.error:
            raise sampler.error

        bl_end = db.q(p, "SHOW MASTER STATUS")[0][:2]
    meta.update({
        "load_start": load_start, "load_end": load_end, "caught_up_t": caught_up,
        "apply_from": apply_from, "replica_cpus_this_run": cpus,
        "events": events,
        "binlog_range": {"start": list(bl_start), "end": list(bl_end)},
    })
    (out_dir / "meta.json").write_text(json.dumps(meta, indent=2, default=str))

    # Binlog decoding is optional. Samples remain available if it fails.
    if analyse_binlog:
        try:
            groups = binlog.commit_groups(bl_start, bl_end)
            (out_dir / "groups.json").write_text(json.dumps(groups, indent=2))
        except Exception as e:  # analysis is a bonus; never lose a run over it
            print(f"  (binlog analysis skipped: {e})")

    summary = analysis.summarize(out_dir)
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    print("  " + analysis.one_line(summary), flush=True)
    return summary


def _wait_drained(sampler: Sampler, name: str, timeout: float, earliest: float) -> float:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if sampler.error:
            raise sampler.error
        s = sampler.latest.get(name)
        # The heartbeat keeps adding a transaction every 100 ms, so "caught
        # up" means: at most a couple of transactions behind and the
        # heartbeat fresh.
        if s and s["t"] >= earliest and s["backlog_tx"] <= 2 and s["hb_lag_s"] != "" and s["hb_lag_s"] < 0.5:
            return s["t"]
        time.sleep(0.2)
    raise TimeoutError(f"{name} did not catch up within {timeout}s")


def _wait_fresh_heartbeat(name: str, timeout: float = 60) -> None:
    c = db.connect(name)
    deadline = time.time() + timeout
    while time.time() < deadline:
        age = db.q1(c, "SELECT TIMESTAMPDIFF(MICROSECOND, ts, NOW(6)) / 1e6 FROM labctl.heartbeat")
        if age is not None and float(age) < 0.5:
            break
        time.sleep(0.1)
    else:
        c.close()
        raise TimeoutError(f"No fresh heartbeat on {name}")
    c.close()


def _wait_io_fetched(r, p, timeout: float = 120) -> None:
    """Catch-up mode: wait until the IO thread has copied the whole workload
    into the relay log, so the drain measures applying, not fetching."""
    target = db.q(p, "SHOW MASTER STATUS")[0][:2]
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = db.slave_status(r)
        if (st.get("Master_Log_File"), st.get("Read_Master_Log_Pos")) >= (target[0], target[1]):
            return
        time.sleep(0.2)
    raise TimeoutError("Replica IO thread did not fetch the workload")


def _set_replica_cpus(cpus) -> float | None:
    """Change the replica container's CPU budget on the fly (cgroup quota)."""
    if cpus is None:
        return None
    cid = subprocess.run(["docker", "compose", "ps", "-q", "replica"], cwd=db.ROOT,
                         capture_output=True, text=True).stdout.strip()
    subprocess.run(["docker", "update", "--cpus", str(cpus), cid], check=True,
                   capture_output=True)
    return float(cpus)


def _vars(server: str, names: list[str]) -> dict:
    c = db.connect(server)
    out = db.global_vars(c, names)
    c.close()
    return out


def _host_info() -> dict:
    info = {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "host_cpus": os.cpu_count(),
        "mariadb_image": os.environ.get("MARIADB_IMAGE", "mariadb:11.8"),
        "primary_cpus_limit": os.environ.get("PRIMARY_CPUS", "0"),
        "replica_cpus_limit": os.environ.get("REPLICA_CPUS", "0"),
    }
    try:
        info["git_commit"] = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], cwd=db.ROOT, capture_output=True,
            text=True, timeout=5).stdout.strip()
    except Exception:
        pass
    try:
        with open("/proc/meminfo") as fh:
            info["host_mem_gb"] = round(int(fh.readline().split()[1]) / 1024 / 1024, 1)
    except OSError:
        pass
    return info
