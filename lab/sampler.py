"""Measure replication lag over time.

Every `interval` seconds the sampler records, for the primary and each
sampled replica, four views of "how far behind":

  hb_lag_s      heartbeat lag: NOW(6) - labctl.heartbeat.ts, read on the
                replica. Sub-second resolution, measured in the data itself.
                The charts use this domain-0 freshness indicator.
  backlog_tx    transactions the replica has not applied yet:
                @@gtid_binlog_pos on the primary minus @@gtid_slave_pos on the
                replica, summed over replication domains. The positions are read in separate queries.
  sbm_s         Seconds_Behind_Master from SHOW SLAVE STATUS: whole seconds,
                based on the timestamp of the event being applied.
  mstd_s        Master_Slave_time_diff (MariaDB 11.6+): difference between
                the time of the last event read from the primary and the last
                one committed on the replica. Read from
                information_schema.SLAVE_STATUS -- it is not a column of
                plain SHOW SLAVE STATUS.

plus throughput counters (GTID sequence numbers, Binlog_commits,
Binlog_group_commits, Slave_retried_transactions) and, every second, what
the parallel worker threads are waiting for (SHOW PROCESSLIST states).
"""
from __future__ import annotations

import csv
import json
import threading
import time
from pathlib import Path

from . import db

FIELDS = [
    "t", "server", "hb_lag_s", "backlog_tx", "sbm_s", "mstd_s",
    "primary_seq", "replica_seq", "retried_tx",
    "p_binlog_commits", "p_binlog_group_commits",
    "r_binlog_commits", "r_binlog_group_commits",
    "relay_log_space", "sql_state", "worker_states",
]

PRIMARY_STATUS = ["Binlog_commits", "Binlog_group_commits"]
REPLICA_STATUS = ["Binlog_commits", "Binlog_group_commits", "Slave_retried_transactions"]

WORKER_STATES_SQL = (
    "SELECT STATE, COUNT(*) FROM information_schema.PROCESSLIST "
    "WHERE USER = 'system user' AND COMMAND = 'Slave_worker' GROUP BY STATE")


class Sampler(threading.Thread):
    def __init__(self, out_csv: Path, replicas: list[str], t0: float, interval: float = 0.25):
        super().__init__(daemon=True, name="sampler")
        self.out_csv = out_csv
        self.replicas = replicas
        self.t0 = t0
        self.interval = interval
        self.stop_event = threading.Event()
        self.latest: dict[str, dict] = {}
        self.error: BaseException | None = None
        self.has_mstd: bool | None = None

    def run(self) -> None:
        try:
            self._run()
        except BaseException as e:  # noqa: BLE001
            self.error = e

    def _run(self) -> None:
        p = db.connect("primary")
        rs = {name: db.connect(name) for name in self.replicas}
        with open(self.out_csv, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS)
            w.writeheader()
            tick = 0
            while not self.stop_event.is_set():
                t_start = time.time()
                ppos = db.parse_gtid_pos(db.q1(p, "SELECT @@gtid_binlog_pos"))
                pst = db.global_status(p, PRIMARY_STATUS)
                for name, r in rs.items():
                    row = self._sample_replica(name, r, ppos, pst, tick)
                    w.writerow(row)
                    self.latest[name] = row
                fh.flush()
                tick += 1
                self.stop_event.wait(max(0.0, self.interval - (time.time() - t_start)))
        p.close()
        for r in rs.values():
            r.close()

    def _sample_replica(self, name, r, ppos, pst, tick) -> dict:
        hb = db.q(r, "SELECT TIMESTAMPDIFF(MICROSECOND, ts, NOW(6)) / 1e6 "
                     "FROM labctl.heartbeat WHERE id = 1")
        rpos = db.parse_gtid_pos(db.q1(r, "SELECT @@gtid_slave_pos"))
        st = db.slave_status(r)
        if self.has_mstd is not False:
            # The 11.6+ columns are in information_schema.SLAVE_STATUS (and
            # SHOW ALL SLAVES STATUS), not in plain SHOW SLAVE STATUS.
            try:
                st["Master_Slave_time_diff"] = db.q1(
                    r, "SELECT Master_Slave_time_diff FROM information_schema.SLAVE_STATUS "
                       "WHERE Connection_name = ''")
                self.has_mstd = True
            except Exception:
                self.has_mstd = False
        rst = db.global_status(r, REPLICA_STATUS)
        workers = ""
        if tick % max(1, int(1 / self.interval)) == 0:
            workers = json.dumps({s or "": n for s, n in db.q(r, WORKER_STATES_SQL)})
        # A stopped SQL thread is fine (catch-up mode stops it on purpose);
        # a stopped SQL thread *with an error* ends the run.
        if st.get("Slave_SQL_Running") == "No" and st.get("Last_SQL_Errno"):
            raise RuntimeError(f"{name}: SQL thread stopped: {st.get('Last_SQL_Error')}")
        return {
            "t": round(time.time() - self.t0, 3),
            "server": name,
            "hb_lag_s": float(hb[0][0]) if hb else "",
            "backlog_tx": db.gtid_backlog(ppos, rpos),
            "sbm_s": st.get("Seconds_Behind_Master"),
            "mstd_s": st.get("Master_Slave_time_diff", ""),
            "primary_seq": sum(ppos.values()),
            "replica_seq": sum(rpos.values()),
            "retried_tx": rst.get("Slave_retried_transactions", ""),
            "p_binlog_commits": pst.get("Binlog_commits"),
            "p_binlog_group_commits": pst.get("Binlog_group_commits"),
            "r_binlog_commits": rst.get("Binlog_commits"),
            "r_binlog_group_commits": rst.get("Binlog_group_commits"),
            "relay_log_space": st.get("Relay_Log_Space"),
            "sql_state": st.get("Slave_SQL_Running_State", ""),
            "worker_states": workers,
        }

    def stop(self) -> None:
        self.stop_event.set()
        self.join(timeout=10)
