"""Turn a run's samples into the numbers the README reasons with.

Definitions (all times in seconds, relative to the run's t0):

  primary_tps     n_tx / (load_end - load_start): how fast the primary
                  committed the workload.
  replica_tps     the replica's apply rate *while it had work queued*
                  (backlog > BUSY_BACKLOG transactions): its capacity. If the
                  replica never fell behind, it is reported equal to the rate
                  it applied at, and `kept_up` is true.
  peak_lag_s      maximum heartbeat lag.
  catchup_s       load_end -> first moment the replica is caught up.
  total_s         load_start -> caught up: the time until the replica holds
                  everything the workload wrote. The headline number.
  lag_auc         integral of heartbeat lag over the run (lag-seconds):
                  peak and duration in one number.
  p_group_ratio   transactions per binlog group commit on the primary
                  (delta Binlog_commits / delta Binlog_group_commits).
  r_group_ratio   the same on the replica's own binlog: parallel apply lets
                  the replica group-commit too.
  retried_tx      delta Slave_retried_transactions: speculative applies that
                  conflicted and were rolled back.
"""
from __future__ import annotations

import csv
import json
import statistics
from collections import Counter
from pathlib import Path

BUSY_BACKLOG = 50


def load_samples(run_dir: Path, server: str = "replica") -> list[dict]:
    rows = []
    with open(run_dir / "samples.csv") as fh:
        for r in csv.DictReader(fh):
            if r["server"] != server:
                continue
            for k in ("t", "hb_lag_s", "mstd_s", "sbm_s"):
                r[k] = float(r[k]) if r[k] not in ("", "None") else None
            for k in ("backlog_tx", "primary_seq", "replica_seq", "retried_tx",
                      "p_binlog_commits", "p_binlog_group_commits",
                      "r_binlog_commits", "r_binlog_group_commits"):
                r[k] = int(r[k]) if r[k] not in ("", "None") else None
            rows.append(r)
    return rows


def _ratio(rows, a, b, t_from, t_to):
    sel = [r for r in rows if t_from <= r["t"] <= t_to and r[a] is not None]
    if len(sel) < 2:
        return None
    da, db_ = sel[-1][a] - sel[0][a], sel[-1][b] - sel[0][b]
    return round(da / db_, 2) if db_ else None


def summarize(run_dir: Path) -> dict:
    meta = json.loads((run_dir / "meta.json").read_text())
    rows = load_samples(run_dir)
    spec = meta["spec"]
    ls, le, cu = meta["load_start"], meta["load_end"], meta["caught_up_t"]
    # Early versions could return the last pre-completion sample.
    if cu is not None and cu < le:
        cu = next((r["t"] for r in rows if r["t"] >= le and r["backlog_tx"] <= 2
                   and r["hb_lag_s"] is not None and r["hb_lag_s"] < 0.5), None)
    af = meta.get("apply_from", ls)   # catch-up mode: the moment the SQL thread started
    n_tx = meta["n_tx"]

    busy_tx = busy_t = 0.0
    for a, b in zip(rows, rows[1:]):
        if a["backlog_tx"] > BUSY_BACKLOG and a["t"] >= af:
            busy_tx += b["replica_seq"] - a["replica_seq"]
            busy_t += b["t"] - a["t"]
    kept_up = busy_t < 2.0
    total_s = (cu - ls) if cu is not None else None
    replica_tps = (busy_tx / busy_t) if not kept_up else (n_tx / total_s if total_s else None)

    lag = [(r["t"], r["hb_lag_s"]) for r in rows if r["hb_lag_s"] is not None]
    auc = sum((t2 - t1) * (l1 + l2) / 2 for (t1, l1), (t2, l2) in zip(lag, lag[1:]))
    retried = [r["retried_tx"] for r in rows if r["retried_tx"] is not None]

    states = Counter()
    for r in rows:
        if r["worker_states"] and af <= r["t"] <= (cu or le):
            for s, n in json.loads(r["worker_states"]).items():
                states[s] += n
    tot_states = sum(states.values())

    groups = {}
    if (run_dir / "groups.json").exists():
        groups = json.loads((run_dir / "groups.json").read_text())

    return {
        "run": run_dir.name,
        "experiment": run_dir.parent.name,
        "workload": spec["workload"], "threads": spec["threads"], "mode": spec["mode"],
        "clients": spec["clients"], "tx_per_client": spec["tx_per_client"], "rows": spec["rows"],
        "hot": spec.get("hot"), "commit_wait_count": spec.get("commit_wait_count"),
        "commit_wait_usec": spec.get("commit_wait_usec"),
        "n_tx": n_tx,
        "load_s": round(le - ls, 2),
        "primary_tps": round(n_tx / (le - ls), 1),
        "replica_tps": round(replica_tps, 1) if replica_tps else None,
        "kept_up": kept_up,
        "peak_lag_s": round(max(l for _, l in lag), 2) if lag else None,
        "catchup_s": round(cu - le, 2) if cu is not None else None,
        "total_s": round(total_s, 2) if total_s else None,
        "drain_s": round(cu - af, 2) if cu is not None else None,
        "catchup_mode": bool(spec.get("catchup")),
        "replica_cpus": meta.get("replica_cpus_this_run") or meta["host"].get("replica_cpus_limit"),
        "lag_auc": round(auc, 1),
        "p_group_ratio": _ratio(rows, "p_binlog_commits", "p_binlog_group_commits", ls, le),
        "r_group_ratio": _ratio(rows, "r_binlog_commits", "r_binlog_group_commits", ls, cu or le),
        "retried_tx": (retried[-1] - retried[0]) if len(retried) > 1 else None,
        "worker_states": {s: round(n / tot_states, 3) for s, n in states.most_common()} if tot_states else {},
        "binlog_mean_group": groups.get("mean_group_size"),
        "binlog_flags": groups.get("flags"),
        "version": meta["replica"].get("version"),
    }


def one_line(s: dict) -> str:
    return (f"{s['run']}: primary {s['primary_tps']:.0f} tx/s, replica {s['replica_tps'] or 0:.0f} tx/s"
            f"{' (kept up)' if s['kept_up'] else ''}, peak lag {s['peak_lag_s']}s, "
            f"caught up {s['total_s']}s after load start, retries {s['retried_tx']}")


def experiment_summaries(exp_dir: Path) -> list[dict]:
    out = []
    for d in sorted(exp_dir.iterdir()):
        if (d / "summary.json").exists():
            out.append(json.loads((d / "summary.json").read_text()))
    return out


def config_key(s: dict) -> str:
    """Everything that identifies a configuration, i.e. the run id minus the repeat."""
    return s["run"].rsplit("-r", 1)[0]


def aggregate(summaries: list[dict], metric: str) -> dict[str, dict]:
    by = {}
    for s in summaries:
        by.setdefault(config_key(s), []).append(s)
    out = {}
    for key, runs in by.items():
        vals = [r[metric] for r in runs if r.get(metric) is not None]
        out[key] = {
            "runs": runs,
            "n": len(vals),
            "median": statistics.median(vals) if vals else None,
            "min": min(vals) if vals else None,
            "max": max(vals) if vals else None,
        }
    return out
