"""python -m lab <command>

  setup                       wire replication, install the control schema
  reset                       recreate the lab schema with fresh data (for hand-driven sessions)
  status                      one-shot replication status of every replica
  watch                       run the heartbeat and print lag / backlog twice a second (Ctrl-C)
  run  [--workload ...]       one run, results in results/adhoc/<run_id>/
  plan experiments/X.json     a whole experiment: every configuration x repeats
  summarize results/X         recompute summary.json for every run of an experiment
  plot [results/X ...]        charts into results/<X>/*.png (default: every experiment)
  report                      results/SUMMARY.md: one table per experiment
  demo-binlog                 annotated row events, commit ids, checksums -- shown live
  demo-delayed                a delayed replica used to undo a DROP TABLE
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from . import analysis, db, topology
from .run import DEFAULTS, execute, run_id_for

RESULTS = Path(os.environ.get("LAB_RESULTS_DIR", db.ROOT / "results")).resolve()


def cmd_setup(a):
    topology.setup(verbose=True)


def cmd_status(a):
    p = db.connect("primary")
    ppos = db.parse_gtid_pos(db.q1(p, "SELECT @@gtid_binlog_pos"))
    print(f"primary   gtid_binlog_pos={db.q1(p, 'SELECT @@gtid_binlog_pos')}")
    p.close()
    for name in topology.running_replicas():
        r = db.connect(name)
        st = db.slave_status(r)
        rpos = db.parse_gtid_pos(db.q1(r, "SELECT @@gtid_slave_pos"))
        try:
            hb = db.q(r, "SELECT TIMESTAMPDIFF(MICROSECOND, ts, NOW(6))/1e6 FROM labctl.heartbeat")
        except Exception:      # a delayed replica may not have applied labctl yet
            hb = [("not yet replicated",)]
        v = db.global_vars(r, ["slave_parallel_threads", "slave_parallel_mode"])
        print(f"{name:<9} IO={st.get('Slave_IO_Running')} SQL={st.get('Slave_SQL_Running')} "
              f"backlog={db.gtid_backlog(ppos, rpos)} tx  newest_heartbeat_age={hb[0][0] if hb else '?'}s  "
              f"Seconds_Behind_Master={st.get('Seconds_Behind_Master')}  "
              f"SQL_Delay={st.get('SQL_Delay')}  threads={v['slave_parallel_threads']} "
              f"mode={v['slave_parallel_mode']}\n{'':<10}state: {st.get('Slave_SQL_Running_State')}")
        r.close()


def cmd_reset(a):
    topology.reset_data(a.accounts, a.archive_rows, 32)
    print(f"lab schema recreated: {a.accounts} accounts, {a.archive_rows} archive rows, 32 own_* tables")


def cmd_watch(a):
    from .workload import Heartbeat
    hb = Heartbeat()          # keeps labctl.heartbeat fresh, so its age on a replica is the lag
    hb.start()
    p = db.connect("primary")
    rs = {n: db.connect(n) for n in topology.running_replicas()}
    try:
        while True:
            ppos = db.parse_gtid_pos(db.q1(p, "SELECT @@gtid_binlog_pos"))
            parts = []
            for n, r in rs.items():
                rpos = db.parse_gtid_pos(db.q1(r, "SELECT @@gtid_slave_pos"))
                try:
                    age = db.q1(r, "SELECT TIMESTAMPDIFF(MICROSECOND, ts, NOW(6))/1e6 FROM labctl.heartbeat")
                    lag = f"{float(age):6.2f}s"
                except Exception:
                    lag = "   n/a"
                parts.append(f"{n}: lag {lag} backlog {db.gtid_backlog(ppos, rpos):>7} tx")
            print(time.strftime("%H:%M:%S  ") + "   ".join(parts), flush=True)
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        hb.stop()
        p.close()
        for conn in rs.values():
            conn.close()


def _spec_from_args(a) -> dict:
    spec = {}
    for k in DEFAULTS:
        v = getattr(a, k, None)
        if v is not None:
            spec[k] = v
    return spec


def cmd_run(a):
    spec = {**DEFAULTS, **_spec_from_args(a)}
    out = RESULTS / a.experiment / run_id_for(spec, a.repeat)
    print(f"run {out}")
    replicas = ["replica"] + (["delayed"] if a.with_delayed else [])
    execute(spec, out, replicas=replicas, analyse_binlog=not a.no_binlog)


def cmd_plan(a):
    plan = json.loads(Path(a.plan).read_text())
    name = plan["name"]
    repeats = a.repeats or plan.get("repeats", 3)
    configs = [{**DEFAULTS, **plan.get("base", {}), **v} for v in plan["vary"]]
    exp_dir = RESULTS / name
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "plan.json").write_text(json.dumps({**plan, "repeats": repeats}, indent=2))
    todo = []
    # Repeats are interleaved (config A, B, C, A, B, C, ...) rather than run back
    # to back, so slow drift of the host affects every configuration alike.
    for rep in range(1, repeats + 1):
        for spec in configs:
            todo.append((spec, rep))
    t_start = time.time()
    for i, (spec, rep) in enumerate(todo, 1):
        out = exp_dir / run_id_for(spec, rep)
        if (out / "summary.json").exists() and not a.force:
            print(f"[{i}/{len(todo)}] {out.name}: done already, skipping (--force to redo)")
            continue
        print(f"[{i}/{len(todo)}] {out.name}  (elapsed {time.time() - t_start:.0f}s)", flush=True)
        execute(spec, out, replicas=["replica"], analyse_binlog=not a.no_binlog)
    print(f"plan {name}: {len(todo)} runs in {time.time() - t_start:.0f}s")


def cmd_summarize(a):
    for exp in a.experiments:
        for d in sorted(Path(exp).iterdir()):
            if (d / "meta.json").exists():
                s = analysis.summarize(d)
                (d / "summary.json").write_text(json.dumps(s, indent=2))
                print(analysis.one_line(s))


def cmd_plot(a):
    from . import plot
    exps = [Path(e).resolve() for e in a.experiments] or sorted(
        d for d in RESULTS.iterdir() if d.is_dir() and ((d / "plan.json").exists() or
                       (d.name == "demo_delayed" and (d / "settings.json").exists())))
    for e in exps:
        if e.name == "demo_delayed":
            from .demos import plot_delayed
            settings = json.loads((e / "settings.json").read_text())
            events = json.loads((e / "events.json").read_text())
            plot_delayed(e, events, settings["delay_seconds"])
            print(f"wrote {e / 'lag.png'}")
            continue
        for f in plot.plot_experiment(e):
            print(f"wrote {f}")


def cmd_report(a):
    from . import report
    path = report.write_report(RESULTS)
    print(f"wrote {path}")


def cmd_demo_binlog(a):
    from . import demos
    demos.binlog_demo()


def cmd_demo_delayed(a):
    from . import demos
    demos.delayed_demo()


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m lab", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("setup").set_defaults(fn=cmd_setup)
    rs = sub.add_parser("reset")
    rs.add_argument("--accounts", type=int, default=200_000)
    rs.add_argument("--archive-rows", type=int, default=1_000)
    rs.set_defaults(fn=cmd_reset)
    sub.add_parser("status").set_defaults(fn=cmd_status)
    sub.add_parser("watch").set_defaults(fn=cmd_watch)

    r = sub.add_parser("run", help="a single run")
    for k, v in DEFAULTS.items():
        if isinstance(v, bool):
            r.add_argument("--" + k.replace("_", "-"), dest=k, action="store_true", default=None)
            continue
        typ = type(v) if v is not None else int
        if k in ("workload", "mode"):
            typ = str
        if k in ("replica_cpus", "sample_interval"):
            typ = float
        r.add_argument("--" + k.replace("_", "-"), dest=k, type=typ, default=None,
                       help=f"default: {v}")
    r.add_argument("--experiment", default="adhoc")
    r.add_argument("--repeat", type=int, default=1)
    r.add_argument("--with-delayed", action="store_true", help="also sample the delayed replica")
    r.add_argument("--no-binlog", action="store_true", help="skip the commit-group analysis")
    r.set_defaults(fn=cmd_run)

    pl = sub.add_parser("plan", help="run an experiment plan")
    pl.add_argument("plan")
    pl.add_argument("--repeats", type=int)
    pl.add_argument("--force", action="store_true")
    pl.add_argument("--no-binlog", action="store_true")
    pl.set_defaults(fn=cmd_plan)

    sm = sub.add_parser("summarize")
    sm.add_argument("experiments", nargs="+")
    sm.set_defaults(fn=cmd_summarize)

    pt = sub.add_parser("plot")
    pt.add_argument("experiments", nargs="*")
    pt.set_defaults(fn=cmd_plot)

    sub.add_parser("report").set_defaults(fn=cmd_report)
    sub.add_parser("demo-binlog").set_defaults(fn=cmd_demo_binlog)
    sub.add_parser("demo-delayed").set_defaults(fn=cmd_demo_delayed)

    a = ap.parse_args(argv)
    a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
