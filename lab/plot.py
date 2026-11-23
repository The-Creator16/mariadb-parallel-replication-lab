"""Charts. Every chart is drawn from the stored samples.csv / summary.json of
an experiment, so `python -m lab plot` regenerates all of them without
touching a database.

Conventions: time on the x axis is seconds since the load started; the grey
band is the period the workload was running on the primary; every repeat is
drawn (thin), the median repeat is emphasised (thick).
"""
from __future__ import annotations

import json
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from . import analysis  # noqa: E402

SURFACE = "#fcfcfb"
INK, INK2, MUTED, GRID = "#0b0b0b", "#52514e", "#8a8985", "#e4e3df"
BLUE, ORANGE, AQUA, YELLOW = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
CATEGORICAL = [BLUE, ORANGE, AQUA, YELLOW]
ORDINAL = ["#86b6ef", "#5598e7", "#2a78d6", "#1c5cab", "#0d366b"]
LOAD_BAND = "#efeeea"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
    "axes.spines.top": False, "axes.spines.right": False, "font.size": 10,
    "axes.titlesize": 11, "axes.titleweight": "bold", "axes.titlecolor": INK,
    "legend.frameon": False, "lines.linewidth": 2, "figure.dpi": 110,
})


# ------------------------------------------------------------------ helpers
def _runs(exp_dir: Path) -> dict[str, list[Path]]:
    by: dict[str, list[Path]] = {}
    for d in sorted(exp_dir.iterdir()):
        if (d / "summary.json").exists():
            by.setdefault(d.name.rsplit("-r", 1)[0], []).append(d)
    return by


def _summary(d: Path) -> dict:
    return json.loads((d / "summary.json").read_text())


def _meta(d: Path) -> dict:
    return json.loads((d / "meta.json").read_text())


def _median_run(dirs: list[Path], metric: str = "total_s") -> Path:
    vals = sorted(dirs, key=lambda d: _summary(d).get(metric) or 1e9)
    return vals[len(vals) // 2]


def _series(d: Path, field: str = "hb_lag_s", server: str = "replica"):
    m = _meta(d)
    rows = analysis.load_samples(d, server)
    t0 = m["load_start"]
    return [r["t"] - t0 for r in rows if r[field] is not None], \
           [r[field] for r in rows if r[field] is not None], m


def _load_band(ax, m: dict, label: bool = False) -> None:
    ax.axvspan(0, m["load_end"] - m["load_start"], color=LOAD_BAND, zorder=0,
               label="workload running on the primary" if label else None)


def _save(fig, path: Path) -> Path:
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path


def _footer(fig, text: str) -> None:
    # A fixed physical gap (inches) below the figure, so short figures do not
    # collide with their x-axis label.
    fig.text(0.0, -0.12 / fig.get_figheight(), text, color=MUTED, fontsize=8, ha="left", va="top",
             wrap=True)


def _setting_line(exp_dir: Path) -> str:
    any_run = next(d for ds in _runs(exp_dir).values() for d in ds)
    m = _meta(any_run)
    return (f"MariaDB {m['replica']['version'].split('-')[0]}   |   "
            f"{m['host']['host_cpus']} vCPU host   |   {exp_dir.name}")


# ------------------------------------------------------------- chart kinds
def lag_small_multiples(exp_dir: Path, key_fn, title: str, fname: str,
                        order=None, cols: int = 4) -> Path:
    groups = _runs(exp_dir)
    keys = order or sorted(groups, key=key_fn)
    rows = (len(keys) + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(3.2 * cols, 2.4 * rows), sharex=True, sharey=True,
                             squeeze=False)
    for ax in axes.flat[len(keys):]:
        ax.set_visible(False)
    for ax, key in zip(axes.flat, keys):
        dirs = groups[key]
        med = _median_run(dirs)
        for d in dirs:
            t, y, m = _series(d)
            ax.plot(t, y, color=BLUE, lw=2.2 if d == med else 0.9, alpha=1 if d == med else 0.45)
        _load_band(ax, _meta(med))
        s = _summary(med)
        ax.set_title(key_fn(key, label=True), loc="left", fontsize=10)
        ax.text(0.98, 0.95, f"peak {s['peak_lag_s']:.1f}s\ncaught up {s['total_s']:.0f}s",
                transform=ax.transAxes, ha="right", va="top", fontsize=8, color=INK2)
    for ax in axes[-1]:
        ax.set_xlabel("seconds since load start")
    for ax in axes[:, 0]:
        ax.set_ylabel("heartbeat lag (s)")
    fig.suptitle(title, x=0.01, ha="left", fontweight="bold", color=INK)
    _footer(fig, "Thick line: median of the repeats (by time to catch up); thin: the other repeats. "
                 "Grey band: workload running on the primary.\n" + _setting_line(exp_dir))
    fig.tight_layout()
    return _save(fig, exp_dir / fname)


def lag_overlay(exp_dir: Path, keys: list[str], labels: list[str], title: str, fname: str,
                colors=None) -> Path:
    groups = _runs(exp_dir)
    colors = colors or ORDINAL
    fig, ax = plt.subplots(figsize=(8, 4))
    longest = None
    labelled = []
    for key, label, c in zip(keys, labels, colors):
        if key not in groups:
            continue
        med = _median_run(groups[key])
        t, y, m = _series(med)
        ax.plot(t, y, color=c, label=label)
        if longest is None or m["load_end"] - m["load_start"] > longest["load_end"] - longest["load_start"]:
            longest = m
        labelled.append((max(y), t[y.index(max(y))], label))
    if longest:
        _load_band(ax, longest, label=True)
    # Direct labels only where they cannot collide: peaks at least 8% of the
    # axis apart. The legend carries identity for the rest.
    top = max((p for p, _, _ in labelled), default=1)
    placed = []
    for peak, x, label in sorted(labelled, reverse=True):
        if all(abs(peak - q) > 0.08 * top for q in placed):
            ax.annotate(label, (x, peak), xytext=(4, 4), textcoords="offset points",
                        fontsize=8, color=INK2)
            placed.append(peak)
    ax.set_xlabel("seconds since load start")
    ax.set_ylabel("heartbeat lag (s)")
    ax.set_ylim(bottom=0)
    ax.legend(loc="upper left", fontsize=8)
    ax.set_title(title, loc="left")
    _footer(fig, "Median repeat by completion time. Shading covers the longest displayed load. " + _setting_line(exp_dir))
    return _save(fig, exp_dir / fname)


def metric_vs_threads(exp_dir: Path, series: list[tuple[str, callable, str]], metric: str,
                      ylabel: str, title: str, fname: str, ref=None) -> Path:
    """Dots = repeats, line = median, x = slave_parallel_threads (log-ish)."""
    sums = [_summary(d) for ds in _runs(exp_dir).values() for d in ds]
    fig, ax = plt.subplots(figsize=(8, 4.2))
    xs_all = sorted({s["threads"] for s in sums})
    pos = {t: i for i, t in enumerate(xs_all)}
    for (label, pred, color) in series:
        sel = [s for s in sums if pred(s) and s.get(metric) is not None]
        by = {}
        for s in sel:
            by.setdefault(s["threads"], []).append(s[metric])
        xs = sorted(by)
        ax.scatter([pos[x] for x in xs for _ in by[x]], [v for x in xs for v in by[x]],
                   s=22, color=color, alpha=0.55, edgecolors="none", zorder=3)
        meds = [statistics.median(by[x]) for x in xs]
        sparse = len(xs) < len(xs_all)
        if sparse:
            label += f" (measured at {', '.join(map(str, xs))} only)"
        ax.plot([pos[x] for x in xs], meds, color=color, marker="o", ms=6, label=label, zorder=4,
                markeredgecolor=SURFACE, markeredgewidth=1.5, ls="--" if sparse else "-")
        if xs:
            ax.annotate((f"{meds[-1]:.1f}" if metric.endswith("_s") else f"{meds[-1]:.0f}"), (pos[xs[-1]], meds[-1]), xytext=(6, 0),
                        textcoords="offset points", va="center", fontsize=8, color=INK2)
            best = max(meds) if metric.endswith("tps") else min(meds)
            bi = meds.index(best)
            if bi != len(meds) - 1:
                ax.annotate((f"{best:.1f}" if metric.endswith("_s") else f"{best:.0f}"), (pos[xs[bi]], best), xytext=(0, -14), ha="center",
                            textcoords="offset points", fontsize=8, color=INK2)
    if ref:
        ax.axhline(ref[0], color=MUTED, lw=1.2, ls="--", zorder=2)
        ax.text(0, ref[0], " " + ref[1], color=INK2, fontsize=8, ha="left", va="bottom")
    ax.set_xticks(range(len(xs_all)), [str(x) for x in xs_all])
    ax.set_xlabel("slave_parallel_threads  (0 = no workers: the SQL thread applies alone)")
    ax.set_ylabel(ylabel)
    ax.set_ylim(bottom=0)
    if len(series) > 1:
        ax.legend(loc="lower right", fontsize=8)
    ax.set_title(title, loc="left")
    _footer(fig, "Dots: individual repeats. Line: median; dashed where only some thread counts were measured. " + _setting_line(exp_dir))
    return _save(fig, exp_dir / fname)


def worker_states(exp_dir: Path, pred, title: str, fname: str) -> Path:
    """Where the worker threads spend their time, by thread count. Stacked
    horizontal bars, at most four named states + other."""
    sums = [_summary(d) for ds in _runs(exp_dir).values() for d in ds if pred(_summary(d))]
    by: dict[int, list[dict]] = {}
    for s in sums:
        if s["threads"] > 0 and s["worker_states"]:
            by.setdefault(s["threads"], []).append(s["worker_states"])
    named = {
        "idle (Waiting for work from SQL thread)": ["Waiting for work from SQL thread"],
        "waiting for prior commit": ["Waiting for prior transaction to commit",
                                     "Waiting for prior transaction to start commit"],
        "committing": ["Commit"],
    }
    labels = list(named) + ["applying row events / other"]
    colors = [GRID, ORANGE, YELLOW, BLUE]
    threads = sorted(by)
    shares = []
    for t in threads:
        agg: dict[str, float] = {}
        for ws in by[t]:
            for k, v in ws.items():
                agg[k] = agg.get(k, 0) + v / len(by[t])
        row = []
        for lab in named:
            row.append(sum(agg.get(s, 0) for s in named[lab]))
        row.append(max(0.0, 1 - sum(row)))
        shares.append(row)
    fig, ax = plt.subplots(figsize=(8, 0.45 * len(threads) + 1.6))
    for i, t in enumerate(threads):
        left = 0
        for j, v in enumerate(shares[i]):
            ax.barh(i, v, left=left, color=colors[j], height=0.7, edgecolor=SURFACE, linewidth=2,
                    label=labels[j] if i == 0 else None)
            if v > 0.07:
                ax.text(left + v / 2, i, f"{v:.0%}", ha="center", va="center", fontsize=8,
                        color=INK if j != 3 else SURFACE)
            left += v
    ax.set_yticks(range(len(threads)), [f"{t} workers" for t in threads])
    ax.invert_yaxis()
    ax.set_xlim(0, 1)
    ax.xaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0))
    ax.grid(axis="y", visible=False)
    ax.legend(loc="lower left", bbox_to_anchor=(0, 1.0), ncol=2, fontsize=8)
    ax.set_title(title, loc="left", pad=44)
    _footer(fig, "Share of worker-thread samples in each SHOW PROCESSLIST state during the apply "
                 "window (sampled once per second, averaged over repeats).\n" + _setting_line(exp_dir))
    return _save(fig, exp_dir / fname)


def lag_metrics(exp_dir: Path, key: str, fname: str) -> Path:
    """Four ways to say 'how far behind', for one run. Two panels, never two y-axes."""
    d = _median_run(_runs(exp_dir)[key])
    m = _meta(d)
    rows = analysis.load_samples(d)
    t = [r["t"] - m["load_start"] for r in rows]
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(8, 5.2), sharex=True,
                                 gridspec_kw={"height_ratios": [3, 2]})
    a1.plot(t, [r["hb_lag_s"] for r in rows], color=BLUE, label="heartbeat lag (NOW() - heartbeat.ts)")
    a1.plot(t, [r["sbm_s"] for r in rows], color=ORANGE, lw=1.6, label="Seconds_Behind_Master")
    if any(r["mstd_s"] is not None for r in rows):
        a1.plot(t, [r["mstd_s"] for r in rows], color=AQUA, lw=1.6, ls="--",
                label="Master_Slave_time_diff (11.6+)")
    _load_band(a1, m)
    a1.set_ylabel("seconds behind")
    a1.legend(loc="upper left", fontsize=8)
    a1.set_title(f"Four measures of the same lag: {d.name}", loc="left")
    a2.plot(t, [r["backlog_tx"] for r in rows], color=BLUE)
    _load_band(a2, m)
    a2.set_ylabel("backlog (transactions)")
    a2.set_xlabel("seconds since load start")
    a2.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1000:.0f}k"))
    _footer(fig, "Backlog = primary @@gtid_binlog_pos minus replica @@gtid_slave_pos. "
                 + _setting_line(exp_dir))
    fig.tight_layout()
    return _save(fig, exp_dir / fname)


def paired_bars(exp_dir: Path, items: list[tuple[str, str, str]], metric: str, xlabel: str,
                title: str, fname: str, ref_lines: dict | None = None, note: str = "") -> Path:
    """Horizontal bars, one per (label, key); median with min-max whisker.
    items: (row label, config key, color)."""
    groups = _runs(exp_dir)
    fig, ax = plt.subplots(figsize=(8.5, 0.42 * len(items) + 1.5))
    for i, (label, key, color) in enumerate(items):
        if key not in groups:
            continue
        vals = [_summary(d)[metric] for d in groups[key] if _summary(d).get(metric) is not None]
        if not vals:
            continue
        med = statistics.median(vals)
        ax.barh(i, med, color=color, height=0.68, edgecolor=SURFACE, linewidth=2)
        ax.plot([min(vals), max(vals)], [i, i], color=INK, lw=1)
        ax.text(max(vals), i, (f"  {med:.1f}" if metric.endswith("_s") else f"  {med:,.0f}"), va="center", fontsize=8, color=INK2)
    ax.set_yticks(range(len(items)), [it[0] for it in items])
    ax.invert_yaxis()
    ax.grid(axis="y", visible=False)
    for x, lab in (ref_lines or {}).items():
        ax.axvline(x, color=MUTED, ls="--", lw=1.1)
        ax.text(x, -0.7, lab, fontsize=8, color=INK2, ha="center")
    ax.set_xlabel(xlabel)
    ax.set_title(title, loc="left", pad=16)
    _footer(fig, "Bar: median of repeats; whisker: min-max. " + note + _setting_line(exp_dir))
    return _save(fig, exp_dir / fname)


def group_histograms(exp_dir: Path, items: list[tuple[str, str]], fname: str) -> Path:
    """Distribution of binlog commit-group sizes: what conservative mode can see."""
    groups = _runs(exp_dir)
    fig, grid = plt.subplots(1, len(items), squeeze=False, figsize=(3.0 * len(items), 2.6), sharey=True)
    axes = grid[0]
    for ax, (label, key) in zip(axes, items):
        if key not in groups or not (groups[key][0] / "groups.json").exists():
            ax.set_visible(False)
            continue
        g = json.loads((groups[key][0] / "groups.json").read_text())
        hist = {int(k): v for k, v in g["size_histogram"].items()}
        buckets = {"1": 0, "2-4": 0, "5-8": 0, "9-16": 0, "17+": 0}
        for size, n in hist.items():
            tx = size * n
            b = "1" if size == 1 else "2-4" if size <= 4 else "5-8" if size <= 8 else \
                "9-16" if size <= 16 else "17+"
            buckets[b] += tx
        total = sum(buckets.values())
        if not total:
            ax.set_visible(False)
            continue
        ax.bar(range(5), [v / total for v in buckets.values()], color=BLUE, width=0.72,
               edgecolor=SURFACE, linewidth=2)
        ax.set_xticks(range(5), list(buckets))
        ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0))
        waited = g["flags"].get("waited", 0) / g["transactions"]
        ax.set_title(label, loc="left", fontsize=10)
        ax.text(0.98, 0.95, f"mean group {g['mean_group_size']}\n'waited' flag {waited:.0%}",
                transform=ax.transAxes, ha="right", va="top", fontsize=8, color=INK2)
        ax.set_xlabel("commit group size")
    axes[0].set_ylabel("share of transactions")
    fig.suptitle("How the primary committed: binlog group-commit sizes (cid= in mariadb-binlog)",
                 x=0.01, ha="left", fontweight="bold", color=INK)
    _footer(fig, "Transactions in one group share a commit id and are what conservative mode applies "
                 "in parallel. First repeat of each workload. " + _setting_line(exp_dir))
    fig.tight_layout()
    return _save(fig, exp_dir / fname)


# ---------------------------------------------------------- per experiment
def _threads_key(key: str, label: bool = False):
    t = int(key.split("-t")[1][:2])
    return (f"{t} workers" if t else "single-threaded (0)") if label else t


def plot_experiment(exp_dir: Path) -> list[Path]:
    name = exp_dir.name
    out: list[Path] = []
    groups = _runs(exp_dir)
    if not groups:
        return out
    if name.startswith("1_scaling"):
        out.append(lag_small_multiples(exp_dir, _threads_key,
                   "Replication lag over time, by slave_parallel_threads (independent workload, live)",
                   "lag_by_threads.png"))
        pick = [k for k in sorted(groups, key=_threads_key) if _threads_key(k) in (0, 2, 4, 16, 64)]
        out.append(lag_overlay(exp_dir, pick, [_threads_key(k, True) for k in pick],
                   "Lag over time: more workers, less lag -- up to a point", "lag_overlay.png"))
        out.append(metric_vs_threads(exp_dir, [("peak lag", lambda s: True, BLUE)], "peak_lag_s",
                   "peak heartbeat lag (s)", "Peak lag vs. workers", "peak_lag_vs_threads.png"))
        out.append(worker_states(exp_dir, lambda s: True,
                   "What the parallel workers are doing", "worker_states.png"))
    elif name.startswith("2_capacity"):
        sums = [_summary(d) for ds in groups.values() for d in ds]
        prim = statistics.median(s["primary_tps"] for s in sums)
        out.append(metric_vs_threads(
            exp_dir,
            [("replica CPU limit 1", lambda s: str(s["replica_cpus"]) in ("1", "1.0"), BLUE),
             ("replica CPU limit 2", lambda s: str(s["replica_cpus"]) in ("2", "2.0"), ORANGE)],
            "replica_tps", "replica apply rate with a backlog (tx/s)",
            "Replica apply capacity vs. workers: where more threads stop helping",
            "capacity_vs_threads.png", ref=(prim, f"primary commit rate {prim:.0f} tx/s ")))
        out.append(worker_states(exp_dir, lambda s: str(s["replica_cpus"]) in ("1", "1.0"),
                   "Capacity runs: what the parallel workers are doing", "worker_states.png"))
    elif name.startswith("3_modes"):
        for wl in ("independent", "hotspot"):
            items = []
            for label, mode, extra in (("single-threaded", "optimistic", "-t00"),
                                       ("minimal", "minimal", "-t16"),
                                       ("conservative", "conservative", "-t16"),
                                       ("conservative + commit wait", "conservative", "-t16-gc"),
                                       ("optimistic (default)", "optimistic", "-t16"),
                                       ("aggressive", "aggressive", "-t16")):
                key = next((k for k in groups if k.startswith(f"{wl}-{mode}{extra}")
                            and ("-gc" in k) == ("-gc" in extra)), None)
                if key:
                    items.append((label, key, MUTED if extra == "-t00" else BLUE))
            base = [k for _, k, _ in items if "-t00" in k]
            ref = {}
            if base:
                v = statistics.median(_summary(d)["replica_tps"] for d in groups[base[0]])
                ref = {v: "single-threaded"}
            out.append(paired_bars(exp_dir, items, "replica_tps", "replica apply rate (tx/s)",
                       f"Parallel modes, 16 workers: {wl} workload", f"modes_{wl}.png", ref,
                       note="Catch-up mode (fixed backlog, primary idle). "
                            "commit wait = binlog_commit_wait_count=16, _usec=10000 on the primary. "))
            # retries next to it: conflicts are the price of speculation
            out.append(paired_bars(exp_dir, [(l, k, ORANGE) for l, k, _ in items if "-t00" not in k],
                       "retried_tx", "Slave_retried_transactions during the run",
                       f"Rolled back and retried: {wl} workload", f"retries_{wl}.png"))
    elif name.startswith("4_workloads"):
        order = ["independent", "hotspot", "serial", "own_innodb", "own_myisam"]
        keys = []
        for wl in order:
            keys += sorted([k for k in groups if k.startswith(wl + "-")], key=_threads_key)
        lab = lambda k, label=False: (f"{k.split('-')[0]} - " + _threads_key(k, True)) if label else keys.index(k)
        out.append(lag_small_multiples(exp_dir, lab, "Lag over time by workload: single-threaded vs 16 workers",
                   "lag_by_workload.png", order=keys, cols=2))
        items = []
        for wl in order:
            for k in sorted([k for k in groups if k.startswith(wl + "-")], key=_threads_key):
                t = _threads_key(k)
                items.append((f"{wl}, {_threads_key(k, True)}", k, ORANGE if t == 0 else BLUE))
        out.append(paired_bars(exp_dir, items, "peak_lag_s", "peak heartbeat lag (s)",
                   "Peak lag by workload (orange: single-threaded, blue: 16 workers)",
                   "peak_lag_by_workload.png"))
        out.append(paired_bars(exp_dir, items, "replica_tps", "replica rate (tx/s, live workload)",
                   "Replica throughput by workload (orange: single-threaded, blue: 16 workers)",
                   "replica_tps_by_workload.png"))
        hist_items = [(wl, next(k for k in groups if k.startswith(wl + "-") and _threads_key(k) == 0))
                      for wl in order if any(k.startswith(wl + "-") for k in groups)]
        out.append(group_histograms(exp_dir, hist_items, "commit_groups.png"))
        k0 = next((k for k in groups if k.startswith("independent-") and _threads_key(k) == 0), None)
        if k0:
            out.append(lag_metrics(exp_dir, k0, "lag_metrics.png"))
    elif name.startswith("5_ddl"):
        fig, axes = plt.subplots(1, 2, figsize=(11, 3.8), sharey=True)
        for ax, t in zip(axes, (0, 16)):
            for wl, color, label in (("ddl", ORANGE, "ALTER in domain 0 (same as all DML)"),
                                     ("ddl_domain", BLUE, "ALTER in gtid_domain_id=1")):
                key = next((k for k in groups if k.startswith(wl + "-") and _threads_key(k) == t), None)
                if not key:
                    continue
                med = _median_run(groups[key])
                tt, y, m = _series(med)
                ax.plot(tt, y, color=color, label=label)
                ev = {e["event"]: e["t"] - m["load_start"] for e in m["events"]}
                if wl == "ddl":
                    _load_band(ax, m)
                    ax.axvspan(ev["ddl_start"], ev["ddl_end_on_primary"], color=YELLOW, alpha=0.18,
                               zorder=0, label="ALTER running on the primary")
            ax.set_title(_threads_key(f"x-t{t:02d}", True), loc="left")
            ax.set_xlabel("seconds since load start")
            ax.set_ylim(bottom=0)
        axes[0].set_ylabel("heartbeat lag (s)")
        axes[1].legend(loc="upper right", fontsize=8)
        fig.suptitle("A 10-13 s ALTER TABLE ... ALGORITHM=COPY under write load",
                     x=0.01, ha="left", fontweight="bold", color=INK)
        _footer(fig, "Median repeat. Yellow: the ALTER running on the primary; it reaches the replica "
                     "after completion on the primary. Shading: domain 0 run. " + _setting_line(exp_dir))
        fig.tight_layout()
        out.append(_save(fig, exp_dir / "ddl_lag.png"))
        items = [(f"{wl}, {_threads_key(k, True)}", k, ORANGE if wl == "ddl" else BLUE)
                 for wl in ("ddl", "ddl_domain")
                 for k in sorted([k for k in groups if k.startswith(wl + "-")], key=_threads_key)]
        out.append(paired_bars(exp_dir, items, "peak_lag_s", "peak heartbeat lag (s)",
                   "Peak lag with a long ALTER (orange: domain 0, blue: own domain)", "ddl_peak_lag.png"))
    elif name.startswith("6_durability"):
        items = []
        for k in sorted(groups, key=lambda k: (_threads_key(k), "durab" in k)):
            relaxed = "durab" in k
            items.append((f"{_threads_key(k, True)}, " + ("sync_binlog=0, flush_log=2" if relaxed
                          else "sync_binlog=1, flush_log=1"), k, AQUA if relaxed else BLUE))
        out.append(paired_bars(exp_dir, items, "replica_tps", "replica apply rate (tx/s)",
                   "Control: replica capacity with full vs relaxed durability", "durability.png",
                   note="Catch-up mode. Blue: durable replica; aqua: relaxed (unsafe on crash). "))
    else:
        out.append(lag_overlay(exp_dir, sorted(groups), sorted(groups), f"{name}: lag over time",
                   "lag.png", colors=CATEGORICAL * 4))
    return out
