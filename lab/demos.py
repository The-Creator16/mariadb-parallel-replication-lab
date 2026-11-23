"""Two guided demos that print every statement they run.

demo-binlog   What is flowing: annotated row events, commit ids (cid=) of
              group commits, CRC32 checksums -- and a checksum catching a
              corrupted event.
demo-delayed  A replica that lags on purpose: watch a DROP TABLE reach the
              normal replica at once, find it in the binlog, and stop the
              delayed replica just before it to get the table back.
"""
from __future__ import annotations

import re
import subprocess
import threading
import time

from . import db, topology
from .binlog import mariadb_binlog
from .workload import Heartbeat


def _h(title: str) -> None:
    print("\n" + "=" * 78 + f"\n{title}\n" + "=" * 78)


def _sql(conn, server: str, sql: str, show_result: bool = True):
    print(f"[{server}]> {sql}")
    rows = db.q(conn, sql)
    if show_result and rows:
        for r in rows[:12]:
            print("    " + " | ".join("NULL" if v is None else str(v) for v in r))
    return rows


def _sh(cmd: str, service: str = "primary") -> str:
    print(f"$ docker compose exec {service} sh -c \"{cmd}\"")
    res = subprocess.run(["docker", "compose", "exec", "-T", service, "sh", "-c", cmd],
                         cwd=db.ROOT, capture_output=True, text=True)
    return res.stdout + res.stderr


# ===================================================================== binlog
def binlog_demo() -> None:
    p = db.connect("primary")
    r = db.connect("replica")
    topology.reset_data(accounts=1000, archive_rows=10, own_tables=1)

    _h("1. The settings that make the binlog legible")
    _sql(p, "primary", "SELECT @@binlog_format, @@binlog_annotate_row_events, @@binlog_checksum, "
                       "@@master_verify_checksum")
    _sql(r, "replica", "SELECT @@replicate_annotate_row_events, @@slave_sql_verify_checksum, "
                       "@@log_slave_updates")

    _h("2. One transaction, annotated")
    start = db.q(p, "SHOW MASTER STATUS")[0][:2]
    _sql(p, "primary", "UPDATE lab.accounts SET balance = balance - 10 WHERE id IN (1, 2, 3)")
    _sql(p, "primary", "SELECT @@last_gtid")
    mid = db.q(p, "SHOW MASTER STATUS")[0][:2]
    out = mariadb_binlog([f"--start-position={start[1]}", f"--stop-position={mid[1]}",
                          "--base64-output=decode-rows", "--verbose", f"/var/lib/mysql/{start[0]}"])
    print("$ mariadb-binlog --base64-output=decode-rows --verbose ...  (excerpt)")
    keep = [ln for ln in out.splitlines()
            if re.search(r"GTID|Annotate_rows|#Q>|Table_map|Update_rows|### |Xid|CRC32", ln)]
    for ln in keep[:40]:
        print("    " + ln)
    print("""
  Read it top to bottom:
    GTID 0-1-N trans     the transaction's id (domain-server-sequence); 'trans' = transactional
    Annotate_rows / #Q>  the SQL statement that produced the row events below it. Without
                         binlog_annotate_row_events you would only see the rows.
    Table_map            which table the following row events refer to
    Update_rows + ###    the row images: what the replica will apply. The replica never
                         re-runs the UPDATE; it applies these before/after images.
    CRC32 0x...          every event header carries its checksum (binlog_checksum=CRC32)""")

    _h("3. The same statement with annotation switched off for this session")
    _sql(p, "primary", "SET SESSION binlog_annotate_row_events = OFF")
    _sql(p, "primary", "UPDATE lab.accounts SET balance = balance - 10 WHERE id IN (4, 5, 6)")
    _sql(p, "primary", "SET SESSION binlog_annotate_row_events = ON")
    mid2 = db.q(p, "SHOW MASTER STATUS")[0][:2]
    out = mariadb_binlog([f"--start-position={mid[1]}", f"--stop-position={mid2[1]}",
                          "--base64-output=decode-rows", "--verbose", f"/var/lib/mysql/{mid[0]}"])
    for ln in [ln for ln in out.splitlines() if re.search(r"GTID|#Q>|Update_rows|### UPDATE", ln)][:6]:
        print("    " + ln)
    print("  -> no #Q> line: the rows are there, the statement that changed them is not.")

    _h("4. Group commit, as written in the binlog: cid=")
    print("  8 clients commit 50 transactions each, concurrently ...")
    threads = []
    for c in range(8):
        def go(c=c):
            conn = db.connect("primary")
            db.q(conn, "CALL labctl.wl_independent(%s, 8, 50, 2, 1000)", (c,))
            conn.close()
        threads.append(threading.Thread(target=go))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    end = db.q(p, "SHOW MASTER STATUS")[0][:2]
    out = mariadb_binlog([f"--start-position={mid2[1]}", f"--stop-position={end[1]}",
                          "--base64-output=decode-rows", f"/var/lib/mysql/{mid2[0]}"],
                         grep="GTID [0-9]+-")
    lines = [re.sub(r"^#\S+ \S+\s+server id \d+\s+end_log_pos \d+ ", "", ln) for ln in out.splitlines()]
    for ln in lines[20:32]:
        print("    " + ln)
    print("""  -> GTIDs that share a cid= were flushed to the binlog in one group commit: they were
     committing at the same moment on the primary, so none waited for another and the replica
     may apply them in parallel (that is all slave_parallel_mode=conservative relies on).""")

    _h("5. Checksums: verify a binlog, then corrupt one byte of a copy")
    f = end[0]
    print(_sh(f"mariadb-binlog --verify-binlog-checksum /var/lib/mysql/{f} > /dev/null && echo 'all events: checksum OK'"))
    # find an Update_rows event of step 2 and flip a byte in its body -- in a COPY.
    out = mariadb_binlog(["--base64-output=decode-rows", f"/var/lib/mysql/{start[0]}"])
    pos = None
    prev_at = None
    for ln in out.splitlines():
        m = re.match(r"# at (\d+)", ln)
        if m:
            prev_at = int(m.group(1))
        if "Update_rows" in ln and prev_at and prev_at >= int(start[1]):
            pos = prev_at
            break
    if pos is None:
        raise RuntimeError("No Update_rows event found in the selected binlog")
    off = pos + 30
    cmd = (f"cp /var/lib/mysql/{start[0]} /tmp/corrupt.bin && "
           f"b=$(od -An -tu1 -j {off} -N1 /tmp/corrupt.bin | tr -d ' ') && "
           f"if [ \"$b\" = 0 ]; then v='\\001'; else v='\\000'; fi && "
           f"printf \"$v\" | dd of=/tmp/corrupt.bin bs=1 seek={off} conv=notrunc 2>/dev/null && "
           f"echo \"events in the copy, not verified: $(mariadb-binlog /tmp/corrupt.bin 2>/dev/null | grep -c '^# at')\" && "
           f"echo \"events in the copy, verified:     $(mariadb-binlog --verify-binlog-checksum /tmp/corrupt.bin 2>/dev/null | grep -c '^# at')\" && "
           f"mariadb-binlog --verify-binlog-checksum /tmp/corrupt.bin > /dev/null 2>/tmp/err; "
           f"echo \"exit status: $?\"; cat /tmp/err")
    print(f"  (copy of {start[0]}: one byte inside the Update_rows event at position {pos} changed)")
    print("    " + _sh(cmd).strip().replace("\n", "\n    "))
    print("""  -> Without verification mariadb-binlog happily decodes the damaged event. With
     --verify-binlog-checksum it stops right before it and reports the CRC failure.
     Note the exit status: 0 (observed on 11.8.6). A script that checks a binlog
     must look at stderr, not at $?.

     A replica does not have that problem: with
     master_verify_checksum the primary checks events as it reads them from disk, the IO
     thread checks what arrives over the network, and slave_sql_verify_checksum makes the SQL
     thread check what it reads back from the relay log. A flipped bit stops replication with
     an error instead of being applied silently.""")
    p.close()
    r.close()


# ==================================================================== delayed
def _applied(conn, gtid: str) -> bool:
    """Has this replica applied `gtid` (domain-server-seq)?"""
    dom, _srv, seq = gtid.split("-")
    have = db.parse_gtid_pos(db.q1(conn, "SELECT @@gtid_slave_pos"))
    return have.get(int(dom), 0) >= int(seq)


def _wait_applied(conn, gtid: str, label: str) -> None:
    # The same wait in one statement would be:
    #   SELECT MASTER_GTID_WAIT('<gtid>', <timeout seconds>);
    # we poll instead, to print the countdown.
    deadline = time.monotonic() + int(db.slave_status(conn).get("SQL_Delay") or 0) + 120
    while not _applied(conn, gtid):
        if time.monotonic() >= deadline:
            raise TimeoutError(f"{label} did not reach {gtid}")
        s = db.slave_status(conn)
        print(f"    {label}: SQL_Remaining_Delay={s['SQL_Remaining_Delay']}  "
              f"state: {s['Slave_SQL_Running_State']}", flush=True)
        time.sleep(5)


def delayed_demo() -> None:
    if not db.is_up("delayed"):
        raise SystemExit("The delayed replica is not running. Start it with:\n"
                         "  make up-delayed setup")
    p, r, d = db.connect("primary"), db.connect("replica"), db.connect("delayed")
    st = db.slave_status(d)
    delay = int(st.get("SQL_Delay") or 0)
    if not delay:
        raise SystemExit("delayed has no MASTER_DELAY; run `python -m lab setup` with it running.")
    hb = Heartbeat()
    hb.start()
    print(f"warming up: the delayed replica first has to reach this demo's heartbeats (~{delay}s) ...")
    time.sleep(0.5)   # let the first heartbeat commit, so the position below includes it
    _wait_applied(d, db.q1(p, "SELECT @@gtid_binlog_pos").split(",")[0], "delayed")

    from pathlib import Path
    from .sampler import Sampler
    out_dir = db.ROOT / "results" / "demo_delayed"
    out_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    sampler = Sampler(out_dir / "samples.csv", ["replica", "delayed"], t0, 0.5)
    sampler.start()
    events = []

    def ev(name):
        events.append({"t": time.time() - t0, "event": name})

    _h(f"1. A replica that lags on purpose: MASTER_DELAY={delay}s")
    _sql(d, "delayed", "SELECT Connection_name, Master_Host FROM information_schema.SLAVE_STATUS")
    print(f"    SQL_Delay={st['SQL_Delay']}  SQL_Remaining_Delay={st['SQL_Remaining_Delay']}  "
          f"Seconds_Behind_Master={st['Seconds_Behind_Master']}")

    _h("2. Precious data is written on the primary")
    _sql(p, "primary", "CREATE DATABASE IF NOT EXISTS shop")
    _sql(p, "primary", "DROP TABLE IF EXISTS shop.customers")
    _sql(p, "primary", "CREATE TABLE shop.customers (id INT PRIMARY KEY, name VARCHAR(40)) ENGINE=InnoDB")
    _sql(p, "primary", "INSERT INTO shop.customers VALUES (1,'Ada'),(2,'Grace'),(3,'Edsger'),(4,'Barbara'),(5,'Donald')")
    ev("customers created")
    created = _sql(p, "primary", "SELECT @@last_gtid")[0][0]
    print(f"  waiting until the delayed replica has applied {created} "
          f"(one statement would do: SELECT MASTER_GTID_WAIT('{created}', 120)) ...")
    _wait_applied(d, created, "delayed")
    _sql(d, "delayed", "SELECT COUNT(*) FROM shop.customers")

    _h("3. The accident")
    _sql(p, "primary", "DROP TABLE shop.customers")
    drop_gtid = _sql(p, "primary", "SELECT @@last_gtid")[0][0]
    ev("DROP TABLE on primary")
    time.sleep(1)
    _sql(r, "replica", "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='shop'")
    print("  -> the normal replica applied the DROP within a second. The delayed one still has the table:")
    _sql(d, "delayed", "SELECT COUNT(*) FROM shop.customers")

    _h("4. Find the statement in the binlog: GTID events make every transaction addressable")
    f = db.q(p, "SHOW MASTER STATUS")[0][0]
    out = mariadb_binlog([f"/var/lib/mysql/{f}"], grep="GTID [0-9]+-|DROP TABLE")
    lines = out.splitlines()
    print(f"$ mariadb-binlog /var/lib/mysql/{f} | grep -E 'GTID [0-9]+-|DROP TABLE'   (the match)")
    for i, ln in enumerate(lines):
        if f"GTID {drop_gtid} " in ln and i + 1 < len(lines):
            print("    " + ln.strip())
            print("    " + lines[i + 1].strip())
    dom, srv, seq = drop_gtid.split("-")
    before = f"{dom}-{srv}-{int(seq) - 1}"
    print(f"  -> the DROP is GTID {drop_gtid}; we want everything up to {before} and nothing after.")

    _h("5. Stop the delayed replica just before it")
    _sql(d, "delayed", "STOP SLAVE")
    _sql(d, "delayed", f"START SLAVE UNTIL master_gtid_pos = '{before}'")
    ev("START SLAVE UNTIL on delayed")
    deadline = time.monotonic() + delay + 120
    while db.slave_status(d)["Slave_SQL_Running"] != "No":
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Delayed replica did not stop at {before}")
        s_ = db.slave_status(d)
        print(f"    delayed: SQL_Remaining_Delay={s_['SQL_Remaining_Delay']}  "
              f"gtid_slave_pos={db.q1(d, 'SELECT @@gtid_slave_pos')}", flush=True)
        time.sleep(5)
    if not _applied(d, before) or _applied(d, drop_gtid) or db.slave_status(d).get("Last_SQL_Errno"):
        raise RuntimeError("Delayed replica stopped outside the recovery position or with an error")
    ev("delayed stopped just before the DROP")
    _sql(d, "delayed", "SELECT @@gtid_slave_pos")
    _sql(d, "delayed", "SELECT * FROM shop.customers")

    _h("6. Copy the rows back to the primary (they replicate to everyone again)")
    ddl = db.q(d, "SHOW CREATE TABLE shop.customers")[0][1]
    _sql(p, "primary", ddl.replace("CREATE TABLE `customers`", "CREATE TABLE shop.customers"))
    rows = db.q(d, "SELECT id, name FROM shop.customers")
    with p.cursor() as cur:
        cur.executemany("INSERT INTO shop.customers VALUES (%s, %s)", rows)
    print(f"[primary]> INSERT INTO shop.customers VALUES ... ({len(rows)} rows from delayed)")
    _sql(p, "primary", "SELECT COUNT(*) FROM shop.customers")
    _sql(d, "delayed", "START SLAVE")
    ev("rows restored; delayed restarted")
    print("  -> the delayed replica resumes; in ~delay seconds it replays the DROP and then the restore.")
    time.sleep(5)
    sampler.stop()
    hb.stop()

    if sampler.error:
        raise sampler.error
    for conn in (p, r, d):
        conn.close()

    import json
    (out_dir / "events.json").write_text(json.dumps(events, indent=2))
    (out_dir / "settings.json").write_text(json.dumps({"delay_seconds": delay}, indent=2))
    plot_delayed(out_dir, events, delay)
    print(f"\nchart: {(out_dir / 'lag.png').relative_to(db.ROOT)}")


def plot_delayed(out_dir, events, delay) -> None:
    from . import analysis, plot
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(8, 3.6))
    for name, color, label in (("replica", plot.BLUE, "replica (no delay)"),
                               ("delayed", plot.ORANGE, f"delayed (MASTER_DELAY={delay})")):
        rows = analysis.load_samples(out_dir, name)
        ax.plot([r["t"] for r in rows], [r["hb_lag_s"] for r in rows], color=color, label=label)
    top = max(r["hb_lag_s"] for r in analysis.load_samples(out_dir, "delayed")) * 1.25
    ax.set_ylim(0, top)
    for i, e in enumerate(events):
        ax.axvline(e["t"], color=plot.MUTED, lw=0.8, ls=":")
        ax.annotate(f"{i + 1}", (e["t"], top * (0.94 - 0.08 * (i % 2))), ha="center", va="center",
                    fontsize=8, color=plot.INK,
                    bbox=dict(boxstyle="circle,pad=0.25", fc=plot.SURFACE, ec=plot.MUTED, lw=0.8))
    key = "   ".join(f"{i + 1} {e['event']}" for i, e in enumerate(events))
    plot._footer(fig, key)
    ax.set_xlabel("seconds since demo start")
    ax.set_ylabel("heartbeat lag (s)")
    ax.legend(loc="center right", fontsize=8)
    ax.set_title("Deliberate lag: the delayed replica during the rescue demo", loc="left")
    fig.savefig(out_dir / "lag.png", bbox_inches="tight")
    plt.close(fig)
