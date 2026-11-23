"""Connections, SQL-file execution and GTID arithmetic.

Everything the harness knows about the servers comes through here.
"""
from __future__ import annotations

import os
import re
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = ROOT / "sql"

# docker compose reads .env by itself; read the same file here so the
# harness and the containers always agree on ports and passwords. Variables
# already set in the environment win, as they do for compose.
if (ROOT / ".env").exists():
    for _line in (ROOT / ".env").read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

PASSWORD = os.environ.get("LAB_ROOT_PASSWORD", "lab")
HOST = os.environ.get("LAB_HOST", "127.0.0.1")
PORTS = {
    "primary": int(os.environ.get("PRIMARY_PORT", 3307)),
    "replica": int(os.environ.get("REPLICA_PORT", 3308)),
    "delayed": int(os.environ.get("DELAYED_PORT", 3309)),
}
# Inside the compose network the servers reach each other by service name.
PRIMARY_INTERNAL_HOST = os.environ.get("PRIMARY_INTERNAL_HOST", "primary")
REPL_USER = "repl"
REPL_PASSWORD = os.environ.get("LAB_REPL_PASSWORD", "repl")


def connect(server: str, dict_cursor: bool = False, autocommit: bool = True, **kw):
    import pymysql.cursors
    return pymysql.connect(
        host=HOST, port=PORTS[server], user="root", password=PASSWORD,
        autocommit=autocommit, charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor if dict_cursor else pymysql.cursors.Cursor,
        connect_timeout=10, **kw)


def is_up(server: str) -> bool:
    import pymysql
    try:
        connect(server).close()
        return True
    except pymysql.err.OperationalError:
        return False


def wait_up(server: str, timeout: float = 180) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if is_up(server):
            return
        time.sleep(1)
    raise SystemExit(f"{server} is not reachable on {HOST}:{PORTS[server]} -- "
                     f"is `docker compose up -d --wait` done?")


def q(conn, sql: str, args=None):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall() if cur.description else None


def q1(conn, sql: str, args=None):
    rows = q(conn, sql, args)
    return rows[0][0] if rows and not isinstance(rows[0], dict) else (rows[0] if rows else None)


def split_sql(text: str) -> list[str]:
    """Split a .sql file into statements, honouring `DELIMITER` lines the way
    the mariadb command-line client does."""
    stmts, buf, delim = [], [], ";"
    for line in text.splitlines():
        m = re.match(r"^\s*DELIMITER\s+(\S+)\s*$", line, re.I)
        if m:
            delim = m.group(1)
            continue
        if not buf and (line.strip().startswith("--") or not line.strip()):
            continue
        buf.append(line)
        joined = "\n".join(buf).rstrip()
        if joined.endswith(delim):
            stmt = joined[: -len(delim)].strip()
            if stmt:
                stmts.append(stmt)
            buf = []
    tail = "\n".join(buf).strip()
    if tail:
        stmts.append(tail)
    return stmts


def run_sql_file(conn, name: str, **params) -> None:
    text = (SQL_DIR / name).read_text()
    for k, v in params.items():
        text = text.replace("{" + k + "}", str(v))
    for stmt in split_sql(text):
        q(conn, stmt)


# A GTID position carries one sequence number per domain.
def parse_gtid_pos(pos: str | None) -> dict[int, int]:
    out: dict[int, int] = {}
    for part in (pos or "").replace("\n", "").split(","):
        part = part.strip()
        if not part:
            continue
        domain, _server, seq = part.split("-")
        out[int(domain)] = max(out.get(int(domain), 0), int(seq))
    return out


def gtid_backlog(primary_pos: dict[int, int], replica_pos: dict[int, int]) -> int:
    return sum(max(0, seq - replica_pos.get(d, 0)) for d, seq in primary_pos.items())


def slave_status(conn) -> dict:
    import pymysql.cursors
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("SHOW SLAVE STATUS")
        row = cur.fetchone()
    return row or {}


def global_status(conn, names: list[str]) -> dict[str, int]:
    placeholders = ",".join(["%s"] * len(names))
    rows = q(conn, f"SHOW GLOBAL STATUS WHERE Variable_name IN ({placeholders})", names)
    out = {}
    for name, val in rows:
        try:
            out[name] = int(val)
        except ValueError:
            out[name] = val
    return out


def global_vars(conn, names: list[str]) -> dict[str, str]:
    placeholders = ",".join(["%s"] * len(names))
    rows = q(conn, f"SHOW GLOBAL VARIABLES WHERE Variable_name IN ({placeholders})", names)
    return {k: v for k, v in rows}


def wait_caught_up(replica: str = "replica", timeout: float = 900, quiet: bool = True) -> float:
    """Block until `replica` has applied everything the primary has binlogged.
    Returns seconds waited."""
    t0 = time.time()
    p = connect("primary")
    r = connect(replica)
    try:
        target = parse_gtid_pos(q1(p, "SELECT @@gtid_binlog_pos"))
        while True:
            have = parse_gtid_pos(q1(r, "SELECT @@gtid_slave_pos"))
            if gtid_backlog(target, have) == 0:
                return time.time() - t0
            st = slave_status(r)
            if st.get("Slave_SQL_Running") == "No" or st.get("Last_SQL_Error"):
                raise RuntimeError(f"{replica} SQL thread stopped: {st.get('Last_SQL_Error')}")
            if time.time() - t0 > timeout:
                raise TimeoutError(f"{replica} did not catch up within {timeout}s")
            if not quiet:
                print(f"  waiting for {replica}: {gtid_backlog(target, have)} transactions behind")
            time.sleep(0.5)
    finally:
        p.close()
        r.close()
