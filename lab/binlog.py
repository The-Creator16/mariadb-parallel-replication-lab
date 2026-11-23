"""Read what the primary actually wrote.

Every transaction in a MariaDB binlog starts with a GTID event. When several
transactions were flushed to the binlog in one group commit, their GTID
events carry the same commit id:

    #...  GTID 0-1-5012 cid=88210 trans
    #...  GTID 0-1-5013 cid=88210 trans
    #...  GTID 0-1-5014 cid=88210 trans     <- one group of 3
    #...  GTID 0-1-5015 trans               <- committed alone: no cid

Transactions in one group were committing at the same moment on the
primary, so none of them waited for another: they cannot conflict, and
`slave_parallel_mode=conservative` can schedule them together. The next group can overlap the commit stage. The
group-size distribution of a run is therefore the parallelism that
conservative mode can see. The flags at the end of the line say more:
`trans` (transactional), `ddl`, and `waited` (the transaction waited for a
row lock on the primary -- optimistic mode will not run it in parallel).
"""
from __future__ import annotations

import collections
import re
import subprocess

from . import db

GTID_RE = re.compile(r"GTID (\d+)-(\d+)-(\d+)(?: cid=(\d+))?(.*)$")


def mariadb_binlog(args: list[str], grep: str | None = None) -> str:
    """Run mariadb-binlog inside the primary container."""
    cmd = "mariadb-binlog " + " ".join(args)
    if grep:
        cmd += f" | grep -E '{grep}'"
    res = subprocess.run(["docker", "compose", "exec", "-T", "primary", "sh", "-c", cmd],
                         cwd=db.ROOT, capture_output=True, text=True, timeout=600)
    if res.returncode not in (0, 1):  # grep exits 1 on no match
        raise RuntimeError(res.stderr.strip())
    return res.stdout


def binlog_files_between(start_file: str, end_file: str) -> list[str]:
    p = db.connect("primary")
    files = [r[0] for r in db.q(p, "SHOW BINARY LOGS")]
    p.close()
    return files[files.index(start_file): files.index(end_file) + 1]


def commit_groups(start: tuple, end: tuple) -> dict:
    files = binlog_files_between(start[0], end[0])
    paths = [f"/var/lib/mysql/{f}" for f in files]
    out = mariadb_binlog([f"--start-position={start[1]}", f"--stop-position={end[1]}",
                          "--base64-output=decode-rows", *paths], grep="GTID [0-9]+-")
    groups = group_stats(out.splitlines())
    if not groups["transactions"]:
        raise ValueError("No GTID events found in the workload binlog range")
    return groups


def group_stats(lines: list[str]) -> dict:
    sizes: list[int] = []
    flags = collections.Counter()
    domains = collections.Counter()
    current_cid, current_size = None, 0
    total = 0
    for line in lines:
        m = GTID_RE.search(line)
        if not m:
            continue
        total += 1
        domain, _server, _seq, cid, rest = m.groups()
        domains[int(domain)] += 1
        for f in ("trans", "ddl", "waited"):
            if re.search(rf"\b{f}\b", rest):
                flags[f] += 1
        if cid is not None and cid == current_cid:
            current_size += 1
            continue
        if current_size:
            sizes.append(current_size)
        current_cid, current_size = cid, 1
        if cid is None:          # a transaction that committed alone
            sizes.append(1)
            current_cid, current_size = None, 0
    if current_size:
        sizes.append(current_size)
    hist = collections.Counter(sizes)
    return {
        "transactions": total,
        "groups": len(sizes),
        "mean_group_size": round(total / len(sizes), 2) if sizes else None,
        "share_in_groups_ge2": round(sum(s for s in sizes if s >= 2) / total, 3) if total else None,
        "size_histogram": {str(k): hist[k] for k in sorted(hist)},
        "flags": dict(flags),
        "domains": {str(k): v for k, v in domains.items()},
    }
