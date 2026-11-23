# Walkthrough: make a replica fall behind and catch it up, by hand

About 15 minutes. Everything here is plain SQL in the `mariadb` client plus
one shell script, so you can see the feature without reading the harness.
The harness (`python -m lab ...`) automates exactly these steps and
measures them.

Prerequisites: Docker with Compose, Python 3.10+ with `pip install -r
requirements.txt` (for `setup`, `reset` and `watch` only).

Two shortcuts used below:

```sh
alias primary='docker compose exec primary mariadb -uroot -plab'
alias replica='docker compose exec replica mariadb -uroot -plab'
```

## 1. Start the servers and wire replication

```sh
docker compose up -d --wait
python -m lab setup
```

`setup` prints every statement it runs. On the replica they are:

```sql
STOP SLAVE;
RESET SLAVE ALL;
SET GLOBAL gtid_slave_pos = '0-1-5';          -- example only: use the current primary position on fresh lab schemas
CHANGE MASTER TO MASTER_HOST='primary', MASTER_PORT=3306,
  MASTER_USER='repl', MASTER_PASSWORD='repl',
  MASTER_USE_GTID=slave_pos, MASTER_DELAY=0;
START SLAVE;
```

Then it creates the `labctl` schema (heartbeat table plus workload procedures)
on the primary, and that replicates. Create the data:

```sh
python -m lab reset        # lab.accounts (200k rows), lab.ledger, ... -- see sql/reset.sql
```

Look at the replica:

```sh
replica -e "SHOW SLAVE STATUS\G" | grep -E "Running|Behind|Gtid_IO|Parallel_Mode|SQL_Running_State"
replica -e "SELECT @@slave_parallel_threads, @@slave_parallel_mode"
```

`slave_parallel_threads = 0`: one SQL thread applies everything.

## 2. Watch lag (second terminal)

```sh
python -m lab watch
```

`watch` updates `labctl.heartbeat.ts = NOW(6)` on the primary every 100 ms and
prints, twice a second, how old that row is on the replica (the lag) and how
many transactions the replica still has to apply (the backlog). Without the
harness, the same two numbers are:

```sql
-- on the replica: how old is the newest heartbeat it has applied?
SELECT TIMESTAMPDIFF(MICROSECOND, ts, NOW(6)) / 1e6 AS lag_s FROM labctl.heartbeat;
-- backlog: compare these two (per GTID domain, the last number is the sequence)
SELECT @@gtid_binlog_pos;   -- on the primary
SELECT @@gtid_slave_pos;    -- on the replica
```

## 3. Make it fall behind

Back in the first terminal: 32 concurrent sessions, 1,000 transactions each.

```sh
scripts/load.sh 32 1000 independent
```

`watch` shows the lag climbing for as long as the load runs, then still
climbing for a while after the load has stopped, before it drops to zero.
Meanwhile the backlog peaks and falls. The primary finishes 32,000
transactions in 12–15 s. The single-threaded replica needs about twice as long.
(If the delayed replica is running, `watch` shows it too: lag ≥ 30 s by design.)

Why: the primary group-commits. Check it:

```sh
primary -e "SHOW GLOBAL STATUS LIKE 'Binlog_%commits'"
replica -e "SHOW GLOBAL STATUS LIKE 'Binlog_%commits'"
```

Run the load again and compare the deltas. On the primary
`Binlog_commits / Binlog_group_commits` is ~8 (eight transactions per disk
flush). On the single-threaded replica it is 1.0: one flush per
transaction.

## 4. Turn on parallel replication

`slave_parallel_threads` can only change while replication is stopped:

```sql
-- on the replica
STOP SLAVE;
SET GLOBAL slave_parallel_threads = 16;
SET GLOBAL slave_parallel_mode = 'optimistic';   -- the default
START SLAVE;
```

Run `scripts/load.sh 32 1000 independent` again. Compare the peak heartbeat age with the serial run. While it runs, look at the workers:

```sql
SELECT ID, STATE FROM information_schema.PROCESSLIST WHERE COMMAND = 'Slave_worker';
```

You will see `Waiting for prior transaction to commit`. The workers apply in
parallel, but they commit in the primary's order. `Binlog_commits /
Binlog_group_commits` on the replica is now well above 1: the replica
group-commits too.

## 5. A workload that does not parallelise

```sh
scripts/load.sh 32 1000 own_myisam     # same pattern, MyISAM tables
scripts/load.sh 32 1000 own_innodb     # same pattern, InnoDB tables
```

With 16 workers, the MyISAM run still lags and the InnoDB run does not. A
MyISAM change cannot be rolled back, so the replica cannot apply it
speculatively. It waits for everything before it. Try `hotspot` too, then
look at `SHOW GLOBAL STATUS LIKE 'Slave_retried_transactions'`.

## 6. A long ALTER, and GTID domains

```sh
python -m lab reset --archive-rows 2000000
scripts/load.sh 32 1500 independent &
sleep 5
primary -e "ALTER TABLE lab.archive ADD COLUMN extra INT NOT NULL DEFAULT 0, ALGORITHM=COPY"
wait
```

The ALTER takes 10–13 s on the primary while the DML carries on around it. On
the replica it only starts once it has committed on the primary, and all
DML behind it waits: the domain-0 heartbeat can go stale. Repeat with the
ALTER in its own replication domain:

```sh
python -m lab reset --archive-rows 2000000
scripts/load.sh 32 1500 independent &
sleep 5
primary -e "SET SESSION gtid_domain_id = 1;
            ALTER TABLE lab.archive ADD COLUMN extra INT NOT NULL DEFAULT 0, ALGORITHM=COPY"
wait
replica -e "SELECT @@gtid_slave_pos"      # now two positions: 0-1-..., 1-1-...
```

With parallel workers, domain 0 keeps flowing while one worker runs the
ALTER. This is out-of-order parallel replication. It is your job to make sure
the two domains do not depend on each other. Here the ALTERed table is not
touched by the DML.

## 7. Look at what is flowing

```sh
make demo-binlog
```

This prints, from your own primary: a transaction's GTID, its
`Annotate_rows` event (the SQL text), the row images, the CRC32 of each event,
the `cid=` shared by a group commit, and `mariadb-binlog
--verify-binlog-checksum` rejecting a copy of the binlog with one byte
changed.

## 8. Lag on purpose

```sh
make up-delayed setup      # starts a third server with MASTER_DELAY=30
make demo-delayed
```

This creates a table, drops it "by accident", finds the DROP in the primary's
binlog, stops the delayed replica just before that GTID with `START SLAVE
UNTIL master_gtid_pos = ...`, and copies the table back to the primary.

## 9. Measure instead of watching

Everything above, measured and repeated:

```sh
python -m lab run --threads 0            # one run, results/adhoc/...
python -m lab run --threads 16
python -m lab plot results/adhoc
```

For a fresh full benchmark, set `LAB_RESULTS_DIR="$PWD/results-local"` as shown in the README.
