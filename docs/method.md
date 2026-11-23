# Method: how the lab measures, and what that does and does not show

## Topology

`docker-compose.yml` runs three MariaDB servers from one image:

| service | role | config | port on the host |
|---|---|---|---|
| `primary` | takes the write load | [config/primary.cnf](../config/primary.cnf) | 3307 |
| `replica` | the replica under test; its parallel settings change per run | [config/replica.cnf](../config/replica.cnf) | 3308 |
| `delayed` | optional (`--profile delayed`), `MASTER_DELAY=30` | [config/delayed.cnf](../config/delayed.cnf) | 3309 |

Baseline settings on the primary and benchmark replica:

* `binlog_format=ROW`, `binlog_annotate_row_events=ON`, `binlog_checksum=CRC32`
* GTID replication (`MASTER_USE_GTID=slave_pos`)
* full durability: `sync_binlog=1`, `innodb_flush_log_at_trx_commit=1`
* the replica writes its own binlog (`log_slave_updates=ON`), as a replica
  that can be promoted must
* `innodb_buffer_pool_size=512M`: the DDL experiment also builds a two-million-row archive; cache residency was not recorded

The harness (Python, `lab/`) runs on the host and connects to the published
ports. It changes a run's settings with `SET GLOBAL` (replication stopped
while doing so), so nothing is restarted between runs.

## One run

[`lab/run.py`](../lab/run.py), in order:

1. **Reset the data**: drop and recreate the `lab` schema on the primary from
   [sql/reset.sql](../sql/reset.sql) (200,000 accounts, generated with the
   Sequence engine), purge old binlogs, wait until the replica has applied
   the reset. Every run starts from identical data.
2. **Apply the settings under test**: `slave_parallel_threads`,
   `slave_parallel_mode` (and for some experiments `binlog_commit_wait_*` on
   the primary, durability on the replica, or the replica's CPU limit).
3. **Start the heartbeat** (`UPDATE labctl.heartbeat SET ts=NOW(6)` on the
   primary every 100 ms) and wait until the replica sees a fresh one.
4. **Start the sampler**: every 250 ms it records heartbeat lag, GTID
   backlog, `Seconds_Behind_Master`, `Master_Slave_time_diff`, binlog and
   group-commit counters, retries; once a second, the state of every worker
   thread. → `samples.csv`
5. **Run the workload**: N connections to the primary, each `CALL`ing a
   procedure from [sql/labctl.sql](../sql/labctl.sql) that commits
   `tx_per_client` transactions. Experiment 4 uses 8,000 transactions with one serial client and 32,000 with the other workloads.
6. **Wait until the replica has caught up** (backlog ≤ 2 transactions and
   heartbeat lag < 0.5 s), plus 3 s.
7. **Analyse**: `summary.json` (definitions in
   [lab/analysis.py](../lab/analysis.py)); and `groups.json`, the commit-group
   sizes and GTID flags in the primary's binlog for this run, read with
   `mariadb-binlog`.

`meta.json` stores the full run spec, the server version, the relevant
server variables as they were on both servers, and the host description.
Each run directory is self-describing.

### Two ways to measure

* **live**: the replica applies while the load runs. This produces the
  lag-over-time curves a user would experience. On a single host the primary
  and replica share the disk, so a replica that works harder slows the primary
  a little (visible in the primary tx/s column of experiment 1).
* **catchup**: the replica's SQL thread is stopped while the load runs (the
  IO thread keeps copying), then started. The drain measures the replica's
  **apply capacity** alone, with the primary idle. This is the clean
  number for "how fast can N workers apply"; the scaling curve uses it.

## Repeats and spread

Every configuration runs 3 times (`repeats` in the plan). Repeats are
**interleaved**: configuration A, B, C, …, then A, B, C again, so slow drift on
the host (thermal, page cache, a noisy neighbour) affects all configurations
alike instead of biasing whichever ran last. The summary reports medians and observed ranges. Overlays select the middle run by total completion time. The binlog histograms show repeat 1.

## Metrics

| metric | definition |
|---|---|
| heartbeat lag | `TIMESTAMPDIFF(MICROSECOND, ts, NOW(6))/1e6` on the replica, where `ts` is set to `NOW(6)` on the primary every 100 ms. Both servers share the host clock; across machines this needs NTP. Resolution ≈ the heartbeat interval. |
| backlog | Sum of nonnegative differences by domain (primary `@@gtid_binlog_pos` seq − replica `@@gtid_slave_pos` seq) |
| peak lag | maximum heartbeat lag in the run |
| caught up after | seconds from load start to the observed catch-up threshold |
| replica tx/s | transactions applied per second while the backlog was > 50, i.e. while the replica was saturated (its capacity). If fewer than two seconds of qualifying intervals are available, workload transaction count divided by total completion time is used instead |
| tx/group | Δ`Binlog_commits` / Δ`Binlog_group_commits` (primary during the load; replica during the apply) |
| retries | Δ`Slave_retried_transactions` |

## The reference machine

The numbers in `results/` were produced on a cloud VM: 2 vCPU (x86_64),
7.8 GiB RAM, virtio disk (fsync ≈ 0.2 ms, measured with `dd oflag=dsync`),
Linux 6.x. The benchmark servers each had a one-CPU quota, except for the selected two-CPU configurations in experiment 2. Quotas do not reserve a dedicated core.

The recorded server reports 11.8.6-MariaDB-5 from Ubuntu-log. The Compose default uses the official 11.8 image. A different build, host, storage device or workload can change both the throughput and the ranking.

## Interpreting saved runs

A sample covers several queries. The backlog comparison reads the primary first and the replica second, so it estimates outstanding GTID events. It includes heartbeat updates and, where applicable, DDL. The throughput numerator on the primary counts workload transactions only. The replica busy-rate numerator counts GTID advances. Neither counter measures physical fsync calls.

The original harness could use a sample taken just before workload completion when checking the catch-up threshold. For six archived runs, the analysis uses the first qualifying sample at or after load end. The original CSV and meta.json are preserved. Worker-state shares count observations during the apply window. In the summary, “after load end” measures the remaining wait. In the individual JSON, drain_s starts at SQL-thread restart in catch-up runs and at load start in live runs.

The DDL timestamps describe the primary. The lab does not directly profile CPU time, lock identities, replica-side ALTER timing or disk service time. Min–max ranges describe the three observed repeats.
