# Reading the replication experiments

## From a commit to a replica read

The primary logs changes to its binary log. A replica IO thread fetches those events into a relay log. The SQL thread applies them itself or dispatches work to parallel workers. Receiving a transaction does not make its changes visible to a reader.

With row-based logging, a row update is replayed from row events. A DDL statement such as ALTER TABLE is logged as a statement. The binlog demo shows the GTID, the optional SQL annotation and the row changes for one update.

## Why measure both age and backlog?

Suppose the primary commits 2,800 transactions per second and the replica applies 1,300. At roughly constant rates, the queue grows by 1,500 transactions each second. The source timestamp the replica has reached advances by about 1,300 / 2,800 seconds per second. Its age therefore increases by about 0.54 seconds per second of load. After clients stop, the backlog can shrink while the replica is still applying older events. The heartbeat age can keep rising during that interval.

## Scheduling within a GTID domain

| Configuration | What overlaps |
|---|---|
| Zero workers | The SQL thread applies the stream serially. |
| Minimal | Workers can overlap commit stages. Row application remains serial. |
| Conservative | Rows from the same primary group commit can be applied concurrently. The next group can begin as its predecessor enters the commit stage. |
| Optimistic | Workers may try transactional updates from different commit groups. The scheduler uses hints such as a lock wait on the primary. A conflict can require rollback and replay. |
| Aggressive | Workers speculate more freely by ignoring the primary-side conflict hint. |

Commit order is preserved within the domain. Different domains can advance independently, so assigning a separate domain requires knowing that the application does not depend on cross-domain order. In the DDL experiment, the ALTER rebuilds lab.archive and the writers update different tables. The heartbeat belongs to the writers’ domain.

## Reading the binlog and worker counters

A shared commit ID in adjacent GTID events identifies a primary binlog commit group. The histograms weight each group by its size: a group of ten contributes ten transactions. A worker observed in a commit-related state may be waiting. A process-list snapshot does not measure CPU utilisation.

Under optimistic scheduling, a MyISAM update waits for earlier events. A later InnoDB update can still overlap it. DDL acts as a barrier on both sides within the domain. InnoDB transactions can be retried. In the shared-counter experiment, conservative mode recorded zero retries, while optimistic mode recorded a median of 1,543. Aggressive mode recorded more retries but still applied faster than optimistic mode. The useful comparison is throughput and lag alongside retries.

A delayed replica intentionally waits before replay. The recovery demo exploits that interval to read a table before its DROP takes effect. The checksum demo addresses a separate question: whether stored event bytes still match their recorded CRC.

[MariaDB parallel replication](https://mariadb.com/docs/server/ha-and-performance/standard-replication/parallel-replication) · [Replica status fields](https://mariadb.com/docs/server/reference/sql-statements/administrative-sql-statements/show/show-replica-status)
