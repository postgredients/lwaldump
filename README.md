# lwaldump

`lwaldump` finds the end of the valid WAL prefix stored in a replica's local
`pg_wal`. It is intended for safe quorum failover and supports PostgreSQL
14 through 19.

## Why SQL replication positions are insufficient

`pg_last_wal_receive_lsn()` belongs to walreceiver state and is lost when
PostgreSQL restarts. `pg_last_wal_replay_lsn()` can remain behind WAL that was
already received and flushed before the restart. Electing a primary by either
value can therefore ignore an acknowledged commit that is still present on
disk.

`lwaldump()` starts at the replay position and scans the valid local WAL
records. A scan failure is an error; callers must not fall back to SQL receive
or replay positions.

`lwaldump_with_timeline()` returns the timeline and endpoint of the last valid
local WAL record, not necessarily the timeline currently being replayed. The
scan follows locally present `.history` files and switches segments at timeline
forks. It does not fetch WAL or history from an archive or a primary. Thus the
result describes the position reachable by replay from the files already in
`pg_wal`, after external WAL sources have been fenced.

`recovery_target_timeline = latest` follows the newest local descendant of the
replay timeline. `current` stays on the replay timeline; a numeric target only
follows that timeline when its local history and WAL are present. A history
file alone does not advance the returned timeline: at least one valid record
on the new timeline must be scanned.

## PostgreSQL 14-19 compatibility

The extension is backend code and does not use `FRONTEND` headers. PostgreSQL
15 moved `GetXLogReplayRecPtr()` to `access/xlogrecovery.h`, so that header is
included conditionally.

PostgreSQL 15 also introduced `NextRecPtr` in `XLogReaderState`. The reader
must be positioned with `XLogBeginRead()`; assigning `EndRecPtr` directly
leaves the next read position at zero. The scan is bounded by the newest local
WAL segment so reaching the current local end does not require a nonexistent
next segment.

## Build and usage

```sh
make PG_CONFIG=/path/to/pg_config
make PG_CONFIG=/path/to/pg_config install
```

```sql
CREATE EXTENSION lwaldump;
SELECT lwaldump();
SELECT * FROM lwaldump_with_timeline();
```

The function is valid only on a server in recovery with a replay position and
readable local WAL files.
