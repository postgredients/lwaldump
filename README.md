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

`lwaldump_with_timeline()` returns the replay timeline together with the
durable endpoint scanned by the extension. This binds a failover vote's
timeline to its exact local WAL position.

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
