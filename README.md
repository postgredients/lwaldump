# lwaldump

`lwaldump` finds the end of the valid WAL prefix stored in a replica's local
`pg_wal`. It can supply a local position for quorum failover and supports
PostgreSQL 14 through 19.

## Why SQL replication positions are insufficient

`pg_last_wal_receive_lsn()` belongs to walreceiver state and is lost when
PostgreSQL restarts. `pg_last_wal_replay_lsn()` can remain behind WAL that was
already received and flushed before the restart. Electing a primary by either
value can therefore ignore an acknowledged commit that is still present on
disk.

`lwaldump()` starts at the replay position and scans the valid local WAL
records on the timeline already reached by replay. For a segment that
straddles an earlier timeline switch it uses the copy on the replay timeline.
The scan stops at the first segment absent from that local recovery path. A
detected scan failure is an error; callers must not fall back to SQL receive
or replay positions.

`lwaldump_with_timeline()` returns the timeline of the last valid record
together with the local endpoint scanned by the extension. The result is the
position recovery can reach from WAL already present in `pg_wal` after
external WAL sources have been fenced. This binds a failover vote's timeline
to its local WAL position.

The scan reads WAL but does not `fsync` it. If WAL was placed in `pg_wal` by
anything other than PostgreSQL's stopped walreceiver, the caller must ensure
the files are durable before recording a vote.

A zero-filled last record or page is indistinguishable from unwritten WAL if
walreceiver's flush position was lost in a restart. The function cannot prove
that an acknowledged record has not been erased from this final segment. A
failover system that needs that guarantee must retain an independent durable
upper bound for acknowledged WAL and compare it with the returned position.

For `recovery_target_timeline = latest`, the last timeline stored by
walreceiver is only a lower bound on the selected recovery path. PostgreSQL's
actual `recoveryTargetTLI` and open WAL file belong to the startup process and
are not visible to this SQL backend. Even if a child history and WAL file are
present, startup may continue reading an already open parent segment. The
function therefore rejects a target timeline until replay itself has reached
it. This applies to both `latest` and an explicit numeric child target. Once
replay has switched, the function can scan its local WAL tail while replay is
paused. Ambiguous sibling histories are also rejected.

The caller must pause WAL replay, stop walreceiver and archive restore before
taking a failover vote. Do not replace, rename, truncate, or rewrite existing
WAL bytes after recovery starts; ordinary walreceiver append before the fence
is allowed. Keep local history files unchanged during the scan. After a manual
WAL change, restart recovery before scanning: startup may retain the old file
or page bytes in its descriptor and read buffer. An
ambiguous `latest` target is an error and must not produce a failover vote.
Keep replay paused until the vote has been recorded.
An active point-in-time recovery target also prevents a local WAL vote because
the server may not replay through the scanned endpoint.

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

Existing installations of version 1.0 can add the timeline function with
`ALTER EXTENSION lwaldump UPDATE` after installing the new extension files.

The function is valid only on a server in recovery with a replay position and
readable local WAL files.

## Tests

The integration tests use PostgreSQL's TAP framework and create their own
primary and standby nodes. Run `make installcheck` after installing the
extension, using a PostgreSQL build with TAP support and the required Perl
modules. The CI Docker image provides those dependencies:

```sh
docker build --build-arg PG_MAJOR=17 --build-arg PG_TAG=17-bookworm \
  -f tests/Dockerfile -t lwaldump-test:17 .
docker run --rm lwaldump-test:17
```
