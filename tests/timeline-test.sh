#!/bin/sh
set -eu

BIN="/usr/lib/postgresql/${PG_MAJOR}/bin"
ROOT="/tmp/lwaldump-timeline-test"
PRIMARY="$ROOT/primary"
CANDIDATE="$ROOT/candidate"
VOTER="$ROOT/voter"
WAL_ARCHIVE="$ROOT/wal-archive"
PRIMARY_SOCKET="$ROOT/primary-socket"
CANDIDATE_SOCKET="$ROOT/candidate-socket"
VOTER_SOCKET="$ROOT/voter-socket"

mkdir -p "$PRIMARY" "$CANDIDATE" "$VOTER" "$WAL_ARCHIVE" \
    "$PRIMARY_SOCKET" "$CANDIDATE_SOCKET" "$VOTER_SOCKET"
chown -R postgres:postgres "$ROOT"

run_as_postgres() {
    su postgres -c "$*"
}

stop_clusters() {
    run_as_postgres "$BIN/pg_ctl -D '$VOTER' -m immediate stop" >/dev/null 2>&1 || true
    run_as_postgres "$BIN/pg_ctl -D '$CANDIDATE' -m immediate stop" >/dev/null 2>&1 || true
    run_as_postgres "$BIN/pg_ctl -D '$PRIMARY' -m immediate stop" >/dev/null 2>&1 || true
}
trap stop_clusters EXIT

run_as_postgres "$BIN/initdb -D '$PRIMARY' --auth=trust --no-sync" >/dev/null
cat >>"$PRIMARY/postgresql.conf" <<EOF
port = 55442
listen_addresses = ''
unix_socket_directories = '$PRIMARY_SOCKET'
wal_level = replica
max_wal_senders = 5
wal_keep_size = '1GB'
EOF
chown postgres:postgres "$PRIMARY/postgresql.conf"

run_as_postgres "$BIN/pg_ctl -D '$PRIMARY' -l '$ROOT/primary.log' -w start" >/dev/null
run_as_postgres "$BIN/psql -h '$PRIMARY_SOCKET' -p 55442 -d postgres -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION lwaldump'" >/dev/null
run_as_postgres "$BIN/psql -h '$PRIMARY_SOCKET' -p 55442 -d postgres -v ON_ERROR_STOP=1 -c 'CREATE TABLE wal_payload (id bigint PRIMARY KEY, payload text)'" >/dev/null
run_as_postgres "$BIN/pg_basebackup -h '$PRIMARY_SOCKET' -p 55442 -D '$CANDIDATE' -X stream -R" >/dev/null
run_as_postgres "$BIN/pg_basebackup -h '$PRIMARY_SOCKET' -p 55442 -D '$VOTER' -X stream -R" >/dev/null
chmod 700 "$CANDIDATE" "$VOTER"

cat >>"$CANDIDATE/postgresql.conf" <<EOF
port = 55443
listen_addresses = ''
unix_socket_directories = '$CANDIDATE_SOCKET'
hot_standby = on
wal_keep_size = '1GB'
restore_command = ''
EOF
chown postgres:postgres "$CANDIDATE/postgresql.conf"
run_as_postgres "$BIN/pg_ctl -D '$CANDIDATE' -l '$ROOT/candidate.log' -w start" >/dev/null

run_as_postgres "$BIN/psql -h '$PRIMARY_SOCKET' -p 55442 -d postgres -v ON_ERROR_STOP=1 -c \"INSERT INTO wal_payload SELECT g, repeat(md5(g::text), 12) FROM generate_series(1, 50000) AS g\"" >/dev/null
fork_lsn="$(run_as_postgres "$BIN/psql -h '$PRIMARY_SOCKET' -p 55442 -d postgres -Atc 'SELECT pg_current_wal_flush_lsn()'")"

i=0
while :; do
    caught_up="$(run_as_postgres "$BIN/psql -h '$CANDIDATE_SOCKET' -p 55443 -d postgres -Atc \"SELECT pg_last_wal_replay_lsn() >= '$fork_lsn'::pg_lsn\"")"
    [ "$caught_up" = "t" ] && break
    i=$((i + 1))
    [ "$i" -lt 240 ] || { echo "candidate did not replay through $fork_lsn"; exit 20; }
    sleep 0.25
done

# Promotion may recycle parent-timeline segments, so retain their local copies.
cp "$CANDIDATE"/pg_wal/00000001* "$WAL_ARCHIVE/"

run_as_postgres "$BIN/pg_ctl -D '$PRIMARY' -m fast -w stop" >/dev/null
run_as_postgres "$BIN/pg_ctl -D '$CANDIDATE' -w promote" >/dev/null

run_as_postgres "$BIN/psql -h '$CANDIDATE_SOCKET' -p 55443 -d postgres -v ON_ERROR_STOP=1 -c \"INSERT INTO wal_payload SELECT g, repeat(md5(g::text), 12) FROM generate_series(50001, 51000) AS g\"" >/dev/null
timeline2_end="$(run_as_postgres "$BIN/psql -h '$CANDIDATE_SOCKET' -p 55443 -d postgres -Atc 'SELECT pg_current_wal_flush_lsn()'")"
run_as_postgres "$BIN/pg_ctl -D '$CANDIDATE' -m fast -w stop" >/dev/null

cp "$WAL_ARCHIVE"/00000001* "$VOTER/pg_wal/"
cp "$CANDIDATE"/pg_wal/0000000* "$VOTER/pg_wal/"
chown postgres:postgres "$VOTER"/pg_wal/0000000*
cat >>"$VOTER/postgresql.conf" <<EOF
port = 55444
listen_addresses = ''
unix_socket_directories = '$VOTER_SOCKET'
hot_standby = on
restore_command = ''
recovery_target_timeline = 'latest'
recovery_min_apply_delay = '1h'
EOF
cat >>"$VOTER/postgresql.auto.conf" <<EOF
primary_conninfo = ''
EOF
chown postgres:postgres "$VOTER/postgresql.conf" "$VOTER/postgresql.auto.conf"
run_as_postgres "$BIN/pg_ctl -D '$VOTER' -l '$ROOT/voter.log' -w start" >/dev/null

result="$(run_as_postgres "$BIN/psql -h '$VOTER_SOCKET' -p 55444 -d postgres -AtF '|' -v ON_ERROR_STOP=1 -c 'SELECT timeline, flush_lsn FROM lwaldump_with_timeline()'")"
timeline="${result%%|*}"
flush_lsn="${result#*|}"
covers_timeline2="$(run_as_postgres "$BIN/psql -h '$VOTER_SOCKET' -p 55444 -d postgres -Atc \"SELECT '$flush_lsn'::pg_lsn >= '$timeline2_end'::pg_lsn\"")"

echo "RECOVERY_PATH_OUTPUT=$result"
echo "TIMELINE2_END=$timeline2_end"
echo "COVERS_TIMELINE2=$covers_timeline2"
[ "$timeline" = "2" ] || exit 21
[ "$covers_timeline2" = "t" ] || exit 22

# Without the child's copy of the segment that contains the fork, only the
# parent up to the switchpoint is reachable. Never read its divergent tail.
run_as_postgres "$BIN/pg_ctl -D '$VOTER' -m immediate -w stop" >/dev/null
child_switch_segment="$(find "$VOTER/pg_wal" -maxdepth 1 -type f \
    -name '00000002*' ! -name '*.history' | sort | head -n 1)"
[ -n "$child_switch_segment" ] || exit 23
mv "$child_switch_segment" "$child_switch_segment.saved"
switchpoint="$(awk 'NR == 1 { print $2 }' "$VOTER/pg_wal/00000002.history")"
run_as_postgres "$BIN/pg_ctl -D '$VOTER' -l '$ROOT/voter.log' -w start" >/dev/null
fallback_result="$(run_as_postgres "$BIN/psql -h '$VOTER_SOCKET' -p 55444 -d postgres -AtF '|' -v ON_ERROR_STOP=1 -c 'SELECT timeline, flush_lsn FROM lwaldump_with_timeline()'")"

echo "MISSING_CHILD_SWITCH_SEGMENT_OUTPUT=$fallback_result"
echo "SWITCHPOINT=$switchpoint"
[ "$fallback_result" = "1|$switchpoint" ] || exit 24
