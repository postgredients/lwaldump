#!/bin/sh
set -eu

BIN="/usr/lib/postgresql/${PG_MAJOR}/bin"
ROOT="/tmp/lwaldump-test"
PRIMARY="$ROOT/primary"
STANDBY="$ROOT/standby"
PRIMARY_SOCKET="$ROOT/primary-socket"
STANDBY_SOCKET="$ROOT/standby-socket"

mkdir -p "$PRIMARY" "$STANDBY" "$PRIMARY_SOCKET" "$STANDBY_SOCKET"
chown -R postgres:postgres "$ROOT"

run_as_postgres() {
    su postgres -c "$*"
}

stop_clusters() {
    run_as_postgres "$BIN/pg_ctl -D '$STANDBY' -m immediate stop" >/dev/null 2>&1 || true
    run_as_postgres "$BIN/pg_ctl -D '$PRIMARY' -m immediate stop" >/dev/null 2>&1 || true
}
trap stop_clusters EXIT

run_as_postgres "$BIN/initdb -D '$PRIMARY' --auth=trust --no-sync" >/dev/null
cat >>"$PRIMARY/postgresql.conf" <<EOF
port = 55432
listen_addresses = ''
unix_socket_directories = '$PRIMARY_SOCKET'
wal_level = replica
max_wal_senders = 5
EOF
chown postgres:postgres "$PRIMARY/postgresql.conf"

run_as_postgres "$BIN/pg_ctl -D '$PRIMARY' -l '$ROOT/primary.log' -w start" >/dev/null
run_as_postgres "$BIN/psql -h '$PRIMARY_SOCKET' -p 55432 -d postgres -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION lwaldump'" >/dev/null
run_as_postgres "$BIN/psql -h '$PRIMARY_SOCKET' -p 55432 -d postgres -v ON_ERROR_STOP=1 -c 'CREATE TABLE wal_payload (id bigint PRIMARY KEY, payload text)'" >/dev/null
run_as_postgres "$BIN/pg_basebackup -h '$PRIMARY_SOCKET' -p 55432 -D '$STANDBY' -X stream -R" >/dev/null
chmod 700 "$STANDBY"

cat >>"$STANDBY/postgresql.conf" <<EOF
port = 55433
listen_addresses = ''
unix_socket_directories = '$STANDBY_SOCKET'
hot_standby = on
restore_command = ''
EOF
chown postgres:postgres "$STANDBY/postgresql.conf"
run_as_postgres "$BIN/pg_ctl -D '$STANDBY' -l '$ROOT/standby.log' -w start" >/dev/null

run_as_postgres "$BIN/psql -h '$STANDBY_SOCKET' -p 55433 -d postgres -v ON_ERROR_STOP=1 -c 'SELECT pg_wal_replay_pause()'" >/dev/null
replay_before="$(run_as_postgres "$BIN/psql -h '$STANDBY_SOCKET' -p 55433 -d postgres -Atc 'SELECT pg_last_wal_replay_lsn()'")"

run_as_postgres "$BIN/psql -h '$PRIMARY_SOCKET' -p 55432 -d postgres -v ON_ERROR_STOP=1 -c \"INSERT INTO wal_payload SELECT g, repeat(md5(g::text), 12) FROM generate_series(1, 180000) AS g\"" >/dev/null
run_as_postgres "$BIN/psql -h '$PRIMARY_SOCKET' -p 55432 -d postgres -v ON_ERROR_STOP=1 -c 'CHECKPOINT; SELECT pg_switch_wal(); SELECT pg_switch_wal();'" >/dev/null
target="$(run_as_postgres "$BIN/psql -h '$PRIMARY_SOCKET' -p 55432 -d postgres -Atc 'SELECT pg_current_wal_flush_lsn()'")"

i=0
while :; do
    received="$(run_as_postgres "$BIN/psql -h '$STANDBY_SOCKET' -p 55433 -d postgres -Atc 'SELECT pg_last_wal_receive_lsn()'")"
    caught_up="$(run_as_postgres "$BIN/psql -h '$STANDBY_SOCKET' -p 55433 -d postgres -Atc \"SELECT COALESCE(pg_last_wal_receive_lsn() >= '$target'::pg_lsn, false)\"")"
    [ "$caught_up" = "t" ] && break
    i=$((i + 1))
    [ "$i" -lt 120 ] || { echo "receiver did not reach $target (at $received)"; exit 10; }
    sleep 0.25
done

run_as_postgres "$BIN/pg_ctl -D '$PRIMARY' -m fast -w stop" >/dev/null
i=0
while :; do
    receivers="$(run_as_postgres "$BIN/psql -h '$STANDBY_SOCKET' -p 55433 -d postgres -Atc 'SELECT count(*) FROM pg_stat_wal_receiver'")"
    [ "$receivers" = "0" ] && break
    i=$((i + 1))
    [ "$i" -lt 80 ] || { echo "walreceiver did not stop"; exit 11; }
    sleep 0.25
done

receive_fenced="$(run_as_postgres "$BIN/psql -h '$STANDBY_SOCKET' -p 55433 -d postgres -Atc 'SELECT pg_last_wal_receive_lsn()'")"
got="$(run_as_postgres "$BIN/psql -h '$STANDBY_SOCKET' -p 55433 -d postgres -v ON_ERROR_STOP=1 -Atc 'SELECT lwaldump()'")"
safe="$(run_as_postgres "$BIN/psql -h '$STANDBY_SOCKET' -p 55433 -d postgres -Atc \"SELECT '$got'::pg_lsn >= '$receive_fenced'::pg_lsn\"")"

echo "PG_VERSION=$($BIN/postgres --version)"
echo "REPLAY_BEFORE=$replay_before"
echo "RECEIVE_FENCED=$receive_fenced"
echo "LWALDUMP_OUTPUT=$got"
echo "COVERS_RECEIVED=$safe"
[ "$safe" = "t" ] || exit 12
