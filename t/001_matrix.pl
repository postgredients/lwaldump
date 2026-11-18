use strict;
use warnings;

use FindBin;
use lib $FindBin::Bin;
use LwaldumpTest qw(new_node psql wait_until lsn_ge);
use Test::More;

my $primary = new_node('matrix_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', "wal_keep_size = '1GB'");
$primary->start;

psql($primary, 'CREATE EXTENSION lwaldump');
psql($primary, 'CREATE TABLE wal_payload (id bigint PRIMARY KEY, payload text)');

$primary->backup('matrix', backup_options => [ '-X', 'stream' ]);
my $standby = new_node('matrix_standby');
$standby->init_from_backup($primary, 'matrix', has_streaming => 1);
$standby->append_conf('postgresql.conf', "restore_command = ''");
$standby->start;

psql($standby, 'SELECT pg_wal_replay_pause()');
ok(wait_until($standby,
	"SELECT pg_get_wal_replay_pause_state() = 'paused'", 't', 60),
	'replay is paused before additional WAL arrives');
my $replay_before = psql($standby, 'SELECT pg_last_wal_replay_lsn()');
my ($first_file, $corrupt_offset) = split /\|/, psql($primary,
	"SELECT pg_walfile_name('$replay_before'::pg_lsn + 512), "
	. "pg_wal_lsn_diff('$replay_before'::pg_lsn + 512, '0/0')::numeric "
	. "% pg_size_bytes(current_setting('wal_segment_size'))");

psql($primary,
	"INSERT INTO wal_payload SELECT g, repeat(md5(g::text), 12) "
	  . "FROM generate_series(1, 180000) AS g");
psql($primary, 'CHECKPOINT');
psql($primary, 'SELECT pg_switch_wal()');
psql($primary, 'SELECT pg_switch_wal()');
psql($primary,
	"INSERT INTO wal_payload VALUES (180001, repeat(md5('tail'), 12))");
my $target = psql($primary, 'SELECT pg_current_wal_flush_lsn()');
my $target_file = psql($primary,
	"SELECT pg_walfile_name('$target'::pg_lsn - 1)");

ok(wait_until($standby,
	"SELECT COALESCE(pg_last_wal_receive_lsn() >= '$target'::pg_lsn, false)",
	't', 240), 'standby received WAL through the target');

$primary->stop('fast');
ok(wait_until($standby,
	'SELECT count(*) = 0 FROM pg_stat_wal_receiver', 't', 30),
	'WAL receiver has stopped');

my $receive_fenced = psql($standby, 'SELECT pg_last_wal_receive_lsn()');
my $got = psql($standby, 'SELECT lwaldump()');
ok(lsn_ge($standby, $got, $receive_fenced),
	'lwaldump reaches at least the fenced receive LSN');
ok(lsn_ge($standby, $receive_fenced, $replay_before),
	'received WAL is no older than paused replay');

diag("replay before=$replay_before, fenced receive=$receive_fenced, lwaldump=$got");

# A known received position must not be silently ignored when the final
# segment disappears and the reader reaches the artificial local bound.
my $missing_path = $standby->data_dir . "/pg_wal/$target_file";
rename($missing_path, "$missing_path.saved")
  or BAIL_OUT("could not hide $missing_path: $!");
my ($gap_stdout, $gap_stderr);
my $gap_exit = $standby->psql('postgres', 'SELECT lwaldump()',
	stdout => \$gap_stdout, stderr => \$gap_stderr);
ok($gap_exit != 0, 'missing received segment is rejected');
like($gap_stderr, qr/before the known received position/,
	'missing segment failure uses the retained receive bound');
rename("$missing_path.saved", $missing_path)
  or BAIL_OUT("could not restore $missing_path: $!");

# A damaged record in the already received range must not turn into an
# apparently valid vote at the previous record's position.
my $path = $standby->data_dir . "/pg_wal/$first_file";
open(my $wal, '+<', $path) or BAIL_OUT("could not open $path: $!");
binmode($wal);
seek($wal, $corrupt_offset, 0) or BAIL_OUT("could not seek in $path: $!");
read($wal, my $original, 1) == 1 or BAIL_OUT("could not read $path: $!");
seek($wal, $corrupt_offset, 0) or BAIL_OUT("could not seek in $path: $!");
print {$wal} chr(ord($original) ^ 1) or BAIL_OUT("could not alter $path: $!");
close($wal) or BAIL_OUT("could not close $path: $!");

my ($stdout, $stderr);
my $exit = $standby->psql('postgres', 'SELECT lwaldump()',
	stdout => \$stdout, stderr => \$stderr);
ok($exit != 0, 'corrupt WAL record is rejected');
like($stderr, qr/could not scan WAL/i,
	'failure identifies the damaged WAL record');

$standby->stop('immediate');
done_testing();
