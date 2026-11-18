use strict;
use warnings;

use FindBin;
use lib $FindBin::Bin;
use LwaldumpTest qw(new_node psql wait_until);
use Test::More;

my $primary = new_node('truncated_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', qq(
wal_level = logical
wal_keep_size = '1GB'
));
$primary->start;
psql($primary, 'CREATE EXTENSION lwaldump');
$primary->backup('before_message', backup_options => ['-X', 'stream']);

my $standby = new_node('truncated_standby');
$standby->init_from_backup($primary, 'before_message', has_streaming => 1);
$standby->append_conf('postgresql.conf', "restore_command = ''");
$standby->start;
psql($standby, 'SELECT pg_wal_replay_pause()');
ok(wait_until($standby,
	"SELECT pg_get_wal_replay_pause_state() = 'paused'"),
	'replay is paused before the long WAL record arrives');

psql($primary, 'SELECT pg_switch_wal()');
my $record_start = psql($primary, 'SELECT pg_current_wal_insert_lsn()');
psql($primary,
	"SELECT pg_logical_emit_message(false, 'truncated-record', "
	. "repeat('x', 20 * 1024 * 1024))");
my $record_end = psql($primary, 'SELECT pg_current_wal_insert_lsn()');
psql($primary, 'SELECT pg_switch_wal()');
my ($start_file, $end_file) = split /\|/, psql($primary,
	"SELECT pg_walfile_name('$record_start'::pg_lsn + 1), "
	. "pg_walfile_name('$record_end'::pg_lsn - 1)");
isnt($start_file, $end_file, 'one WAL record spans two segments');
ok(wait_until($standby,
	"SELECT COALESCE(pg_last_wal_receive_lsn() >= '$record_end'::pg_lsn, false)",
	't', 120), 'standby received the complete long record');

$primary->stop('fast');
ok(wait_until($standby, 'SELECT count(*) = 0 FROM pg_stat_wal_receiver'),
	'WAL receiver has stopped');
my $path = $standby->data_dir . "/pg_wal/$end_file";
rename($path, "$path.saved")
  or BAIL_OUT("could not hide $path: $!");

my ($stdout, $stderr);
my $exit = $standby->psql('postgres', 'SELECT lwaldump()',
	stdout => \$stdout, stderr => \$stderr);
ok($exit != 0, 'a WAL record cut by the local segment bound is rejected');
like($stderr, qr/extends beyond available local WAL/,
	'failure identifies the incomplete WAL record');

$standby->stop('immediate');
done_testing();
