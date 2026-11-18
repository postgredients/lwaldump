use strict;
use warnings;

use File::Copy qw(copy);
use FindBin;
use lib $FindBin::Bin;
use LwaldumpTest qw(new_node psql wait_until);
use Test::More;

my $primary = new_node('ancestor_segment_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', "wal_keep_size = '1GB'");
$primary->start;
psql($primary, 'CREATE EXTENSION lwaldump');
psql($primary, 'CREATE TABLE wal_payload (id int PRIMARY KEY)');
$primary->backup('before_fork', backup_options => ['-X', 'stream']);

my $child = new_node('ancestor_segment_child');
$child->init_from_backup($primary, 'before_fork', has_streaming => 1);
$child->append_conf('postgresql.conf', "wal_keep_size = '1GB'");
$child->start;
$primary->stop('fast');
$child->promote;
psql($child, 'INSERT INTO wal_payload VALUES (1)');

my $voter = new_node('ancestor_segment_voter');
$voter->init_from_backup($primary, 'before_fork', has_streaming => 1);
$voter->enable_streaming($child);
$voter->append_conf('postgresql.conf', qq(
restore_command = ''
recovery_target_timeline = 'latest'
));
$voter->start;
ok(wait_until($voter,
	'SELECT EXISTS (SELECT 1 FROM wal_payload WHERE id = 1)'),
	'voter replays the child record');
psql($voter, 'SELECT pg_wal_replay_pause()');
psql($child, 'INSERT INTO wal_payload VALUES (2)');
ok(wait_until($voter,
	"SELECT pg_get_wal_replay_pause_state() = 'paused'"),
	'voter is paused on timeline 2');
my $replay_lsn = psql($voter, 'SELECT pg_last_wal_replay_lsn()');

my $history_path = $child->data_dir . '/pg_wal/00000002.history';
open(my $history, '<', $history_path)
  or BAIL_OUT("could not read $history_path: $!");
my @lines = <$history>;
close($history);
my ($ancestor_tli, $fork_lsn) = split /\s+/, $lines[-1];
is($ancestor_tli, '1', 'child branched from timeline 1');
is(psql($child,
	"SELECT pg_wal_lsn_diff('$fork_lsn', '0/0')::numeric "
	. "% pg_size_bytes(current_setting('wal_segment_size')) <> 0"),
	't', 'fork lies inside a WAL segment');
is(psql($child,
	"SELECT '$replay_lsn'::pg_lsn > '$fork_lsn'::pg_lsn"),
	't', 'replay lies after the ancestor fork');
my ($replay_file, $fork_file) = split /\|/, psql($child,
	"SELECT pg_walfile_name('$replay_lsn'::pg_lsn + 1), "
	. "pg_walfile_name('$fork_lsn'::pg_lsn)");
is(substr($replay_file, 8), substr($fork_file, 8),
	'replay and ancestor fork lie in one WAL segment');
$child->stop('immediate');
$voter->append_conf('postgresql.auto.conf', "primary_conninfo = ''");
$voter->reload;
ok(wait_until($voter, 'SELECT count(*) = 0 FROM pg_stat_wal_receiver'),
	'voter WAL receiver is stopped');
my ($baseline_tli) = split /\|/, psql($voter,
	'SELECT timeline, flush_lsn FROM lwaldump_with_timeline()');
is($baseline_tli, '2', 'baseline scan follows timeline 2');

my $suffix = substr($replay_file, 8);
my $old_file = '00000001' . $suffix;
my $new_file = '00000002' . $suffix;
my $voter_wal = $voter->data_dir . '/pg_wal';
copy($primary->data_dir . "/pg_wal/$old_file", "$voter_wal/$old_file")
  or BAIL_OUT("could not copy ancestor WAL: $!");
rename("$voter_wal/$new_file", "$voter_wal/$new_file.saved")
  or BAIL_OUT("could not hide child WAL: $!");

my ($stdout, $stderr);
my $exit = $voter->psql('postgres',
	'SELECT timeline, flush_lsn FROM lwaldump_with_timeline()',
	stdout => \$stdout, stderr => \$stderr);
ok($exit != 0, 'ancestor segment ending before replay is rejected');
like($stderr, qr/could not find WAL at the replay position/,
	'failure identifies missing WAL at replay');

$voter->stop('immediate');
done_testing();
