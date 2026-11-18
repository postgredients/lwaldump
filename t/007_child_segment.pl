use strict;
use warnings;

use File::Copy qw(copy);
use FindBin;
use lib $FindBin::Bin;
use LwaldumpTest qw(new_node psql wait_until lsn_ge);
use Test::More;

my $primary = new_node('child_segment_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', "wal_keep_size = '1GB'");
$primary->start;
psql($primary, 'CREATE EXTENSION lwaldump');
psql($primary, 'CREATE TABLE wal_payload (id int PRIMARY KEY)');
$primary->backup('before_tli2', backup_options => ['-X', 'stream']);

my $parent = new_node('child_segment_parent');
$parent->init_from_backup($primary, 'before_tli2', has_streaming => 1);
$parent->append_conf('postgresql.conf', "wal_keep_size = '1GB'");
$parent->start;
$primary->stop('fast');
$parent->promote;
psql($parent, 'SELECT pg_switch_wal()');
$parent->backup('on_tli2', backup_options => ['-X', 'stream']);

my $child = new_node('child_segment_child');
$child->init_from_backup($parent, 'on_tli2', has_streaming => 1);
$child->append_conf('postgresql.conf', "restore_command = ''");
$child->start;

my $voter = new_node('child_segment_voter');
$voter->init_from_backup($parent, 'on_tli2', has_streaming => 1);
$voter->append_conf('postgresql.conf', qq(
restore_command = ''
recovery_target_timeline = 'latest'
recovery_min_apply_delay = '15s'
));
$voter->start;
psql($voter, 'SELECT pg_wal_replay_pause()');
ok(wait_until($voter,
	"SELECT pg_get_wal_replay_pause_state() = 'paused'"),
	'voter is paused on timeline 2');
my $replay_lsn = psql($voter, 'SELECT pg_last_wal_replay_lsn()');

psql($parent, 'INSERT INTO wal_payload VALUES (1)');
my $parent_end = psql($parent, 'SELECT pg_current_wal_flush_lsn()');
ok(wait_until($child,
	"SELECT COALESCE(pg_last_wal_replay_lsn() >= '$parent_end'::pg_lsn, false)"),
	'future child has replayed the parent record');
$parent->stop('immediate');
$child->promote;
psql($child, 'INSERT INTO wal_payload VALUES (2)');
my $child_end = psql($child, 'SELECT pg_current_wal_flush_lsn()');

my $history_name = '00000003.history';
my $history_src = $child->data_dir . "/pg_wal/$history_name";
open(my $history, '<', $history_src)
  or BAIL_OUT("could not read $history_src: $!");
my @lines = <$history>;
close($history);
my ($fork_tli, $fork_lsn) = split /\s+/, $lines[-1];
is($fork_tli, '2', 'child branched from timeline 2');
is(psql($child,
	"SELECT pg_wal_lsn_diff('$fork_lsn', '0/0')::numeric "
	. "% pg_size_bytes(current_setting('wal_segment_size')) <> 0"),
	't', 'fork lies inside a WAL segment');
my ($replay_file, $child_file) = split /\|/, psql($child,
	"SELECT pg_walfile_name('$replay_lsn'::pg_lsn + 1), "
	. "pg_walfile_name('$fork_lsn'::pg_lsn)");
is(substr($replay_file, 8), substr($child_file, 8),
	'voter replay and child fork lie in the same segment');
$child->stop('immediate');

$voter->append_conf('postgresql.auto.conf', "primary_conninfo = ''");
$voter->reload;
ok(wait_until($voter, 'SELECT count(*) = 0 FROM pg_stat_wal_receiver'),
	'voter WAL receiver is stopped');
my $voter_wal = $voter->data_dir . '/pg_wal';
copy($history_src, "$voter_wal/$history_name")
  or BAIL_OUT("could not copy child history: $!");
copy($child->data_dir . "/pg_wal/$child_file", "$voter_wal/$child_file")
  or BAIL_OUT("could not copy child WAL: $!");
my $parent_file = '00000002' . substr($child_file, 8);
rename("$voter_wal/$parent_file", "$voter_wal/$parent_file.saved")
  or BAIL_OUT("could not hide parent WAL: $!");

my ($scan_stdout, $scan_stderr);
my $scan_exit = $voter->psql('postgres',
	'SELECT timeline, flush_lsn FROM lwaldump_with_timeline()',
	stdout => \$scan_stdout, stderr => \$scan_stderr);
ok($scan_exit != 0,
	'child segment is rejected until replay actually switches timelines');
like($scan_stderr, qr/replay has not reached target timeline 3/,
	'failure identifies the pending timeline switch');

psql($voter, 'SELECT pg_wal_replay_resume()');
ok(wait_until($voter,
	'SELECT EXISTS (SELECT 1 FROM wal_payload WHERE id = 2)'),
	'PostgreSQL replays the child copy without the parent file');
psql($voter, 'SELECT pg_wal_replay_pause()');
ok(wait_until($voter,
	"SELECT pg_get_wal_replay_pause_state() = 'paused'"),
	'voter pauses after switching to timeline 3');
my ($scan_tli, $scan_end) = split /\|/, psql($voter,
	'SELECT timeline, flush_lsn FROM lwaldump_with_timeline()');
is($scan_tli, '3', 'scan reports the timeline reached by replay');
ok(lsn_ge($voter, $scan_end, $child_end),
	'scan reaches the child-only record after replay switched');

$voter->stop('immediate');
done_testing();
