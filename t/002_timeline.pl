use strict;
use warnings;

use File::Copy qw(copy);
use File::Basename qw(basename);
use FindBin;
use lib $FindBin::RealBin;
use LwaldumpTest qw(new_node psql wait_until lsn_ge);
use Test::More;

sub require_wait
{
	my ($node, $query, $label, $timeout) = @_;
	my $ready = wait_until($node, $query, 't', $timeout // 60);
	ok($ready, $label);
	BAIL_OUT($label) unless $ready;
}

sub copy_matching_wal
{
	my ($source, $target, $prefix) = @_;
	my @files = glob($source->data_dir . "/pg_wal/$prefix*");
	@files = grep { -f $_ } @files;
	BAIL_OUT("no WAL files with prefix $prefix") unless @files;
	for my $file (@files)
	{
		copy($file, $target->data_dir . '/pg_wal/' . basename($file))
		  or BAIL_OUT("could not copy $file: $!");
	}
}

sub read_history
{
	my ($node, $timeline) = @_;
	my $name = sprintf('%08X.history', $timeline);
	my $path = $node->data_dir . "/pg_wal/$name";
	open(my $fh, '<', $path) or BAIL_OUT("could not read $path: $!");
	my @lines = <$fh>;
	close($fh);
	return @lines;
}

sub expect_path_error
{
	my ($node, $label, $detail) = @_;
	my ($stdout, $stderr);
	my $exit = $node->psql(
		'postgres', 'SELECT timeline, flush_lsn FROM lwaldump_with_timeline()',
		stdout => \$stdout, stderr => \$stderr);
	ok($exit != 0, "$label is rejected");
	like($stderr, qr/could not determine recovery path/,
		"$label reports an ambiguous recovery path");
	like($stderr, $detail, "$label identifies the conflicting WAL")
	  if defined $detail;
}

my $primary = new_node('timeline_primary');
$primary->init;
$primary->append_conf('postgresql.conf', qq(
wal_level = replica
max_wal_senders = 5
wal_keep_size = '1GB'
));
$primary->start;
psql($primary, 'CREATE EXTENSION lwaldump');
psql($primary, 'CREATE TABLE wal_payload (id bigint PRIMARY KEY, payload text)');
$primary->backup('before_fork', backup_options => ['-X', 'stream']);

my $candidate = new_node('timeline_candidate');
$candidate->init_from_backup($primary, 'before_fork', has_streaming => 1);
$candidate->append_conf('postgresql.conf', qq(
hot_standby = on
restore_command = ''
wal_keep_size = '1GB'
));
$candidate->start;

my $voter = new_node('timeline_voter');
$voter->init_from_backup($primary, 'before_fork', has_streaming => 1);
$voter->append_conf('postgresql.auto.conf', "primary_conninfo = ''");

psql($primary,
	'INSERT INTO wal_payload SELECT g, repeat(md5(g::text), 12) FROM generate_series(1, 50000) AS g');
psql($primary, 'SELECT pg_switch_wal()');
my $fork_lsn = psql($primary, 'SELECT pg_current_wal_flush_lsn()');
is(psql($primary,
	"SELECT pg_wal_lsn_diff('$fork_lsn', '0/0')::numeric % pg_size_bytes(current_setting('wal_segment_size'))"),
	'0', 'fork LSN is on a WAL segment boundary');
require_wait($candidate,
	"SELECT pg_last_wal_replay_lsn() >= '$fork_lsn'::pg_lsn",
	'candidate replayed through the future fork');

# Restart as a disconnected standby and promote it to timeline 2.
$candidate->stop('immediate');
$candidate->append_conf('postgresql.conf', "primary_conninfo = ''");
$primary->stop('fast');
$candidate->start;
$candidate->promote;
psql($candidate,
	'INSERT INTO wal_payload SELECT g, repeat(md5(g::text), 12) FROM generate_series(50001, 51000) AS g');
$candidate->backup('timeline2_base', backup_options => ['-X', 'stream']);

my $branch3 = new_node('timeline_branch3');
$branch3->init_from_backup($candidate, 'timeline2_base', has_streaming => 1);
my $branch4 = new_node('timeline_branch4');
$branch4->init_from_backup($candidate, 'timeline2_base', has_streaming => 1);
my $replay_voter = new_node('timeline_replay_voter');
$replay_voter->init_from_backup($candidate, 'timeline2_base', has_streaming => 1);
$replay_voter->append_conf('postgresql.conf', qq(
hot_standby = on
restore_command = ''
recovery_target_timeline = 'latest'
));
$replay_voter->start;
psql($replay_voter, 'SELECT pg_wal_replay_pause()');
require_wait($replay_voter,
	"SELECT pg_get_wal_replay_pause_state() = 'paused'",
	'replay voter is actually paused');

psql($candidate,
	'INSERT INTO wal_payload SELECT g, repeat(md5(g::text), 12) FROM generate_series(52000, 53000) AS g');
psql($candidate, 'SELECT pg_switch_wal()');
my $replay_source_end = psql($candidate, 'SELECT pg_current_wal_flush_lsn()');
require_wait($replay_voter,
	"SELECT COALESCE(pg_last_wal_receive_lsn() >= '$replay_source_end'::pg_lsn, false)",
	'replay voter received WAL through timeline 2');
my $timeline2_end = psql($candidate, 'SELECT pg_current_wal_flush_lsn()');

my $pitr_voter = new_node('timeline_pitr_voter');
$pitr_voter->init_from_backup($primary, 'before_fork', has_streaming => 1);
$pitr_voter->enable_streaming($candidate);
$pitr_voter->append_conf('postgresql.conf', qq(
hot_standby = on
restore_command = ''
recovery_target_timeline = 'latest'
recovery_target_lsn = '$fork_lsn'
recovery_target_action = 'pause'
));
$pitr_voter->start;
require_wait($pitr_voter,
	"SELECT pg_get_wal_replay_pause_state() = 'paused'",
	'PITR voter paused at its recovery target');

$voter->append_conf('postgresql.conf', qq(
hot_standby = on
restore_command = ''
recovery_target_timeline = 'latest'
recovery_min_apply_delay = '15s'
));
$voter->start;
psql($voter, 'SELECT pg_wal_replay_pause()');
require_wait($voter,
	"SELECT pg_get_wal_replay_pause_state() = 'paused'",
	'voter paused before the timeline fork');

$branch3->append_conf('postgresql.conf', qq(
hot_standby = on
restore_command = ''
recovery_target_timeline = 'current'
));
$branch3->start;
require_wait($branch3,
	"SELECT COALESCE(pg_last_wal_replay_lsn() >= '$replay_source_end'::pg_lsn, false)",
	'future timeline 3 replayed through its fork');
# Freeze the parent before promoting a child. Immediate shutdown adds no
# shutdown checkpoint WAL beyond the future forkpoint.
psql($candidate, 'SELECT pg_switch_wal()');
$timeline2_end = psql($candidate, 'SELECT pg_current_wal_flush_lsn()');
require_wait($branch3,
	"SELECT COALESCE(pg_last_wal_receive_lsn() >= '$timeline2_end'::pg_lsn, false)",
	'future timeline 3 received the frozen parent WAL');
require_wait($replay_voter,
	"SELECT COALESCE(pg_last_wal_receive_lsn() >= '$timeline2_end'::pg_lsn, false)",
	'replay voter received the frozen parent WAL');
$candidate->stop('immediate');
copy_matching_wal($candidate, $voter, '00000001');
copy_matching_wal($candidate, $voter, '00000002');
$branch3->promote;
psql($branch3,
	"INSERT INTO wal_payload VALUES (51001, repeat(md5('branch3'), 12))");
my $timeline3_end = psql($branch3, 'SELECT pg_current_wal_flush_lsn()');
$branch3->stop('fast');

# Force a real sibling TLI 4 by making the second standby aware of TLI 3,
# while asking it to promote its current TLI 2 rather than follow TLI 3.
my $history3 = $branch3->data_dir . '/pg_wal/00000003.history';
copy($history3, $branch4->data_dir . '/pg_wal/00000003.history')
  or BAIL_OUT("could not copy timeline 3 history: $!");
$branch4->append_conf('postgresql.auto.conf', "primary_conninfo = ''");
$branch4->append_conf('postgresql.conf', qq(
hot_standby = on
restore_command = ''
recovery_target_timeline = 'current'
));
$branch4->start;
$branch4->promote;
psql($branch4,
	"INSERT INTO wal_payload VALUES (51002, repeat(md5('branch4'), 12))");
my $timeline4_end = psql($branch4, 'SELECT pg_current_wal_flush_lsn()');
$branch4->stop('fast');

# Fence every source before reading local pg_wal. The walreceiver has to exit
# before the extension scans, and no archive restore is configured.
$voter->append_conf('postgresql.auto.conf', "primary_conninfo = ''");
$replay_voter->append_conf('postgresql.auto.conf', "primary_conninfo = ''");
$pitr_voter->append_conf('postgresql.auto.conf', "primary_conninfo = ''");
$voter->reload;
$replay_voter->reload;
$pitr_voter->reload;
require_wait($voter,
	'SELECT count(*) = 0 FROM pg_stat_wal_receiver',
	'voter walreceiver is fenced', 20);
require_wait($replay_voter,
	'SELECT count(*) = 0 FROM pg_stat_wal_receiver',
	'replay voter walreceiver is fenced', 20);
require_wait($pitr_voter,
	'SELECT count(*) = 0 FROM pg_stat_wal_receiver',
	'PITR voter walreceiver is fenced', 20);

my ($target_stdout, $target_stderr);
my $target_exit = $pitr_voter->psql('postgres',
	'SELECT timeline, flush_lsn FROM lwaldump_with_timeline()',
	stdout => \$target_stdout, stderr => \$target_stderr);
ok($target_exit != 0, 'active PITR target is rejected');
like($target_stderr, qr/point-in-time recovery target/,
	'PITR failure explains the replay limit');

my @history3 = read_history($branch3, 3);
my @history4 = read_history($branch4, 4);
is((split /\s+/, $history3[-1])[0], '2', 'timeline 3 branched from 2');
is((split /\s+/, $history4[-1])[0], '2', 'timeline 4 branched from 2');
copy_matching_wal($branch3, $voter, '00000003');
copy_matching_wal($branch3, $replay_voter, '00000003');

expect_path_error($replay_voter, 'pending timeline 3 switch',
	qr/replay has not reached target timeline 3/);

psql($replay_voter, 'SELECT pg_wal_replay_resume()');
require_wait($replay_voter,
	'SELECT EXISTS (SELECT 1 FROM wal_payload WHERE id = 51001)',
	'PostgreSQL replayed the timeline 3 record');
is(psql($replay_voter, 'SELECT pg_is_in_recovery()'), 't',
	'replay voter remains in recovery');
psql($replay_voter, 'SELECT pg_wal_replay_pause()');
require_wait($replay_voter,
	"SELECT pg_get_wal_replay_pause_state() = 'paused'",
	'replay voter paused after switching to timeline 3');
my ($replay_tli, $replay_end) = split /\|/,
	psql($replay_voter,
		'SELECT timeline, flush_lsn FROM lwaldump_with_timeline()');
is($replay_tli, '3', 'paused standby reports timeline 3 after replay switched');
ok(lsn_ge($replay_voter, $replay_end, $timeline3_end),
	'local scan covers timeline 3 WAL after replay switched');

my @history2 = read_history($voter, 2);
my $switchpoint = (split /\s+/, $history2[0])[1];
is(psql($voter,
	"SELECT pg_wal_lsn_diff('$switchpoint', '0/0')::numeric % pg_size_bytes(current_setting('wal_segment_size'))"),
	'0', 'timeline 2 switchpoint is on a WAL segment boundary');
expect_path_error($voter, 'pending timeline 3 switch',
	qr/replay has not reached target timeline 3/);

my $voter_wal = $voter->data_dir . '/pg_wal';
copy_matching_wal($branch4, $voter, '00000004');
expect_path_error($voter, 'sibling timelines 3 and 4');

# Rewrite TLI 4 as a descendant of 3 and add an unrelated newer history.
copy($history3, "$voter_wal/00000004.history")
  or BAIL_OUT("could not rewrite timeline 4 history: $!");
open(my $hist4, '>>', "$voter_wal/00000004.history")
  or BAIL_OUT("could not append timeline 4 history: $!");
print {$hist4} "3\t$timeline3_end\tdescendant timeline\n";
close($hist4);
open(my $hist5, '>', "$voter_wal/00000005.history")
  or BAIL_OUT("could not write timeline 5 history: $!");
print {$hist5} "1\t$switchpoint\tunrelated newest timeline\n";
close($hist5);
expect_path_error($voter, 'incompatible newest timeline 5');
unlink("$voter_wal/00000004.history", "$voter_wal/00000005.history") == 2
  or BAIL_OUT("could not remove synthetic histories: $!");

# PostgreSQL parsed this postmaster GUC using base 0. The extension should
# consume its numeric value rather than parse the text a second time.
$voter->stop('immediate');
$voter->append_conf('postgresql.auto.conf', "recovery_target_timeline = '0x2'");
$voter->start;
psql($voter, 'SELECT pg_wal_replay_pause()');
require_wait($voter,
	"SELECT pg_get_wal_replay_pause_state() = 'paused'",
	'numeric voter is paused after restart');
expect_path_error($voter, 'numeric recovery target 0x2',
	qr/replay has not reached target timeline 2/);

done_testing();
