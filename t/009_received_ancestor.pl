use strict;
use warnings;

use File::Basename qw(basename);
use File::Copy qw(copy);
use FindBin;
use lib $FindBin::RealBin;
use LwaldumpTest qw(new_node psql wait_until lsn_ge);
use Test::More;

sub require_wait
{
	my ($node, $query, $label) = @_;
	my $ready = wait_until($node, $query);
	ok($ready, $label);
	BAIL_OUT($label) unless $ready;
}

my $primary = new_node('received_ancestor_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', "wal_keep_size = '1GB'");
$primary->start;
psql($primary, 'CREATE EXTENSION lwaldump');
psql($primary, 'CREATE TABLE wal_rows (id integer PRIMARY KEY)');
$primary->backup('before_fork', backup_options => ['-X', 'stream']);

my $child = new_node('received_ancestor_child');
$child->init_from_backup($primary, 'before_fork', has_streaming => 1);
$child->append_conf('postgresql.conf', "restore_command = ''");
$child->start;

my $voter = new_node('received_ancestor_voter');
$voter->init_from_backup($primary, 'before_fork', has_streaming => 1);
$voter->append_conf('postgresql.conf', qq(
restore_command = ''
recovery_target_timeline = 'latest'
));
$voter->start;
psql($voter, 'SELECT pg_wal_replay_pause()');

psql($primary, 'INSERT INTO wal_rows VALUES (1)');
require_wait($voter,
	"SELECT pg_get_wal_replay_pause_state() = 'paused'",
	'voter paused before the child fork');
my $before_fork = psql($primary, 'SELECT pg_current_wal_flush_lsn()');
require_wait($child,
	"SELECT COALESCE(pg_last_wal_replay_lsn() >= '$before_fork'::pg_lsn, false)",
	'child replayed before promotion');
$child->promote;
psql($child, 'INSERT INTO wal_rows VALUES (2)');
my $child_end = psql($child, 'SELECT pg_current_wal_flush_lsn()');

# The old primary continues past the child's fork.  The voter receives that
# WAL first, although its explicit recovery target names the child timeline.
psql($primary, 'INSERT INTO wal_rows VALUES (3)');
psql($primary, 'SELECT pg_switch_wal()');
psql($primary, 'INSERT INTO wal_rows VALUES (4)');
my $parent_end = psql($primary, 'SELECT pg_current_wal_flush_lsn()');
my $history = $child->data_dir . '/pg_wal/00000002.history';
copy($history, $voter->data_dir . '/pg_wal/00000002.history')
  or BAIL_OUT("could not copy $history: $!");
my $fork_lsn;
{
	open(my $fh, '<', $history) or BAIL_OUT("could not read $history: $!");
	my $line = <$fh>;
	close($fh);
	$fork_lsn = (split /\s+/, $line)[1];
}
ok(lsn_ge($primary, $parent_end, $fork_lsn) &&
	$parent_end ne $fork_lsn,
	'old primary received WAL beyond the child fork');

is(psql($voter,
	"SELECT pg_last_wal_replay_lsn() < '$fork_lsn'::pg_lsn"),
	't', 'replay is still before the fork');
require_wait($voter,
	"SELECT COALESCE(pg_last_wal_receive_lsn() >= '$parent_end'::pg_lsn, false)",
	'voter received ancestor WAL past the fork');

$primary->stop('immediate');
require_wait($voter, 'SELECT count(*) = 0 FROM pg_stat_wal_receiver',
	'voter WAL receiver stopped');
$child->stop('immediate');
my @child_wal = grep { -f $_ }
  glob($child->data_dir . '/pg_wal/00000002*');
BAIL_OUT('no child timeline WAL') unless @child_wal;
for my $file (@child_wal)
{
	copy($file, $voter->data_dir . '/pg_wal/' . basename($file))
	  or BAIL_OUT("could not copy $file: $!");
}

my ($stdout, $stderr);
my $exit = $voter->psql('postgres',
	'SELECT timeline, flush_lsn FROM lwaldump_with_timeline()',
	stdout => \$stdout, stderr => \$stderr);
ok($exit != 0,
	'ancestor WAL received past the fork makes the live recovery path ambiguous');
like($stderr, qr/could not determine recovery path/,
	'failure identifies the ambiguous recovery path');
like($stderr, qr/replay has not reached target timeline 2/,
	'failure identifies the pending child timeline');

# A fresh startup process can instead choose the child segment from the start.
$voter->stop('immediate');
$voter->append_conf('postgresql.auto.conf', "primary_conninfo = ''");
$voter->start;
require_wait($voter,
	'SELECT count(*) = 1 FROM wal_rows WHERE id = 2',
	'native PostgreSQL replay follows the child WAL');
ok(lsn_ge($voter,
	psql($voter, 'SELECT pg_last_wal_replay_lsn()'), $child_end),
	'native replay reaches the child WAL endpoint');
is(psql($voter, 'SELECT count(*) FROM wal_rows WHERE id = 3'),
	'0', 'native replay excludes old primary WAL after the fork');

$voter->stop('immediate');
done_testing();
