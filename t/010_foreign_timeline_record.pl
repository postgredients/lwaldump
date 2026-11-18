use strict;
use warnings;

use File::Copy qw(copy);
use FindBin;
use lib $FindBin::Bin;
use LwaldumpTest qw(new_node psql wait_until);
use Test::More;

sub require_wait
{
	my ($node, $query, $label) = @_;
	my $ready = wait_until($node, $query);
	ok($ready, $label);
	BAIL_OUT($label) unless $ready;
}

my $primary = new_node('foreign_timeline_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', "wal_keep_size = '1GB'");
$primary->start;
psql($primary, 'CREATE EXTENSION lwaldump');
psql($primary, 'CREATE TABLE wal_rows (id integer PRIMARY KEY)');
$primary->backup('before_fork', backup_options => ['-X', 'stream']);

my $child = new_node('foreign_timeline_child');
$child->init_from_backup($primary, 'before_fork', has_streaming => 1);
$child->start;

my $voter = new_node('foreign_timeline_voter');
$voter->init_from_backup($primary, 'before_fork', has_streaming => 1);
$voter->append_conf('postgresql.auto.conf', "primary_conninfo = ''");
$voter->append_conf('postgresql.conf', qq(
restore_command = ''
recovery_target_timeline = '1'
recovery_min_apply_delay = '1h'
));

psql($primary, 'INSERT INTO wal_rows VALUES (1)');
my $pre_fork = psql($primary, 'SELECT pg_current_wal_flush_lsn()');
require_wait($child,
	"SELECT COALESCE(pg_last_wal_replay_lsn() >= '$pre_fork'::pg_lsn, false)",
	'child replayed parent WAL before promotion');
$child->promote;
psql($child, 'INSERT INTO wal_rows VALUES (2)');

my $history = $child->data_dir . '/pg_wal/00000002.history';
open(my $history_file, '<', $history)
  or BAIL_OUT("could not read $history: $!");
my $history_line = <$history_file>;
close($history_file);
my $fork_lsn = (split /\s+/, $history_line)[1];
my $parent_name = psql($primary,
	"SELECT pg_walfile_name('$fork_lsn'::pg_lsn + 1)");
my $child_name = '00000002' . substr($parent_name, 8);
$child->stop('immediate');
copy($child->data_dir . "/pg_wal/$child_name",
	$voter->data_dir . "/pg_wal/$parent_name")
  or BAIL_OUT("could not copy child WAL under parent name: $!");

# The segment has this cluster's system ID and valid record CRCs, but its
# promotion record switches to TLI 2 inside a page still marked TLI 1.
$voter->start;
psql($voter, 'SELECT pg_wal_replay_pause()');
psql($voter, "ALTER SYSTEM SET recovery_min_apply_delay = '0'");
psql($voter, 'SELECT pg_reload_conf()');
require_wait($voter,
	"SELECT pg_get_wal_replay_pause_state() = 'paused'",
	'voter paused before the foreign timeline switch');
is(psql($voter,
	"SELECT pg_last_wal_replay_lsn() < '$fork_lsn'::pg_lsn"),
	't', 'replay remains on the parent timeline before the fork');
is(psql($voter, 'SELECT count(*) FROM pg_stat_wal_receiver'),
	'0', 'voter has no WAL receiver');

my ($stdout, $stderr);
my $exit = $voter->psql('postgres', 'SELECT lwaldump()',
	stdout => \$stdout, stderr => \$stderr);
ok($exit != 0, 'promotion record on a foreign timeline is rejected');
like($stderr, qr/timeline 2 outside the recovery path/i,
	'failure identifies the foreign record timeline');

$voter->stop('immediate');
$primary->stop('immediate');
done_testing();
