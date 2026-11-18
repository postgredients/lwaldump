# Reproduction: a synchronous standby can flush-acknowledge a transaction
# committed on a new timeline while its own replay has already run past the
# forkpoint along the old timeline.  Promoting that standby loses the
# acknowledged transaction.
#
# The acknowledgement is genuine: the victim's walreceiver receives and
# fsyncs the acknowledged record into its pg_wal (pg_last_wal_receive_lsn
# passes it), and the new primary grants the synchronous commit based on
# that real flushed position -- not on a stale replay LSN remembered from
# the old timeline.  What the victim can never do is replay the record: its
# WAL chain followed the old branch past the forkpoint, so the timeline-2
# continuation at the reader's position is a broken chain.
#
# old_primary (TLI 1) writes a checkpoint mid-segment, then WAL that the
# victim receives while its replay stays paused, so the victim holds
# old-branch WAL up to R, far past the future forkpoint F.  new_primary is
# stopped at F and promoted there (TLI 2), and writes past R so that the
# streamed timeline-2 WAL extends beyond the victim's stuck reader position.
# The archive receives only 00000002.history, no timeline-2 segments.
#
# On restart the victim adopts timeline 2 as its recovery target, but the
# only local copy of the forkpoint segment is the old TLI-1 partial, so
# replay crosses the forkpoint along the old branch.  The walreceiver
# streams and flushes the timeline-2 WAL, including the synchronously
# committed sentinel; the replay can never apply it.  After the failover
# to the victim the acknowledged sentinel row is gone.

use strict;
use warnings;

use POSIX ":sys_wait_h";
use File::Copy qw(copy);
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

sub read_history_first_line
{
	my ($node, $timeline) = @_;
	my $path = $node->data_dir . sprintf('/pg_wal/%08X.history', $timeline);
	open(my $fh, '<', $path) or BAIL_OUT("could not read $path: $!");
	my $line = <$fh>;
	close($fh);
	chomp($line) if defined $line;
	return $line;
}

my $old = new_node('old_primary');
$old->init(allows_streaming => 1, allows_archiving => 1);
$old->append_conf('postgresql.conf', qq(
wal_level = replica
max_wal_senders = 5
wal_keep_size = '256MB'
));
$old->start;

# Begin a fresh segment so that the checkpoint below lands mid-segment: on
# restart the victim's recovery then opens the forkpoint segment's old
# TLI-1 copy before any timeline-2 data can arrive locally, which commits
# the replay to the old branch.
psql($old, 'SELECT pg_switch_wal()');

# The shared archive ("S3"): it will hold the timeline-2 history file, but
# none of the timeline-2 segments.
my $archdir = $old->archive_dir;
mkdir($archdir) unless -d $archdir;

psql($old, 'CREATE TABLE wal_payload (id bigint PRIMARY KEY, payload text)');
psql($old,
	'INSERT INTO wal_payload SELECT g, repeat(md5(g::text), 40) FROM generate_series(1, 200) AS g');
psql($old, 'CHECKPOINT');

$old->backup('before_fork', backup_options => ['-X', 'stream']);

my $new = new_node('new_primary');
$new->init_from_backup($old, 'before_fork', has_streaming => 1,
	has_restoring => 1);
$new->append_conf('postgresql.conf', qq(
hot_standby = on
synchronous_standby_names = 'victim'
));
$new->start;

my $victim = new_node('victim');
$victim->init_from_backup($old, 'before_fork', has_streaming => 1,
	has_restoring => 1);
$victim->append_conf('postgresql.conf', qq(
hot_standby = on
wal_retrieve_retry_interval = '100ms'
));
$victim->start;

# Both standbys replay the checkpoint, then replay is paused: new_primary's
# promotion will fork exactly at its frozen position F.
$old->wait_for_catchup($new);
$old->wait_for_catchup($victim);
psql($new, 'SELECT pg_wal_replay_pause()');
require_wait($new, "SELECT pg_get_wal_replay_pause_state() = 'paused'",
	'new_primary replay is paused');
psql($victim, 'SELECT pg_wal_replay_pause()');
require_wait($victim, "SELECT pg_get_wal_replay_pause_state() = 'paused'",
	'victim replay is paused');
my $pause_lsn = psql($victim, 'SELECT pg_last_wal_replay_lsn()');

# Stop new_primary while it is fully caught up, so that it never receives
# the old-branch WAL generated next.
$new->stop('fast');

# The old branch: the victim receives all of it while its replay stays
# paused, so it ends up with old-branch WAL up to R > F.
psql($old,
	'INSERT INTO wal_payload SELECT g, (SELECT string_agg(md5(g::text || \':\' || n), \'\') FROM generate_series(1, 200) AS n) FROM generate_series(201, 1700) AS g');
psql($old, 'CHECKPOINT');
my $received = psql($old, 'SELECT pg_current_wal_flush_lsn()');
require_wait($victim, "SELECT pg_last_wal_receive_lsn() >= '$received'::pg_lsn",
	'victim received the whole old branch while replay stayed paused', 120);
ok(lsn_ge($victim, $received, $pause_lsn),
	'victim local old-branch WAL extends past the future forkpoint');

$old->stop('fast');

# new_primary comes back with no upstream, replays its local WAL up to F,
# and promotes there: timeline 2 forks at F.
$new->append_conf('postgresql.auto.conf', "primary_conninfo = ''");
$new->start;
require_wait($new, "SELECT pg_last_wal_replay_lsn() >= '$pause_lsn'::pg_lsn",
	'new_primary replayed its local WAL up to the future forkpoint');
$new->promote;
require_wait($new, 'SELECT NOT pg_is_in_recovery()', 'new_primary promoted');

my $hist = read_history_first_line($new, 2);
my ($parent_tli, $fork) = (split /\s+/, $hist)[0, 1];
is($parent_tli, '1', 'timeline 2 forked from timeline 1');
ok(lsn_ge($victim, $received, $fork),
	'forkpoint is before the victim old-branch end');
diag("forkpoint=$fork old_branch_end=$received");

# Timeline 2 writes past R.  This is what makes the acknowledgement below
# genuine: the walreceiver initializes its flush position from the replay
# LSN, so it only fsyncs streamed WAL that lies beyond the replay position;
# the stream must therefore reach past the reader's stuck position.
psql($new,
	'SET synchronous_commit = off; INSERT INTO wal_payload SELECT g, (SELECT string_agg(md5(g::text || \':\' || n), \'\') FROM generate_series(1, 300) AS n) FROM generate_series(3001, 4500) AS g');
my $new_flush = psql($new, 'SELECT pg_current_wal_flush_lsn()');
ok(lsn_ge($new, $new_flush, $received), 'timeline-2 WAL extends past R');

# The sentinel: a synchronous commit that blocks until the victim flushes
# the record.  It has to run in the background, because the victim is only
# restarted afterwards.
my $sentinel_out = $new->basedir . '/sentinel.out';
my $sentinel_pid = fork();
BAIL_OUT("could not fork: $!") unless defined $sentinel_pid;
if ($sentinel_pid == 0)
{
	open(STDOUT, '>', $sentinel_out) or die "could not write $sentinel_out: $!";
	open(STDERR, '>&', STDOUT) or die "could not redirect stderr: $!";
	exec('psql', '-X', '-q', '-v', 'ON_ERROR_STOP=1',
		'-h', $new->host, '-p', $new->port, '-d', 'postgres',
		'-c', "SET synchronous_commit = on; INSERT INTO wal_payload VALUES (999999, 'acknowledged on timeline 2')")
	  or die "could not exec psql: $!";
}

# "S3" receives the history file, but no timeline-2 segments.  The failover
# manager repoints the victim at the new primary; application_name must be
# set explicitly, since the new primary only acknowledges its synchronous
# standby 'victim'.
copy($new->data_dir . '/pg_wal/00000002.history', "$archdir/00000002.history")
  or BAIL_OUT('could not publish the timeline-2 history file');
$victim->append_conf('postgresql.auto.conf',
	"primary_conninfo = '" . $new->connstr . " application_name=victim'");

# The victim restarts: startup adopts timeline 2 as its recovery target, but
# the only local copy of the forkpoint segment is the old TLI-1 partial, so
# replay crosses the forkpoint along the old branch.
$victim->stop('immediate');
$victim->start;
require_wait($victim, "SELECT pg_last_wal_replay_lsn() >= '$fork'::pg_lsn",
	'victim replay crossed the forkpoint on the old branch', 120);

# Freeze the replay mid-old-branch while the walreceiver streams and flushes
# the timeline-2 WAL: the paused reader produces no read failures, so the
# recovery loop cannot kill the walreceiver, and the acknowledgement can be
# observed in a stable state.
psql($victim, 'SELECT pg_wal_replay_pause()');
require_wait($victim, "SELECT pg_get_wal_replay_pause_state() = 'paused'",
	'victim replay paused past the forkpoint');

require_wait($victim, 'SELECT count(*) = 1 FROM pg_stat_wal_receiver',
	'victim walreceiver reconnected to the timeline-2 primary', 60);
require_wait($new,
	"SELECT (SELECT sync_state FROM pg_stat_replication WHERE application_name = 'victim') = 'sync'",
	'victim is a synchronous standby of timeline 2', 60);

# THE synchronous commit: it returns only after the victim's walreceiver
# actually flushed the record to disk -- a genuine durable ACK.
my $ack_tries = 0;
my $ack_status;
my $acked_ok = 0;
while (1)
{
	my $r = waitpid($sentinel_pid, WNOHANG);
	if ($r == $sentinel_pid)
	{
		$ack_status = $?;
		$acked_ok = ($? == 0);
		last;
	}
	last if ++$ack_tries >= 60;
	sleep(1);
}
if (!$acked_ok)
{
	kill('TERM', $sentinel_pid);
	waitpid($sentinel_pid, 0);
	open(my $sf, '<', $sentinel_out) or die $!;
	my $sentinel_stderr = do { local $/; <$sf> };
	close($sf);
	diag("sentinel psql output: $sentinel_stderr");
}
ok($acked_ok, 'synchronous commit on timeline 2 was acknowledged');
my $acked_lsn = psql($new, 'SELECT pg_current_wal_flush_lsn()');

# The victim durably flushed the acknowledged record: this is what makes the
# ACK genuine rather than a leftover replay position from the old timeline.
require_wait($victim, "SELECT pg_last_wal_receive_lsn() >= '$acked_lsn'::pg_lsn",
	'victim flushed the acknowledged record to disk', 60);
require_wait($new,
	"SELECT (SELECT flush_lsn FROM pg_stat_replication WHERE application_name = 'victim') >= '$acked_lsn'::pg_lsn",
	'walsender saw the victim flush the acknowledged record', 60);
diag('victim pg_stat_wal_receiver='
	. psql($victim, 'SELECT status, flushed_lsn, received_tli FROM pg_stat_wal_receiver'));
diag("acked_lsn=$acked_lsn");

# Let the replay run to the end of the old branch: the timeline-2 WAL that
# arrived in the meantime is a broken continuation there, so the replay gets
# stuck for good.  From here the recovery loop starts killing the
# walreceiver again, but the acknowledged record is already durable.
psql($victim, 'SELECT pg_wal_replay_resume()');
require_wait($victim,
	"SELECT pg_wal_lsn_diff('$received', pg_last_wal_replay_lsn()) < 8192",
	'victim replayed the whole old branch up to R', 120);
my $blown = psql($victim, 'SELECT pg_last_wal_replay_lsn()');
ok(lsn_ge($victim, $blown, $fork), 'replay position is past the forkpoint');
is(psql($victim, 'SELECT count(*) FROM wal_payload WHERE id BETWEEN 201 AND 1700'),
	'1500', 'victim replayed the divergent old-branch rows');

# The replay is stuck at R forever and never reaches the acknowledged
# record, which lies further up the timeline-2 WAL.
my $replay_stuck = psql($victim, 'SELECT pg_last_wal_replay_lsn()');
sleep(10);
is(psql($victim, 'SELECT pg_last_wal_replay_lsn()'), $replay_stuck,
	'victim replay did not advance');
ok(lsn_ge($victim, $acked_lsn, $replay_stuck),
	'replay never reached the acknowledged record');
is(psql($victim, 'SELECT count(*) FROM wal_payload WHERE id = 999999'), '0',
	'acknowledged row is not visible on the victim');

# Soft evidence: the rejected timeline-2 continuation shows up in the log.
my @cands = eval { $victim->logfile } || ();
push @cands, glob($victim->basedir . '/*.log');
my $logpath;
for my $c (@cands)
{
	next unless defined $c && -f $c;
	$logpath = $c;
	last;
}
if (defined $logpath)
{
	open(my $lf, '<', $logpath) or die "could not read $logpath: $!";
	my $content = do { local $/; <$lf> };
	close($lf);
	my @bad = grep { /waiting for WAL|prev-link|invalid record|invalid resource manager/ }
	  split(/\n/, $content);
	diag("victim log ($logpath), last relevant lines:");
	my $first = @bad - 4;
	$first = 0 if $first < 0;
	diag("  $_") for @bad[$first .. $#bad];
}
else
{
	diag('victim log not found');
}

# Failover to the standby that acknowledged the transaction.  The primary
# dies first, then the naive promotion attempt aborts the victim: at the end
# of recovery it must read the record at R, and the only copy at that
# position is the broken timeline-2 continuation it received.
$new->stop('fast');
my $promote_rc = system('pg_ctl', '-D', $victim->data_dir, 'promote');
diag("pg_ctl promote rc=$promote_rc") if $promote_rc != 0;
my $crashed = 0;
if (defined $logpath)
{
	for (1 .. 30)
	{
		open(my $lf, '<', $logpath) or last;
		my $content = do { local $/; <$lf> };
		close($lf);
		if ($content =~ /PANIC:  invalid resource manager ID/
			&& $content =~ /shutting down due to startup process failure/)
		{
			$crashed = 1;
			last;
		}
		sleep(1);
	}
}
ok($crashed, 'naive promotion aborts the victim on the broken timeline-2 continuation');

# Operator recovery: remove the poisoned timeline-2 segment copies and
# promote the old branch.  The acknowledged transaction is not on it.
# First wait for the aborted postmaster to exit completely.
my $pidfile = $victim->data_dir . '/postmaster.pid';
my $exited = 0;
for (1 .. 30)
{
	if (!-f $pidfile)
	{
		$exited = 1;
		last;
	}
	sleep(1);
}
BAIL_OUT('aborted victim postmaster did not exit') unless $exited;
# The postmaster exited outside of the framework's control; clear its
# bookkeeping before restarting the node.
$victim->{_pid} = undef;

# The blow-past advanced the minimum recovery point past the forkpoint on
# timeline 1, so the victim can never adopt timeline 2 as its target again:
# the operator pins it to the branch it actually has.
$victim->append_conf('postgresql.auto.conf',
	"recovery_target_timeline = 'current'");

my $victim_wal = $victim->data_dir . '/pg_wal';
opendir(my $waldir, $victim_wal) or BAIL_OUT("could not open $victim_wal: $!");
my @poisoned = grep { /^[0-9A-F]{24}$/ && $_ ge '000000020000000000000000' }
  readdir($waldir);
closedir($waldir);
diag('removed poisoned segments: ' . join(' ', @poisoned));
for my $seg (@poisoned)
{
	unlink("$victim_wal/$seg") or BAIL_OUT("could not remove $seg: $!");
}

$victim->start;
require_wait($victim,
	"SELECT pg_wal_lsn_diff('$received', pg_last_wal_replay_lsn()) < 8192",
	'victim replayed the old branch again after cleanup', 120);
$victim->promote;
require_wait($victim, 'SELECT NOT pg_is_in_recovery()', 'victim promoted');

is(psql($victim, 'SELECT count(*) FROM wal_payload WHERE id = 999999'), '0',
	'synchronously acknowledged transaction is LOST after failover');
is(psql($victim, 'SELECT count(*) FROM wal_payload WHERE id BETWEEN 201 AND 1700'),
	'1500', 'victim kept the divergent old branch');
my $victim_hist = read_history_first_line($victim, 3);
is((split /\s+/, $victim_hist)[0], '1',
	'the promoted victim forked from the old timeline 1');

done_testing();
