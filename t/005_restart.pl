use strict;
use warnings;

use FindBin;
use lib $FindBin::Bin;
use LwaldumpTest qw(new_node psql wait_until lsn_ge);
use Test::More;

my $primary = new_node('restart_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', "wal_keep_size = '1GB'");
$primary->start;
psql($primary, 'CREATE EXTENSION lwaldump');
psql($primary, 'CREATE TABLE wal_payload (id integer PRIMARY KEY, payload text)');
$primary->backup('before_wal', backup_options => ['-X', 'stream']);

my $standby = new_node('restart_standby');
$standby->init_from_backup($primary, 'before_wal', has_streaming => 1);
$standby->append_conf('postgresql.conf', "restore_command = ''");
$standby->start;

psql($standby, 'SELECT pg_wal_replay_pause()');
ok(wait_until($standby,
	"SELECT pg_get_wal_replay_pause_state() = 'paused'"),
	'replay is paused before new WAL arrives');
my $replay_before = psql($standby, 'SELECT pg_last_wal_replay_lsn()');

psql($primary, 'SELECT pg_switch_wal()');
my $write_start = psql($primary, 'SELECT pg_current_wal_insert_lsn()');
psql($primary,
	"INSERT INTO wal_payload SELECT g, repeat(md5(g::text), 8) "
	. "FROM generate_series(1, 12000) AS g");
my $received = psql($primary, 'SELECT pg_current_wal_flush_lsn()');
my $next_file = psql($primary,
	"SELECT pg_walfile_name('$received'::pg_lsn + "
	. "pg_size_bytes(current_setting('wal_segment_size'))) ");
my ($corrupt_lsn, $corrupt_file, $corrupt_offset) = split /\|/,
	psql($primary,
		"SELECT '$write_start'::pg_lsn + 1048576, "
		. "pg_walfile_name('$write_start'::pg_lsn + 1048576), "
		. "pg_wal_lsn_diff('$write_start'::pg_lsn + 1048576, '0/0')::numeric "
		. "% pg_size_bytes(current_setting('wal_segment_size'))");
ok(lsn_ge($primary, $received, $corrupt_lsn),
	'WAL payload extends past the future corruption point');
ok(wait_until($standby,
	"SELECT COALESCE(pg_last_wal_receive_lsn() >= '$received'::pg_lsn, false)"),
	'standby received the entire payload');

$primary->stop('fast');
ok(wait_until($standby, 'SELECT count(*) = 0 FROM pg_stat_wal_receiver'),
	'WAL receiver stopped');
my $baseline = psql($standby, 'SELECT lwaldump()');
ok(lsn_ge($standby, $baseline, $received),
	'original scan reaches the received WAL');

# Restarting removes the walreceiver's shared-memory flush position. Keep the
# payload segment aside until replay pause has been requested again.
$standby->append_conf('postgresql.auto.conf', qq(
primary_conninfo = ''
));
$standby->stop('immediate');
my $path = $standby->data_dir . "/pg_wal/$corrupt_file";
rename($path, "$path.saved")
  or BAIL_OUT("could not hide $path before restart: $!");
$standby->start;
psql($standby, 'SELECT pg_wal_replay_pause()');
rename("$path.saved", $path)
  or BAIL_OUT("could not restore $path after restart: $!");
ok(wait_until($standby,
	"SELECT pg_get_wal_replay_pause_state() = 'paused'"),
	'replay pauses again after restart');
my $replay_after = psql($standby, 'SELECT pg_last_wal_replay_lsn()');
is(psql($standby, "SELECT '$replay_after'::pg_lsn < '$corrupt_lsn'::pg_lsn"),
	't', 'the corruption point is beyond resumed replay');
$baseline = psql($standby, 'SELECT lwaldump()');
ok(lsn_ge($standby, $baseline, $received),
	'scan still reaches received WAL after restart');

# An unwritten page has a zero header and a zero body.  A torn or corrupted
# page with a zero header but nonzero body must not be accepted as EOF after
# restart has removed the walreceiver's remembered flush position.
my ($baseline_offset, $segment_size) = split /\|/,
	psql($standby,
		"SELECT pg_wal_lsn_diff('$baseline', '0/0')::numeric "
		. "% pg_size_bytes(current_setting('wal_segment_size')), "
		. "pg_size_bytes(current_setting('wal_segment_size'))");
is(psql($standby,
	"SELECT floor(pg_wal_lsn_diff('$baseline', '0/0')::numeric "
	. "/ pg_size_bytes(current_setting('wal_segment_size'))) = "
	. "floor(pg_wal_lsn_diff('$write_start', '0/0')::numeric "
	. "/ pg_size_bytes(current_setting('wal_segment_size')))"),
	't', 'the endpoint remains in the payload segment');
my $tail_page;
open(my $tail_wal, '+<', $path) or BAIL_OUT("could not open $path: $!");
binmode($tail_wal);
for (my $offset = (int($baseline_offset / 8192) + 1) * 8192;
	$offset + 8192 <= $segment_size; $offset += 8192)
{
	seek($tail_wal, $offset, 0)
	  or BAIL_OUT("could not seek in $path: $!");
	read($tail_wal, my $page, 8192) == 8192
	  or BAIL_OUT("could not read full page in $path: $!");
	if ($page eq "\0" x 8192)
	{
		$tail_page = $offset;
		last;
	}
}
ok(defined $tail_page, 'there is an unwritten page after the endpoint');
BAIL_OUT('no unwritten page for corruption test') unless defined $tail_page;
seek($tail_wal, $tail_page + 64, 0)
  or BAIL_OUT("could not seek in $path: $!");
print {$tail_wal} "X" or BAIL_OUT("could not alter $path: $!");
close($tail_wal) or BAIL_OUT("could not close $path: $!");

my ($tail_stdout, $tail_stderr);
my $tail_exit = $standby->psql('postgres', 'SELECT lwaldump()',
	stdout => \$tail_stdout, stderr => \$tail_stderr);
ok($tail_exit != 0, 'nonzero body after a zero page header is rejected');
like($tail_stderr, qr/could not scan WAL/i,
	'zero-header corruption reports a scan failure');
open($tail_wal, '+<', $path) or BAIL_OUT("could not open $path: $!");
binmode($tail_wal);
seek($tail_wal, $tail_page + 64, 0)
  or BAIL_OUT("could not seek in $path: $!");
print {$tail_wal} "\0" or BAIL_OUT("could not restore $path: $!");
close($tail_wal) or BAIL_OUT("could not close $path: $!");
is(psql($standby, 'SELECT lwaldump()'), $baseline,
	'scan succeeds again after restoring the empty page');

# A zero long header is a missing segment only when the whole segment is zero.
# Place one byte in its second page to catch scans that inspect only page one.
my $next_path = $standby->data_dir . "/pg_wal/$next_file";
my $had_next_file = -e $next_path;
rename($next_path, "$next_path.saved")
  or BAIL_OUT("could not save $next_path: $!") if $had_next_file;
open(my $next_wal, '>', $next_path)
  or BAIL_OUT("could not create $next_path: $!");
binmode($next_wal);
truncate($next_wal, $segment_size)
  or BAIL_OUT("could not size $next_path: $!");
seek($next_wal, 8192 + 64, 0)
  or BAIL_OUT("could not seek in $next_path: $!");
print {$next_wal} "X" or BAIL_OUT("could not alter $next_path: $!");
close($next_wal) or BAIL_OUT("could not close $next_path: $!");

my ($segment_stdout, $segment_stderr);
my $segment_exit = $standby->psql('postgres', 'SELECT lwaldump()',
	stdout => \$segment_stdout, stderr => \$segment_stderr);
ok($segment_exit != 0, 'nonzero body after a zero segment header is rejected');
like($segment_stderr, qr/nonzero data after zero WAL segment header/i,
	'failure identifies the corrupt zero-header segment');
unlink($next_path) or BAIL_OUT("could not remove $next_path: $!");
rename("$next_path.saved", $next_path)
  or BAIL_OUT("could not restore $next_path: $!") if $had_next_file;
is(psql($standby, 'SELECT lwaldump()'), $baseline,
	'scan succeeds again after restoring the next segment');

open(my $wal, '+<', $path) or BAIL_OUT("could not open $path: $!");
binmode($wal);
seek($wal, $corrupt_offset, 0) or BAIL_OUT("could not seek in $path: $!");
read($wal, my $byte, 1) == 1 or BAIL_OUT("could not read $path: $!");
seek($wal, $corrupt_offset, 0) or BAIL_OUT("could not seek in $path: $!");
print {$wal} chr(ord($byte) ^ 1) or BAIL_OUT("could not alter $path: $!");
close($wal) or BAIL_OUT("could not close $path: $!");

my ($stdout, $stderr);
my $exit = $standby->psql('postgres', 'SELECT lwaldump()',
	stdout => \$stdout, stderr => \$stderr);
ok($exit != 0, 'corrupt WAL after restart is rejected');
like($stderr, qr/could not scan WAL/i,
	'failure identifies the corrupt WAL');

$standby->stop('immediate');
done_testing();
