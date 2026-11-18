use strict;
use warnings;

use FindBin;
use lib $FindBin::Bin;
use LwaldumpTest qw(new_node psql wait_until lsn_ge);
use Test::More;

my $primary = new_node('foreign_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', "wal_keep_size = '1GB'");
$primary->start;
psql($primary, 'CREATE EXTENSION lwaldump');
psql($primary, 'CREATE TABLE wal_payload (id integer PRIMARY KEY)');
$primary->backup('before_wal', backup_options => ['-X', 'stream']);

my $standby = new_node('foreign_standby');
$standby->init_from_backup($primary, 'before_wal', has_streaming => 1);
$standby->append_conf('postgresql.conf', "restore_command = ''");
$standby->start;

psql($primary, 'INSERT INTO wal_payload VALUES (1)');
my $replay_target = psql($primary, 'SELECT pg_current_wal_flush_lsn()');
ok(wait_until($standby,
	"SELECT COALESCE(pg_last_wal_replay_lsn() >= '$replay_target'::pg_lsn, false)"),
	'standby replayed the first transaction');

psql($standby, 'SELECT pg_wal_replay_pause()');
ok(wait_until($standby,
	"SELECT pg_get_wal_replay_pause_state() = 'paused'"),
	'replay is paused');
my $replay_lsn = psql($standby, 'SELECT pg_last_wal_replay_lsn()');
isnt(psql($standby,
	"SELECT pg_wal_lsn_diff('$replay_lsn', '0/0')::numeric % pg_size_bytes(current_setting('wal_segment_size'))"),
	'0', 'replay starts inside a WAL segment');
my $replay_file = psql($primary,
	"SELECT pg_walfile_name('$replay_lsn'::pg_lsn)");

psql($primary,
	'INSERT INTO wal_payload SELECT g FROM generate_series(2, 1000) AS g');
my $receive_target = psql($primary, 'SELECT pg_current_wal_flush_lsn()');
ok(wait_until($standby,
	"SELECT COALESCE(pg_last_wal_receive_lsn() >= '$receive_target'::pg_lsn, false)"),
	'standby received more WAL while replay stayed paused');

$primary->stop('fast');
ok(wait_until($standby, 'SELECT count(*) = 0 FROM pg_stat_wal_receiver'),
	'WAL receiver stopped before the local scan');
my $baseline = psql($standby, 'SELECT lwaldump()');
ok(lsn_ge($standby, $baseline, $receive_target),
	'unmodified WAL scans through the received transaction');

# Change only the system identifier in a valid segment's long page header.
# WAL records and their CRCs remain intact, while the scan starts later in
# the same segment. The reader must still check the segment identity.
my $sysid = psql($standby,
	'SELECT system_identifier::text FROM pg_control_system()');
my $encoded_sysid = pack('Q', $sysid);
my $path = $standby->data_dir . "/pg_wal/$replay_file";
open(my $wal, '+<', $path) or BAIL_OUT("could not open $path: $!");
binmode($wal);
read($wal, my $header, 64) == 64
  or BAIL_OUT("could not read the WAL long page header from $path: $!");
my $sysid_offset = index($header, $encoded_sysid);
ok($sysid_offset >= 0 && index($header, $encoded_sysid,
		$sysid_offset + 1) < 0,
	'found the cluster system identifier exactly once in the WAL header');
BAIL_OUT('could not locate WAL system identifier') if $sysid_offset < 0;
my $foreign_sysid = $encoded_sysid;
substr($foreign_sysid, 0, 1) = chr(ord(substr($foreign_sysid, 0, 1)) ^ 1);
seek($wal, $sysid_offset, 0) or BAIL_OUT("could not seek in $path: $!");
print {$wal} $foreign_sysid or BAIL_OUT("could not alter $path: $!");
close($wal) or BAIL_OUT("could not close $path: $!");

my ($stdout, $stderr);
my $exit = $standby->psql('postgres', 'SELECT lwaldump()',
	stdout => \$stdout, stderr => \$stderr);
ok($exit != 0, 'foreign system identifier is rejected');
like($stderr, qr/WAL file is from different database system/i,
	'failure identifies the foreign WAL segment');

# A page can have a valid header and record CRCs while claiming a timeline
# outside the server's recovery history.  Recovery itself rejects that page.
open($wal, '+<', $path) or BAIL_OUT("could not reopen $path: $!");
binmode($wal);
seek($wal, $sysid_offset, 0) or BAIL_OUT("could not seek in $path: $!");
print {$wal} $encoded_sysid or BAIL_OUT("could not restore $path: $!");
seek($wal, 4, 0) or BAIL_OUT("could not seek to page timeline in $path: $!");
print {$wal} pack('L', 2)
  or BAIL_OUT("could not change page timeline in $path: $!");
close($wal) or BAIL_OUT("could not close $path: $!");

($stdout, $stderr) = ('', '');
$exit = $standby->psql('postgres', 'SELECT lwaldump()',
	stdout => \$stdout, stderr => \$stderr);
ok($exit != 0, 'page from outside recovery timeline history is rejected');
like($stderr, qr/timeline 2 outside the recovery path/i,
	'failure identifies the foreign page timeline');

$standby->stop('immediate');
done_testing();
