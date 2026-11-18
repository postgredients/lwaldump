use strict;
use warnings;

use FindBin;
use lib $FindBin::Bin;
use LwaldumpTest qw(new_node psql);
use Test::More;

my $node = new_node('upgrade');
$node->init;
$node->start;

psql($node, "CREATE EXTENSION lwaldump VERSION '1.0'");
psql($node, 'ALTER EXTENSION lwaldump DROP FUNCTION lwaldump_with_timeline()');
psql($node, 'DROP FUNCTION lwaldump_with_timeline()');
is(psql($node, "SELECT extversion FROM pg_extension WHERE extname = 'lwaldump'"),
	'1.0', 'a historical installation can remain at version 1.0');
is(psql($node, "SELECT to_regprocedure('lwaldump_with_timeline()') IS NULL"),
	't', 'version 1.0 exposes only the original function');

psql($node, 'ALTER EXTENSION lwaldump UPDATE');
is(psql($node, "SELECT extversion FROM pg_extension WHERE extname = 'lwaldump'"),
	'1.1', 'ALTER EXTENSION upgrades to version 1.1');
is(psql($node, "SELECT to_regprocedure('lwaldump_with_timeline()') IS NOT NULL"),
	't', 'the upgrade adds the timeline function');

psql($node, 'DROP EXTENSION lwaldump');
psql($node, "CREATE EXTENSION lwaldump VERSION '1.0'");
psql($node, 'ALTER EXTENSION lwaldump UPDATE');
is(psql($node, "SELECT extversion FROM pg_extension WHERE extname = 'lwaldump'"),
	'1.1', 'upgrade accepts version 1.0 with a preexisting timeline function');
is(psql($node,
	"SELECT count(*) = 1 FROM pg_depend d JOIN pg_extension e ON d.refobjid = e.oid "
	. "WHERE e.extname = 'lwaldump' AND d.objid = 'lwaldump_with_timeline()'::regprocedure"),
	't', 'preexisting timeline function stays in the extension');

psql($node, 'DROP EXTENSION lwaldump');
psql($node, 'CREATE EXTENSION lwaldump');
is(psql($node, "SELECT extversion FROM pg_extension WHERE extname = 'lwaldump'"),
	'1.1', 'fresh installation uses version 1.1');

$node->stop;
done_testing();
