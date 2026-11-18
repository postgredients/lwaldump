package LwaldumpTest;

use strict;
use warnings;

use Exporter 'import';
use Test::More ();
use Time::HiRes qw(sleep time);

our @EXPORT_OK = qw(new_node psql wait_until lsn_ge);
our $NODE_CLASS;

BEGIN
{
	# PostgreSQL 15 moved the TAP node helpers into the PostgreSQL::Test
	# namespace.  Load the helpers belonging to the selected PGXS installation.
	if (eval { require PostgreSQL::Test::Cluster; 1 })
	{
		$NODE_CLASS = 'PostgreSQL::Test::Cluster';
	}
	else
	{
		my $cluster_error = $@;
		eval { require PostgresNode; 1 }
		  or die "could not load PostgreSQL TAP node helpers: "
		  . "$cluster_error; $@";
		$NODE_CLASS = 'PostgresNode';
	}
}

sub new_node
{
	my ($name) = @_;
	die 'node name is required' unless defined $name && length $name;

	return $NODE_CLASS eq 'PostgresNode'
	  ? PostgresNode->get_new_node($name)
	  : PostgreSQL::Test::Cluster->new($name);
}

sub psql
{
	my ($node, $sql) = @_;
	return $node->safe_psql('postgres', $sql);
}

sub wait_until
{
	my ($node, $sql, $expected, $timeout) = @_;
	$expected = 't' unless defined $expected;
	$timeout = 60 unless defined $timeout;

	my $deadline = time + $timeout;
	my ($actual, $error);
	while (1)
	{
		my $ok = eval { $actual = psql($node, $sql); 1 };
		$error = $@ unless $ok;
		return 1 if $ok && defined $actual && $actual eq $expected;
		last if time >= $deadline;
		sleep(0.1);
	}

	Test::More::diag("timed out waiting for SQL result '$expected': $sql");
	Test::More::diag("last result: " . (defined $actual ? $actual : '(none)'));
	Test::More::diag("last error: $error") if defined $error && length $error;
	return 0;
}

sub lsn_ge
{
	my ($node, $left, $right) = @_;
	for my $lsn ($left, $right)
	{
		die 'invalid LSN' unless defined $lsn && $lsn =~ /\A[0-9A-F]+\/[0-9A-F]+\z/i;
	}

	return psql($node, "SELECT '$left'::pg_lsn >= '$right'::pg_lsn") eq 't';
}

1;
