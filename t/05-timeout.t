#!/usr/bin/env perl
use strict;
use warnings;

use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";
use Test::More;
use Data::Dumper;
use Test::Tarantool;
use EV::Tarantool;
use EV::Tarantool::Multi;
use AnyEvent;

my $short_timeout = 0.01;

sub AE::cvt(;$){
	my $after = shift // 1;
	EV::now_update;
	my $cv; my %s;
	$s{timer} = AE::timer $after,0, sub { $cv->croak('AE::cvt timed out') if %s; };
	$cv = AE::cv sub { %s=(); };
	return $cv;
}

# Prepare to test (setup section)
my $spaces=<<_EOF;
space[0] = {
	enabled = 1,
	index = [{
		type = TREE,
		unique = 1,
		key_field = [{
			fieldno = 0,
			type = NUM
		}]
	}]
}
_EOF

my $evtnt_spaces = { 0 =>
	{	name => 'main',
		fields => ['id'],
		types => ['INT'],
		indexes => {
			0 => { name => 'pk', fields => ['id'] }
		},
	}
};

my $tarantool= Test::Tarantool->new(
	cleanup  => !$ENV{NO_CLEANUP},
	title    => 'evt',
	arena    => 0.01,
	logger   => sub { diag ( 'evt ', @_ ) if $ENV{TEST_VERBOSE}},
	timeout  => 0.1,
	#initlua => $self->init_lua,
	spaces   => $spaces,
	on_die   => sub { fail "Test::Tarantool evt is dead!!!!!!!! $!"; },
	wal_mode => 'none',
);
$tarantool->start(my $started= AE::cvt 10);
my ($res, $reason) = eval{ $started->recv };
BAIL_OUT("Tarantool not started in 10 seconds: $@") if $@;
ok($res, "Test::Tarantool returned true value after start");

{
	my $client = EV::Tarantool->new({
		host      => $tarantool->{host},
		port      => $tarantool->{port},
		spaces    => $evtnt_spaces,
		timeout   => $short_timeout,
		cnntrace  => $ENV{TEST_VERBOSE} ? 1 : 0,
		reconnect => 0,
		connected => my $connected = AE::cvt,
		connfail  => sub {
			fail "No call";
		},
	});
	$client->connect;
	my ($obj, $host, $port) = eval {$connected->recv};
	BAIL_OUT("EV::Tarantool not connected: $@") if $@;
	is( $obj, $client, 'connect passes client object as 1st arg to callback');
	is( $host, $tarantool->{host}, 'connect passes server host as 2nd arg to callback');
	is( $port, $tarantool->{port}, 'connect passes server port as 3rd arg to callback');

	$client->lua('box.dostring', ['box.space[0]:len()'], my $lua_success = AE::cvt);
	my ($luares, $maybe_reason, $maybe_error) = eval{$lua_success->recv};
	is($luares->{status}, 'ok', 'Lua call return status=ok when success');

	# Tests for timeout start here
	$tarantool->pause;
	$client->lua('box.dostring', ['box.space[0]:len()'], my $lua_timeout = AE::cvt);
	($luares, $maybe_reason, $maybe_error) = eval{$lua_timeout->recv};
	if ($@) {
		fail "EV::Tarantool: lua timeout not working: $@";
	} else {
		is($luares, undef, '1st arg to callback not defined on error');
		like($maybe_reason, qr/timed?\s*out/i, '2st arg describes error on timeout');
	}

	$client->select('main', [[0]], my $cvt = AE::cvt($short_timeout+1));
	($luares, $maybe_reason, $maybe_error) = eval { $cvt->recv };
	if ($@) {
		fail "EV::Tarantool: select timeout not working: $@";
	} else {
		is($luares, undef, '1st arg to callback not defined on error');
		like($maybe_reason, qr/timed?\s*out/i, '2st arg describes error on timeout');
	}
	$client->insert('main', [0], $cvt = AE::cvt($short_timeout+1));
	($luares, $maybe_reason, $maybe_error) = eval { $cvt->recv };
	if ($@) {
		fail "EV::Tarantool: insert timeout not working: $@";
	} else {
		is($luares, undef, 'insert: 1st arg to callback not defined on error');
		like($maybe_reason, qr/timed?\s*out/i, 'insert: 2st arg describes error on timeout');
	}
	$client->update('main', [0], [[id=> '=', 1]], $cvt = AE::cvt($short_timeout+1));
	($luares, $maybe_reason, $maybe_error) = eval { $cvt->recv };
	if ($@) {
		fail "EV::Tarantool: update timeout not working: $@";
	} else {
		is($luares, undef, 'update 1st arg to callback not defined on error');
		like($maybe_reason, qr/timed?\s*out/i, 'update 2st arg describes error on timeout');
	}
	$client->delete('main', [1], $cvt = AE::cvt($short_timeout+1));
	($luares, $maybe_reason, $maybe_error) = eval { $cvt->recv };
	if ($@) {
		fail "EV::Tarantool: delete timeout not working: $@";
	} else {
		is($luares, undef, 'delete 1st arg to callback not defined on error');
		like($maybe_reason, qr/timed?\s*out/i, 'delete 2st arg describes error on timeout');
	}
	# Done testing for timeout in EV::Tarantool
	$tarantool->resume;
	undef $client;
}

{
	my $client = EV::Tarantool->new({
		host      => $tarantool->{host},
		port      => $tarantool->{port},
		spaces    => $evtnt_spaces,
		timeout   => $short_timeout+2, # We gonna pass real timeout in opts
		cnntrace  => $ENV{TEST_VERBOSE} ? 1 : 0,
		reconnect => 0,
		connected => my $connected = AE::cvt,
		connfail  => sub {
			fail "No call";
		},
	});
	$client->connect;
	my ($obj, $host, $port) = eval {$connected->recv};
	BAIL_OUT("EV::Tarantool not connected: $@") if $@;
	is( $obj, $client, 'connect passes client object as 1st arg to callback');
	is( $host, $tarantool->{host}, 'connect passes server host as 2nd arg to callback');
	is( $port, $tarantool->{port}, 'connect passes server port as 3rd arg to callback');

	$client->lua('box.dostring', ['box.space[0]:len()'], my $lua_success = AE::cvt);
	my ($luares, $maybe_reason, $maybe_error) = eval{$lua_success->recv};
	is($luares->{status}, 'ok', 'Lua call return status=ok when success');

	# Tests for timeout start here
	my $opts = {timeout => $short_timeout};
	$tarantool->pause;
	$client->lua('box.dostring', ['box.space[0]:len()'], $opts, my $lua_timeout = AE::cvt);
	($luares, $maybe_reason, $maybe_error) = eval{$lua_timeout->recv};
	if ($@) {
		fail "EV::Tarantool: lua timeout not working: $@";
	} else {
		is($luares, undef, '1st arg to callback not defined on error');
		like($maybe_reason, qr/timed?\s*out/i, '2st arg describes error on timeout');
	}

	$client->select('main', [[0]], $opts, my $cvt = AE::cvt($short_timeout+1));
	($luares, $maybe_reason, $maybe_error) = eval { $cvt->recv };
	if ($@) {
		fail "EV::Tarantool: select direct timeout not working: $@";
	} else {
		is($luares, undef, '1st arg to callback not defined on error');
		like($maybe_reason, qr/timed?\s*out/i, '2st arg describes error on timeout');
	}
	$client->insert('main', [0], $opts, $cvt = AE::cvt($short_timeout+1));
	($luares, $maybe_reason, $maybe_error) = eval { $cvt->recv };
	if ($@) {
		fail "EV::Tarantool: insert direct timeout not working: $@ " . Dumper($luares,$maybe_reason);
	} else {
		is($luares, undef, 'insert: 1st arg to callback not defined on error');
		like($maybe_reason, qr/timed?\s*out/i, 'insert: 2st arg describes error on timeout');
	}
	$client->update('main', [0], [[id=> '=', 1]], $opts, $cvt = AE::cvt($short_timeout+1));
	($luares, $maybe_reason, $maybe_error) = eval { $cvt->recv };
	if ($@) {
		fail "EV::Tarantool: update direct timeout not working: $@";
	} else {
		is($luares, undef, 'update 1st arg to callback not defined on error');
		like($maybe_reason, qr/timed?\s*out/i, 'update 2st arg describes error on timeout');
	}
	$client->delete('main', [1], $opts, $cvt = AE::cvt($short_timeout+1));
	($luares, $maybe_reason, $maybe_error) = eval { $cvt->recv };
	if ($@) {
		fail "EV::Tarantool: delete direct timeout not working: $@";
	} else {
		is($luares, undef, 'delete 1st arg to callback not defined on error');
		like($maybe_reason, qr/timed?\s*out/i, 'delete 2st arg describes error on timeout');
	}
	# Done testing for timeout in EV::Tarantool
	$tarantool->resume;
	undef $client;
}
#is($luares->{status}, 'ok', 'Lua call return status=ok when success');

#$client = EV::Tarantool::Multi->new(
#	servers => [ sprintf("rw:%s:%s", $tarantool->{host}, $tarantool->{port}) ],
#	spaces  => $evtnt_spaces,
#	timeout => 0.1,
#	reconnect => 0,
#	connected => $connected = AE::cv,
#	connfail => sub {
#		fail "No call";
#	},
#);
#$client->connect;
#my ($obj, $host, $port) = $connected->recv;
#is( $obj, $client, 'Multi connect passes client object as 1st arg to callback');
#is( $host, $tarantool->{host}, 'Multi connect passes server host as 2nd arg to callback');
#is( $port, $tarantool->{port}, 'Multi connect passes server port as 3rd arg to callback');
#
#$client->lua('box.dostring', ['box.space[0]:len()'], $lua_success = AE::cv);
#my ($luares, $maybe_reason, $maybe_error) = $lua_success->recv;
#is($luares->{status}, 'ok', 'Multi Lua call return status=ok when success');
#$tarantool->pause;
##$client->select('main', [[0]], my $cv = AE::cv);
##($luares, $maybe_reason, $maybe_error) = $cv->recv;
##is($luares, undef, 'Multi 1st arg to callback not defined on error');
##like($maybe_reason, qr/timed?\s*out/i, 'Multi 2st arg describes error on timeout');
#$tarantool->resume;
#
#$tarantool->stop(my $stopped= AE::cv);
#my @stop_res = $stopped->recv;

done_testing;

1;


