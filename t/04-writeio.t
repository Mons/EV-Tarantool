#!/usr/bin/env perl

use 5.010;
use strict;
use Test::More;# skip_all => "TODO";
use FindBin;
use lib "t/lib","lib","$FindBin::Bin/../blib/lib","$FindBin::Bin/../blib/arch";
use EV;
use EV::Tarantool;
use Time::HiRes 'sleep','time';
use Data::Dumper;
use Errno;
use Scalar::Util 'weaken';
use TestTarantool;

$EV::DIED = sub {
	warn "@_";
	EV::unloop;
	exit;
};


my $tnt = tnt_run();

my $w;$w = EV::timer 15,0,sub { undef $w; fail "Timed out"; exit; };

my $cfs = 0;
my $c = EV::Tarantool->new({
	host => $tnt->{host},
	port => $tnt->{port},
	reconnect => 1,
	timeout => 10,
	connected => sub {
		my $c = shift;
		my %start;

		$start{1} = 1;
		$c->lua('dummy',['x'x(2**20)], sub {
			if ($_[0]) {
				delete $start{ $_[0]{id} };
				pass "First big";
			} else {
				fail "First big $_[1]";
			}
		});
		for (1..1200) {
			$start{$_+1} = ();
			$c->lua('dummy',['x', $_], sub {
				if ($_[0]) {
					if (exists $start{ $_[0]{id} }) {
						delete $start{ $_[0]{id} };
						if (not %start) {
							pass "All done";
							$c->disconnect;
						}
					} else {
						fail "Duplicate response for $_[0]{id}";
						EV::unloop;
					}
				} else {
					shift;
					fail "Request failed: @_";
					EV::unloop;
				}
			});
		}
	},
	connfail => sub {
		my $err = 0+$!;
		is $err, Errno::ECONNREFUSED, 'connfail - refused' or diag "$!, $_[1]";
		$cfs++
		and 
			EV::unloop;
	},
	disconnected => sub {
		my $c = shift;
		warn "PL: Disconnected: @_";
		pass "Disconnected";
		EV::unloop;
	},
});

$c->connect;

EV::loop;
done_testing();
