package EV::Tarantool::Multi;

use 5.010;
use strict;
use warnings;
no warnings 'uninitialized';
use Scalar::Util qw(weaken);
use EV::Tarantool;
use Carp;
sub U(@) { $_[0] }

sub log_err {}
sub log_warn {
	shift;
	warn "@_\n"
}

sub new {
	my $pkg = shift;
	my $self = bless {
		timeout => 1,
		recovery_lag  => 1,
		reconnect => 1/3,
		connected_mode => 'any', # rw|ro|any - when to call 'connected'
		cnntrace => 1,
		wbuf_limit => 16000,
		@_,
		stores => [],
		rwstores => [],
		rostores => [],
	},$pkg;
	
	my $servers = delete $self->{servers};
	my $spaces = delete $self->{spaces};
	$self->{servers} = [];
	
	my $i = 0;
	my $rws = 0;
	my $ros = 0;
	for (@$servers) {
		my $srv;
		my $id = $i++;
		if (ref) {
			$srv = { %$_, id => $id };
		}
		else {
			m{^(?:(rw|ro):|)([^:]+)(?::(\d+)|)};
			$srv = {
				rw   => $1 eq 'rw' ? 1 : defined $1 ? 0 : 1,
				host => $2,
				port => $3 // 33013,
				id   => $id,
				gen  => 1,
			};
		}
		$srv->{node} = ($srv->{rw} ? 'rw' : 'ro' ) . ':' . $srv->{host} . ':' . $srv->{port};
		if ($srv->{rw}) { $rws++ } else { $ros++; }
		push @{$self->{servers}}, $srv;
		my $warned;
		$srv->{c} = EV::Tarantool->new({
			host => $srv->{host},
			port => $srv->{port},
			timeout => $self->{timeout},
			reconnect => $self->{reconnect},
			spaces => $spaces,
			read_buffer => 2*1024*1024,
			cnntrace => $self->{cnntrace},
			wbuf_limit => $self->{wbuf_limit},
			connected => sub {
				my $c = shift;
				@{ $srv->{peer} = {} }{qw(host port)} = @_;
				$c->lua('box.dostring',['return box.info.status'],sub {
					$warned = 0;
					if (my $res = shift) {
						my ($status) = @{ $res->{tuples}[0] };
						warn "Connected with status $status\n";
						my $gen = ++$srv->{gen};
						if ($status eq 'primary') {
							$srv->{rw} = 1;
						}
						elsif ($status =~ m{^replica/}) {
							$srv->{rw} = 0;
						}
						else {
							$srv->{rw} = 0;
							warn "strange status received: $status";
						}
						$self->_db_online( $srv );
						my $check;$check = sub { my $check = $check;
							$gen == $srv->{gen} or return;
							$c->lua('box.dostring',['return box.info.status'],sub {
								if (my $res = shift) {
									my ($newstatus) = @{ $res->{tuples}[0] };
									# warn $newstatus;
									my $rw = $newstatus eq 'primary' ? 1 : 0;
									if ($rw != $srv->{rw}) {
										$self->_db_offline( $srv, "Status change $srv->{host}:$srv->{port}: $status to $newstatus");
										$srv->{rw} = $rw;
										$self->_db_online( $srv );
									}
									$status = $newstatus;
								} else {
									warn "Status request failed on host $srv->{host}:$srv->{port}: @_";
								}
								my $w;$w = EV::timer 0.1,0,sub {
									undef $w;
									$check->();
								};
							});
						};$check->();weaken($check);
					} else {
						warn "Initial request failed on $srv->{host}:$srv->{port}: @_";
						$c->reconnect;
						return;
					}
				});
				
				### This will wait for good tarantool with async protocol
				# $c->lua('box.dostring',['
				# 	if box.status_change_wait and box.status_change_wait_version == 4 then else
				# 		box.status_waiters = setmetatable({},{ __mode = "kv" })
				# 		function box.status_change_wait(status,timeout)
				# 			timeout = tonumber(timeout)
				# 			local ch = box.ipc.channel(1)
				# 			box.status_waiters[ch] = ch
				# 			local start = box.time()
				# 			while true do
				# 				local delta = box.time() - start
				# 				if status ~= box.info.status or delta >= timeout then
				# 					box.status_waiters[ch] = nil
				# 					ch = nil
				# 					return box.tuple.new({ box.info.status, tostring(box.status_change_wait_version) })
				# 				end
				# 				local z = ch:get(timeout - delta)
				# 			end
				# 		end
				# 		if box.status_change_wait then else
				# 			local prev = box.on_reload_configuration
				# 			function box.on_reload_configuration()
				# 				collectgarbage("collect")
				# 				for ch in pairs(box.status_waiters) do
				# 					ch:put(true)
				# 				end
				# 				prev()
				# 			end
				# 		end
				# 		box.status_change_wait_version = 4
				# 	end
				# 	return box.tuple.new({ box.info.status, tostring(box.status_change_wait_version) })
				# '],sub {
				# 	if (my $res = shift) {
				# 		my ($status,$ver) = @{ $res->{tuples}[0] };
				# 		warn "Connected with status $status, watcher v$ver\n";
				# 		my $gen = ++$srv->{gen};
				# 		my $wait_timeout = 300;
				# 		my $statuswait;$statuswait = sub { my $statuswait = $statuswait;
				# 			$gen == $srv->{gen} or return;
				# 			$c->lua('box.status_change_wait',[$status,$wait_timeout], { timeout => $wait_timeout+10 },sub {
				# 				if (my $res = shift) {
				# 					my ($newstatus,$version) = @{ $res->{tuples}[0] };
				# 					my $rw = $newstatus eq 'primary' ? 1 : 0;
				# 					if ($rw != $srv->{rw}) {
				# 						$self->_db_offline( $srv, "Status change $srv->{host}:$srv->{port}: $status to $newstatus");
				# 						$srv->{rw} = $rw;
				# 						$self->_db_online( $srv );
				# 					}
				# 					$status = $newstatus;
				# 				} else {
				# 					warn "status request failed on host $srv->{host}:$srv->{port}: @_";
				# 				}
				# 				$statuswait->();
				# 			});
				# 		};$statuswait->();weaken($statuswait);
				# 		if ($status eq 'primary') {
				# 			$srv->{rw} = 1;
				# 		}
				# 		elsif ($status =~ m{^replica/}) {
				# 			$srv->{rw} = 0;
				# 		}
				# 		else {
				# 			$srv->{rw} = 0;
				# 			warn "strange status received: $status";
				# 		}
				# 		$warned = 0;
				# 		$self->_db_online( $srv );
				# 	} else {
				# 		warn "Initial request failed on $srv->{host}:$srv->{port}: @_";
				# 		$c->reconnect;
				# 		return;
				# 	}
				# });
			},
			connfail => sub {
				my ($c,$fail) = @_;
				$self->{connfail} ? $self->{connfail}( U($self,$c),$fail ) :
				!$warned++ && $self->log_warn( "Connection to $srv->{node} failed: $fail" );
			},
			disconnected => sub {
				my $c = shift;
				$srv->{gen}++;
				@_ and $srv->{peer} and $self->log_warn( "Connection to $srv->{node}/$srv->{peer}{host}:$srv->{peer}{port} closed: @_" );
				$self->_db_offline( $srv, @_ );
				
			},
		});
		#$srv->{c}->connect;
	}
	#if ($self->{connected_mode} eq 'rw' and not $rws ) {
	#	die "Cluster could not ever be 'connected' since waiting for at least one 'rw' node, and have none of them (@{$servers})\n";
	#}
	#if ($self->{connected_mode} eq 'ro' and not $ros ) {
	#	die "Cluster could not ever be 'connected' since waiting for at least one 'ro' node, and have none of them (@{$servers})\n";
	#}
	if (not $ros+$rws ) {
		die "Cluster could not ever be 'connected' since have no servers (@{$servers})\n";
	}
	
	return $self;
}

sub connect : method {
	my $self = shift;
	for my $srv (@{ $self->{servers} }) {
		$srv->{c}->connect;
	}
}

sub disconnect : method {
	my $self = shift;
	for my $srv (@{ $self->{servers} }) {
		$srv->{c}->disconnect;
	}
}

sub ok {
	my $self = shift;
	if (@_ and $_[0] ne 'any') {
		return @{ $self->{$_[0].'stores'} } > 0 ? 1 : 0;
	} else {
		return @{ $self->{stores} } > 0 ? 1 : 0;
	}
}

sub _db_online {
	my $self = shift;
	my $srv  = shift;
	
	my $first = (
		$self->{connected_mode} eq 'rw' ? ( $srv->{rw} && (@{ $self->{rwstores} } == 0) ) :
		$self->{connected_mode} eq 'ro' ? ( !$srv->{rw} && (@{ $self->{rostores} } == 0) ) :
		( @{ $self->{stores} } == 0 )
	) || 0;
	
	#warn "online $srv->{node} for $self->{connected_mode}; first = $first";
	
	push @{ $self->{stores} }, $srv;
	push @{ $self->{rwstores} }, $srv if $srv->{rw};
	push @{ $self->{rostores} }, $srv if !$srv->{rw};
	
	my $key = $srv->{rw} ? 'rw' : 'ro';
	my $event = "${key}_connected";
	my @args = ( U($self,$srv->{c}), @{ $srv->{peer} }{qw(host port)} );
	
	$self->{change} and $self->{change}->($self,"connected",$srv->{rw} ? 'rw' : 'ro',@{ $srv->{peer} }{qw(host port)});
	
	$self->{$event} && $self->{$event}( @args );
	$first and $self->{connected} and $self->{connected}( @args );
	
	if ( $self->{all_connected} and @{ $self->{servers} } == @{ $self->{stores} } ) {
		$self->{all_connected}( $self, $self->{stores} );
	}
}

sub _db_offline {
	my $self = shift;
	my $srv  = shift;
	my $c = $srv->{c};
	
	$self->{stores}   = [ grep $_ != $srv, @{ $self->{stores} } ];
	$self->{rwstores} = [ grep $_ != $srv, @{ $self->{rwstores} } ] if $srv->{rw};
	$self->{rostores} = [ grep $_ != $srv, @{ $self->{rostores} } ] if !$srv->{rw};
	
	#my $last = ( $self->{connected_mode} eq 'rw' ? ( $srv->{rw} && (@{ $self->{rwstores} } == 0) ) : ( @{ $self->{stores} } == 0 ) ) || 0;
	my $last = (
		$self->{connected_mode} eq 'rw' ? ( $srv->{rw} && (@{ $self->{rwstores} } == 0) ) :
		$self->{connected_mode} eq 'ro' ? ( !$srv->{rw} && (@{ $self->{rostores} } == 0) ) :
		( @{ $self->{stores} } == 0 )
	) || 0;
	
	my $key = $srv->{rw} ? 'rw' : 'ro';
	my $event = "${key}_disconnected";
	
	$self->{change} and $self->{change}->($self,"disconnected",$srv->{rw} ? 'rw' : 'ro',@{ $srv->{peer} }{qw(host port)}, @_);
	my @args = ( U($self,$srv->{c}), @_ );
	$self->{$event} && $self->{$event}( @args );
	
	$last and $self->{disconnected} and $self->{disconnected}( @args );
	
	if( @{ $self->{stores} } == 0 and $self->{all_disconnected} ) {
		$self->{all_disconnected}( $self );
	}
}

=for rem
	RW     - send request only to RW node
	RO     - send request only to RO node
	ANY    - send request to any node
	ARO    - send request to any node, but prefer ro
	ARW    - send request to any node, but prefer rw
=cut

sub _srv_by_mode {
	my $self = shift;
	my $mode;
	#warn "@_";
	if ( @_ > 1 and !ref $_[-2] and $_[-2] =~ /^(?:r[ow]|any|a(?:ny|)r[ow])$/i  ) {
		$mode = splice @_, -2,1;
	} else {
		$mode = $self->{connected_mode};
	}
	my $srv;
	if ($mode eq 'rw') {
		@{ $self->{rwstores} } or do { $_[-1]( undef, "Have no connected nodes for mode $mode" ), return };
		$srv = $self->{rwstores}[ rand @{ $self->{rwstores} } ];
	}
	elsif ($mode eq 'ro' ) { # fb to any
		@{ $self->{rostores} } or do { $_[-1]( undef, "Have no connected nodes for mode $mode" ), return };
		$srv = $self->{rostores}[ rand @{ $self->{rostores} } ];
	}
	elsif ($mode eq 'arw') {
		@{ $self->{stores} } or do { $_[-1]( undef, "Have no connected nodes for mode $mode" ), return };
		$srv = @{ $self->{rwstores} } ? $self->{rwstores}[ rand @{ $self->{rwstores} } ] : $self->{rostores}[ rand @{ $self->{rostores} } ];
	}
	elsif ($mode eq 'aro') {
		@{ $self->{stores} } or do { $_[-1]( undef, "Have no connected nodes for mode $mode" ), return };
		$srv = @{ $self->{rostores} } ? $self->{rostores}[ rand @{ $self->{rostores} } ] : $self->{rwstores}[ rand @{ $self->{rwstores} } ];
	}
	else {
		@{ $self->{stores} } or do { $_[-1]( undef, "Have no connected nodes for mode $mode" ), return };
		$srv = $self->{stores}[ rand @{ $self->{stores} } ];
		
	}
	my $cb = pop;
	return $srv->{c}, sub {
		if ($_[0]) {
			$_[0]{mode} = $srv->{rw} ? 'rw' : 'ro';
		}
		elsif ($_[2]) {
			$_[2]{mode} = $srv->{rw} ? 'rw' : 'ro';
		}
		goto &$cb;
	};
}

sub _srv_rw {
	my $self = shift;
	my $mode;
	#warn "@_";
	if ( @_ > 1 and !ref $_[-2] and $_[-2] =~ /^(?:r[ow]|any|a(?:ny|)r[ow])$/i  ) {
		$mode = splice @_, -2,1;
		if ($mode ne 'rw') {
			carp "Can't use mode '$mode' for modification query";
		}
	}
	@{ $self->{rwstores} } or do { $_[-1]( undef, "Have no connected nodes for mode rw" ), return };
	my $srv = $self->{rwstores}[ rand @{ $self->{rwstores} } ];

	my $cb = pop;
	return $srv->{c}, sub {
		if ($_[0]) {
			$_[0]{mode} = $srv->{rw} ? 'rw' : 'ro';
		}
		elsif ($_[2]) {
			$_[2]{mode} = $srv->{rw} ? 'rw' : 'ro';
		}
		goto &$cb;
	};
}

sub ping : method {
	my ($srv,$cb)  = &_srv_by_mode or return;
	$srv->ping(@_,$cb);
}

sub lua : method {
	my ($srv,$cb)  = &_srv_by_mode or return;
	$srv->lua(@_,$cb);
}

sub select : method {
	my ($srv,$cb)  = &_srv_by_mode or return;
	$srv->select(@_,$cb);
}

sub insert : method {
	my ($srv,$cb)  = &_srv_rw or return;
	$srv->insert(@_,$cb);
}

sub delete : method {
	my ($srv,$cb)  = &_srv_rw or return;
	$srv->delete(@_,$cb);
}

sub update : method {
	my ($srv,$cb)  = &_srv_rw or return;
	$srv->update(@_,$cb);
}

sub each : method {
	my $self = shift;
	my $cb = pop;
	my $flags = shift;
	for my $s (@{ $self->{stores} }) {
		$cb->($s);
	}
}



1;
