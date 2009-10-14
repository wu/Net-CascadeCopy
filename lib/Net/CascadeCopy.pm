package Net::CascadeCopy;
use Mouse;

use Benchmark;
use Log::Log4perl qw(:easy);
use POSIX ":sys_wait_h"; # imports WNOHANG
use Proc::Queue size => 32, debug => 0, trace => 0, delay => 1;
use version; our $VERSION = qv('0.2.0');

my $logger = get_logger( 'default' );

# inside-out Perl class
use Class::Std::Utils;
{
    # available/remaining/completed/failed servers and running processes
    my %data_of;

    # keep track of total transfer time for each job to calculate savings
    my %total_time_of;

    # ssh command used to log in to remote server in order to run command
    my %ssh_of;
    my %ssh_args_of;

    # copy command
    my %command_of;
    my %command_args_of;

    # path to be copied
    my %source_path_of;
    my %target_path_of;

    # options for output
    my %output_of;

    # maximum number of failures per server
    my %max_failures_of;

    # maximum processes per remote server
    my %max_forks_of;

    # keep track of child processes
    my %children_of;

    # Constructor takes path of file system root directory...
    sub new {
        my ($class, $arg_ref) = @_;

        # Bless a scalar to instantiate the new object...
        my $new_object = bless \do{my $anon_scalar}, $class;

        # Initialize the object's attributes...
        $ssh_of{ident $new_object}          = $arg_ref->{ssh}          || "ssh";
        $ssh_args_of{ident $new_object}     = $arg_ref->{ssh_args}     || "-x -A";
        $max_failures_of{ident $new_object} = $arg_ref->{max_failures} || 3,
        $max_forks_of{ident $new_object}    = $arg_ref->{max_forks}    || 2;
        $output_of{ident $new_object}       = $arg_ref->{output}       || "";

        return $new_object;
    }

    sub _get_data {
        my ($self) = @_;
        return $data_of{ident $self};
    }

    sub set_command {
        my ( $self, $command, $args ) = @_;

        $command_of{ident $self} = $command;
        $command_args_of{ident $self} = $args || "";

        return 1;
    }

    sub set_source_path {
        my ( $self, $path ) = @_;
        $source_path_of{ident $self} = $path;
    }

    sub set_target_path {
        my ( $self, $path ) = @_;
        $target_path_of{ident $self} = $path;
    }

    sub add_group {
        my ( $self, $group, $servers_a ) = @_;

        $logger->info( "Adding group: $group: ",
                       join( ", ", @$servers_a ),
                   );

        # initialize data structures
        for my $server ( @{ $servers_a } ) {
            $data_of{ident $self}->{remaining}->{ $group }->{$server} = 1;
        }

        # first server to transfer from is the current server
        $data_of{ident $self}->{available}->{ $group }->{localhost} = 1;

        # initialize data structures
        $data_of{ident $self}->{completed}->{ $group } = [];

    }

    sub transfer {
        my ( $self ) = @_;

        unless ( $command_of{ident $self} ) {
            die "ERROR: no transfer command has been set, try set_command()";
        }

        unless ( $source_path_of{ident $self} ) {
            die "ERROR: no source path has been set!";
        }

        unless ( $target_path_of{ident $self} ) {
            die "ERROR: no target path has been set!";
        }

        my $transfer_start = new Benchmark;

      LOOP:
        while ( 1 ) {
            last LOOP unless $self->_transfer_loop( $transfer_start );
            sleep 1;
        }
    }

    sub _transfer_loop {
        my ( $self, $transfer_start ) = @_;

        $self->_check_for_completed_processes();

        # keep track if there are any remaining servers in any groups
        my ( $remaining_flag, $available_flag );

        # iterate through groups with reamining servers
        for my $group ( $self->_get_remaining_groups() ) {

            # there are still available servers to sync
            if ( $self->_get_available_servers( $group )  ) {
                my $source = $self->_reserve_available_server( $group );

                my $busy;
                for my $fork ( 1 .. $max_forks_of{ident $self} ) {
                    next if $source eq "localhost" && $fork > 1;
                    if ( $self->_get_remaining_servers( $group ) ) {

                        my $target = $self->_reserve_remaining_server( $group );
                        $self->_start_process( $group, $source, $target );
                        $busy++;
                    }
                }

                unless ( $busy ) {
                    $logger->debug( "No remaining servers for available server $source" );
                }
            }
        }

        if ( ! scalar keys %{ $data_of{ident $self}->{remaining} } && ! $data_of{ident $self}->{running} ) {
            my $transfer_end = new Benchmark;
            my $transfer_diff = timediff( $transfer_end, $transfer_start );
            my $transfer_time = $self->_human_friendly_time( $transfer_diff->[0] );
            $logger->warn( "Job completed in $transfer_time" );

            my $total_time = $self->_human_friendly_time( $total_time_of{ident $self} );
            $logger->info ( "Cumulative tansfer time of all jobs: $total_time" );

            my $savings = $total_time_of{ident $self} - $transfer_diff->[0];
            if ( $savings ) {
                $savings = $self->_human_friendly_time( $savings );
                $logger->info( "Approximate Time Saved: $savings" );
            }
            $logger->warn( "Completed successfully" );
            return;
        }

        return 1;
    }

    sub _human_friendly_time {
        my ( $self, $seconds ) = @_;

        return "0 secs" unless $seconds;

        my @time_string;

        if ( $seconds > 3600 ) {
            my $hours = int( $seconds / 3600 );
            $seconds = $seconds % 3600;
            push @time_string, "$hours hrs";
        }
        if ( $seconds > 60 ) {
            my $minutes = int( $seconds / 60 );
            $seconds = $seconds % 60;
            push @time_string, "$minutes mins";
        }
        if ( $seconds ) {
            push @time_string, "$seconds secs";
        }

        return join " ", @time_string;
    }

    sub _print_status {
        my ( $self, $group ) = @_;

        # completed procs
        my $completed = 0;
        if ( $data_of{ident $self}->{completed}->{ $group } ) {
            $completed = scalar @{ $data_of{ident $self}->{completed}->{ $group } };
        }

        # running procs
        my $running = 0;
        if ( $data_of{ident $self}->{running} ) {
            for my $pid ( keys %{ $data_of{ident $self}->{running} } ) {
                if ( $data_of{ident $self}->{running}->{ $pid }->{group} eq $group ) {
                    $running++;
                }
            }
        }

        # unstarted
        my $unstarted = 0;
        if ( $data_of{ident $self}->{remaining}->{ $group } ) {
            $unstarted = scalar keys %{ $data_of{ident $self}->{remaining}->{ $group } };
        }

        # failed
        my $errors = 0;
        my $failures = 0;
        if ( $data_of{ident $self}->{failed} && $data_of{ident $self}->{failed}->{ $group } ) {
            for my $server ( keys %{ $data_of{ident $self}->{failed}->{ $group }} ) {
                $errors += $data_of{ident $self}->{failed}->{ $group }->{ $server };
                if ( $data_of{ident $self}->{failed}->{ $group }->{ $server } >= $max_failures_of{ident $self} ) {
                    $failures++;
                }
            }
        }

        $logger->info( "\U$group: ",
                       "completed:$completed ",
                       "running:$running ",
                       "left:$unstarted ",
                       "errors:$errors ",
                       "failures:$failures ",
                   );
    }

    sub _start_process {
        my ( $self, $group, $source, $target ) = @_;

        my $f=fork;
        if (defined ($f) and $f==0) {

            my $command;
            #my $command = "scp /tmp/foo $target:/tmp/foo";
            if ( $source eq "localhost" ) {
                $command = join " ", $command_of{ident $self},
                                     $command_args_of{ident $self},
                                     $source_path_of{ident $self},
                                     "$target:$target_path_of{ident $self}";
            }
            else {
                $command = join " ", $ssh_of{ident $self},
                                     $ssh_args_of{ident $self},
                                     $source,
                                     $command_of{ident $self},
                                     $command_args_of{ident $self},
                                     $target_path_of{ident $self},
                                     "$target:$target_path_of{ident $self}";
            }

            print "COMMAND: $command\n";

            my $output = $output_of{ident $self} || "";
            if ( $output eq "stdout" ) {
                # don't modify command
            } elsif ( $output eq "log" ) {
                # redirect all child output to log
                $command = "$command >> ccp.$source.$target.log 2>&1"
            } else {
                # default is to redirectout stdout to /dev/null
                $command = "$command >/dev/null"
            }

            $logger->info( "Starting: ($group) $source => $target" );
            $logger->debug( "Starting new child: $command" );

            system( $command );

            if ($? == -1) {
                $logger->logconfess( "failed to execute: $!" );
            } elsif ($? & 127) {
                $logger->logconfess( sprintf "child died with signal %d, %s coredump",
                                     ($? & 127),  ($? & 128) ? 'with' : 'without'
                                 );
            } else {
                my $exit_status = $? >> 8;
                if ( $exit_status ) {
                    $logger->error( "child exit status: $exit_status" );
                }
                exit $exit_status;
            }
        } else {
            my $start = new Benchmark;
            $data_of{ident $self}->{running}->{ $f } = { group  => $group,
                                                         source => $source,
                                                         target => $target,
                                                         start  => $start,
                                                     };
        }
    }

    sub _check_for_completed_processes {
        my ( $self ) = @_;

        return unless $data_of{ident $self}->{running};

        # find any processes that ended and reschedule the source and
        # target servers in the available pool
        for my $pid ( keys %{ $data_of{ident $self}->{running} } ) {
            if ( waitpid( $pid, WNOHANG) ) {

                # check the exit status of the command.
                if ( $? ) {
                    $self->_failed_process( $pid );
                } else {
                    $self->_succeeded_process( $pid );
                }
            }
        }

        unless ( keys %{ $data_of{ident $self}->{running} } ) {
            delete $data_of{ident $self}->{running};
        }
    }

    sub _succeeded_process {
        my ( $self, $pid ) = @_;

        my $group  = $data_of{ident $self}->{running}->{ $pid }->{group};
        my $source = $data_of{ident $self}->{running}->{ $pid }->{source};
        my $target = $data_of{ident $self}->{running}->{ $pid }->{target};
        my $start  = $data_of{ident $self}->{running}->{ $pid }->{start};

        # calculate time for this transfer
        my $end = new Benchmark;
        my $diff = timediff( $end, $start );
        # keep track of transfer time totals
        $total_time_of{ident $self} += $diff->[0];

        my $time = $self->_human_friendly_time( $diff->[0] );
        $logger->warn( "Succeeded: ($group) $source => $target ($time)" );

        $self->_mark_available( $group, $source );
        $self->_mark_completed( $group, $target );
        $self->_mark_available( $group, $target );

        delete $data_of{ident $self}->{running}->{ $pid };

        $self->_print_status( $group );
    }


    sub _failed_process {
        my ( $self, $pid ) = @_;

        my $group  = $data_of{ident $self}->{running}->{ $pid }->{group};
        my $source = $data_of{ident $self}->{running}->{ $pid }->{source};
        my $target = $data_of{ident $self}->{running}->{ $pid }->{target};
        my $start  = $data_of{ident $self}->{running}->{ $pid }->{start};

        # calculate time for this transfer
        my $end = new Benchmark;
        my $diff = timediff( $end, $start );
        # keep track of transfer time totals
        $total_time_of{ident $self} += $diff->[0];

        $logger->warn( "Failed: ($group) $source => $target ($diff->[0] seconds)" );

        # there was an error during the transfer, reschedule
        # it at the end of the list
        $self->_mark_available( $group, $source );
        my $fail_count = $self->_mark_failed( $group, $target );
        if ( $fail_count >= $max_failures_of{ident $self} ) {
            $logger->fatal( "Error: giving up on ($group) $target" );
        } else {
            $self->_mark_remaining( $group, $target );
        }

        delete $data_of{ident $self}->{running}->{ $pid };

        $self->_print_status( $group );
    }

    sub _get_available_servers {
        my ( $self, $group ) = @_;
        return unless $data_of{ident $self}->{available};
        return unless $data_of{ident $self}->{available}->{ $group };

        my @hosts = sort keys %{ $data_of{ident $self}->{available}->{ $group } };
        return @hosts;
    }

    sub _reserve_available_server {
        my ( $self, $group ) = @_;
        if ( $self->_get_remaining_servers( $group ) ) {
            my ( $server ) = $self->_get_available_servers( $group );
            $logger->debug( "Reserving ($group) $server" );
            $children_of{ident $self}->{ $server }++;
            return $server;
        }
    }

    sub _get_remaining_servers {
        my ( $self, $group ) = @_;

        return unless $data_of{ident $self}->{remaining};

        return unless $data_of{ident $self}->{remaining}->{ $group };

        my @hosts = sort keys %{ $data_of{ident $self}->{remaining}->{ $group } };
        return @hosts;
    }

    sub _reserve_remaining_server {
        my ( $self, $group ) = @_;

        if ( $self->_get_remaining_servers( $group ) ) {
            my $server = ( sort keys %{ $data_of{ident $self}->{remaining}->{ $group } } )[0];
            delete $data_of{ident $self}->{remaining}->{ $group }->{$server};
            $logger->debug( "Reserving ($group) $server" );

            # delete remaining data structure as groups are completed
            unless ( scalar keys %{ $data_of{ident $self}->{remaining}->{ $group } } ) {
                $logger->debug( "Group empty: $group" );
                delete $data_of{ident $self}->{remaining}->{ $group };
                unless ( scalar ( keys %{ $data_of{ident $self}->{remaining} } ) ) {
                    $logger->debug( "No servers remaining" );
                    delete $data_of{ident $self}->{remaining};
                }
            }
            return $server;
        }
    }

    sub _get_remaining_groups {
        my ( $self ) = @_;
        return unless $data_of{ident $self}->{remaining};
        my @keys = sort keys %{ $data_of{ident $self}->{remaining} };
        return unless scalar @keys;
        return @keys;
    }

    sub _mark_available {
        my ( $self, $group, $server ) = @_;

        # don't reschedule localhost for future syncs
        return if $server eq "localhost";

        $logger->debug( "Server available: ($group) $server" );
        $data_of{ident $self}->{available}->{ $group }->{$server} = 1;
    }

    sub _mark_remaining {
        my ( $self, $group, $server ) = @_;

        $logger->debug( "Server remaining: ($group) $server" );
        $data_of{ident $self}->{remaining}->{ $group }->{$server} = 1;
    }

    sub _mark_completed {
        my ( $self, $group, $server ) = @_;

        $logger->debug( "Server completed: ($group) $server" );
        push @{ $data_of{ident $self}->{completed}->{ $group } }, $server;
    }

    sub _mark_failed {
        my ( $self, $group, $server ) = @_;

        $logger->debug( "Server completed: ($group) $server" );
        $data_of{ident $self}->{failed}->{ $group }->{ $server }++;
        my $failures = $data_of{ident $self}->{failed}->{ $group }->{ $server };
        $logger->debug( "$failures failures for ($group) $server" );
        return $failures;
    }

}


1;

__END__

=head1 NAME

Net::CascadeCopy - Rapidly propagate (rsync/scp/...) files to many
servers in multiple locations.


=head1 SYNOPSIS

    use Net::CascadeCopy;

    # create a new CascadeCopy object
    my $ccp = Net::CascadeCopy->new( { ssh          => "/path/to/ssh",
                                       ssh_flags    => "-x -A",
                                       max_failures => 3,
                                       max_forks    => 2,
                                       output       => "log",
                                   } );

    # set the command and arguments to use to transfer file(s)
    $ccp->set_command( "rsync", "-rav --checksum --delete -e ssh" );

    # another example with scp instead
    $ccp->set_command( "/path/to/scp", "-p" );


    # set path on the local server
    $ccp->set_source_path( "/path/on/local/server" );
    # set path on all remote servers
    $ccp->set_target_path( "/path/on/remote/servers" );

    # add lists of servers in multiple datacenters
    $ccp->add_group( "datacenter1", \@dc1_servers );
    $ccp->add_group( "datacenter2", \@dc2_servers );

    # transfer all files
    $ccp->transfer();


=head1 DESCRIPTION

This module implements a scalable method of quickly propagating files
to a large number of servers in one or more locations via rsync or
scp.

A frequent solution to distributing a file or directory to a large
number of servers is to copy it from a central file server to all
other servers.  To speed this up, multiple file servers may be used,
or files may be copied in parallel until the inevitable bottleneck in
network/disk/cpu is reached.  These approaches run in O(n) time.

This module and the included script, ccp, take a much more efficient
approach that is O(log n).  Once the file(s) are been copied to a
remote server, that server will be promoted to be used as source
server for copying to remaining servers.  Thus, the rate of transfer
increases exponentially rather than linearly.

Servers can be specified in groups (e.g. datacenter) to prevent
copying across groups.  This maximizes the number of transfers done
over a local high-speed connection (LAN) while minimizing the number
of transfers over the WAN.

The number of multiple simultaneous transfers per source point is
configurable.  The total number of simultaneously forked processes is
limited via Proc::Queue, and is currently hard coded to 32.



=head1 CONSTRUCTOR

=over 8

=item new( { option => value } )

Returns a reference to a new use Net::CascadeCopy object.

Supported options:

=over 4

=item ssh => "/path/to/ssh"

Name or path of ssh script ot use to log in to each remote server to
begin a transfer to another remote server.  Default is simply "ssh" to
be invoked from $PATH.

=item ssh_flags => "-x -A"

Command line options to be passed to ssh script.  Default is to
disable X11 and enable agent forwarding.

=item max_failures => 3

The Maximum number of transfer failures to allow before giving up on a
target host.  Default is 3.

=item max_forks => 2

The maximum number of simultaneous transfers that should be running
per source server.  Default is 2.

=item output => undef

Specify options for child process output.  The default is to discard
stdout and display stderr.  "log" can be specified to redirect stdout
and stderr of each transfer to to ccp.sourcehost.targethost.log.
"stdout" option also exists which will not supress stdout, but this
option is only intended for debugging.

=back

=back

=head1 INTERFACE


=over 8

=item $self->add_group( $groupname, \@servers )

Add a group of servers.  Ideally all servers will be located in the
same datacenter.  This may be called multiple times with different
group names to create multiple groups.

=item $self->set_command( $command, $args )

Set the command and arguments that will be used to transfer files.
For example, "rsync" and "-ravuz" could be used for rsync, or "scp"
and "-p" could be used for scp.

=item $self->set_source_path( $path )

Specify the path on the local server where the source files reside.

=item $self->set_target_path( $path )

Specify the target path on the remote servers where the files should
be copied.

=item $self->transfer( )

Transfer all files.  Will not return until all files are transferred.

=back


=head1 BUGS AND LIMITATIONS

Note that this is still a beta release.

There is one known bug.  If an initial copy from the localhost to the
first server in one of the groups fails, it will not be retried.  the
real solution to this bug is to refactor the logic for the inital copy
from localhost.  The current logic is a hack.  Max forks should be
configured for localhost transfers, and localhost could be listed in a
group to allow it to be re-used by that group once all the intial
transfers to the first server in each group were completed.

If using rsync for the copy mechanism, it is recommended that you use
the "--delete" and "--checksum" options.  Otherwise, if the content of
the directory structure varies slightly from system to system, then
you may potentially sync different files from some servers than from
others.

Since the copies will be performed between machines, you must be able
to log into each source server to each target server (in the same
group).  Since empty passwords on ssh keys are insecure, the default
ssh arguments enable the ssh agent for authentication (the -A option).
Note that each server will need an entry in .ssh/known_hosts for each
other server.

Multiple syncs will be initialized within a few seconds on remote
hosts.  Ideally this could be configurable to wait a certain amount of
time before starting additional syncs.  This would give rsync some
time to finish computing checksums, a potential disk/cpu bottleneck,
and move into the network bottleneck phase before starting the next
transfer.

There is no timeout enforced in CascadeCopy yet.  A copy command that
hangs forever will prevent CascadeCopy from ever completing.

Please report problems to VVu@geekfarm.org.  Patches are welcome.

=head1 SEE ALSO

ccp - command line script distributed with this module

http://www.geekfarm.org/wu/muse/CascadeCopy.html


=head1 AUTHOR

Alex White  C<< <vvu@geekfarm.org> >>




=head1 LICENCE AND COPYRIGHT

Copyright (c) 2007, Alex White C<< <vvu@geekfarm.org> >>. All rights reserved.

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

- Redistributions of source code must retain the above copyright
  notice, this list of conditions and the following disclaimer.

- Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.

- Neither the name of the geekfarm.org nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
