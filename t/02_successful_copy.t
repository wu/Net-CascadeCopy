#!perl
use Test::More tests => 4;

use Log::Log4perl qw(:easy);

my $conf =<<END_LOG4PERLCONF;
# Screen output at INFO level
log4perl.rootLogger=DEBUG, SCREEN

# Info to screen and logfile
log4perl.appender.SCREEN.Threshold=INFO
log4perl.appender.SCREEN=Log::Log4perl::Appender::ScreenColoredLevels
log4perl.appender.SCREEN.layout=PatternLayout
log4perl.appender.SCREEN.layout.ConversionPattern=%d %m%n
log4perl.appender.SCREEN.stderr=0

END_LOG4PERLCONF

Log::Log4perl::init( \$conf );

use Net::CascadeCopy;

my $ccp;
ok( $ccp = Net::CascadeCopy->new( { ssh => 'echo' } ),
    "Creating a new ccp object"
);

ok( $ccp->set_command( "echo" ),
    "Setting the command to 'echo"
);

ok( $ccp->set_source_path( "/foo" ),
    "Setting the source path"
);

ok( $ccp->set_target_path( "/foo" ),
    "Setting the target path"
);

my @hosts1 = map { "host$_" } 101 .. 105;
ok( $ccp->add_group( "first", [ @hosts1 ] ),
    "Adding first host group"
);

my @hosts2 = map { "host$_" } 201 .. 205;
ok( $ccp->add_group( "second", [ @hosts2 ] ),
    "Adding second host group"
);

{
    is_deeply( [ $ccp->get_available_servers( 'first' ) ],
               [ 'localhost' ],
               "Checking that only localhost available in first group"
           );

    is_deeply( [ $ccp->get_available_servers( 'second' ) ],
               [ 'localhost' ],
               "Checking that only localhost available in second group"
           );

    is_deeply( [ $ccp->get_remaining_servers( 'first' ) ],
               \@hosts1,
               "Checking that all servers in first group are in the 'remaining' group"
           );

    is_deeply( [ $ccp->get_remaining_servers( 'second' ) ],
               \@hosts2,
               "Checking that all servers in second group are in the 'remaining' group"
           );
}

ok( $ccp->_transfer_loop(),
    "Executing a single transfer loop"
);

sleep 1;

$ccp->_check_for_completed_processes();

{
    is_deeply( [ $ccp->get_remaining_servers( 'first' ) ],
               [ @hosts1[ 1 .. $#hosts1 ] ],
               "Checking that one servers is no longer in the first group"
           );

    is_deeply( [ $ccp->get_remaining_servers( 'second' ) ],
               [ @hosts2[ 1 .. $#hosts2 ] ],
               "Checking that one server is no longer in the second group"
           );

    # code_smell: localhost shouldn't really be in this list
    is_deeply( [ $ccp->get_available_servers( 'first' ) ],
               [ $hosts1[0], 'localhost' ],
               "Checking that one servers is now available in first dc"
           );

    is_deeply( [ $ccp->get_available_servers( 'second' ) ],
               [ $hosts2[0], 'localhost' ],
               "Checking that one servers is now available in second dc"
           );
}


ok( $ccp->_transfer_loop(),
    "Executing a single transfer loop"
);

sleep 1;

$ccp->_check_for_completed_processes();

{
    is_deeply( [ sort $ccp->get_remaining_servers( 'first' ) ],
               [ 'host104', 'host105' ],
               "Checking that host 104+105 are remaining"
           );

    is_deeply( [ sort $ccp->get_remaining_servers( 'second' ) ],
               [ 'host204', 'host205' ],
               "Checking that host 204+205 are remaining"
           );

    # code_smell: localhost shouldn't really be in this list
    use YAML;
    print YAML::Dump $ccp->get_available_servers( 'first' );

    is_deeply( [ sort $ccp->get_available_servers( 'first' ) ],
               [ 'host101', 'host102', 'host103' ],
               "Checking that hosts 101-103 are now available for transfer"
           );

    is_deeply( [ sort $ccp->get_available_servers( 'second' ) ],
               [ 'host201', 'host202', 'host203' ],
               "Checking that hosts 201-203 are now available for transfer"
           );
}
