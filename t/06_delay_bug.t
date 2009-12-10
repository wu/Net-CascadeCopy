#!perl
use strict;

use Net::CascadeCopy;
use Test::More tests => 7;
use Test::Differences;

#use Log::Log4perl qw(:easy);
#Log::Log4perl->easy_init($DEBUG);
#my $logger = get_logger( 'default' );

my $ccp;
ok( $ccp = Net::CascadeCopy->new( { ssh => 'sleep 5; echo' } ),
    "Creating a new ccp object"
);

ok( $ccp->set_command( "sleep 3; echo" ),
    "Setting the command to 'echo"
);

ok( $ccp->set_source_path( "/foo" ),
    "Setting the source path"
);

ok( $ccp->set_target_path( "/foo" ),
    "Setting the target path"
);

my @hosts1 = map { "host$_" } 101 .. 110;
ok( $ccp->add_group( "first", [ @hosts1 ] ),
    "Adding first host group"
);

$ccp->transfer();

my $map = $ccp->get_transfer_map();

eq_or_diff( [ sort keys %{ $map } ],
            [ qw( host101 host102 host103 localhost ) ],
            "Checking source hosts in the transfer map"
        );

eq_or_diff( [ keys %{ $map->{localhost} } ],
            [ 'host101' ],
            "Checking that localhost only xferred to host101"
        );

