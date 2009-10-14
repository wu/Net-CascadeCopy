use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name         => 'Net::CascadeCopy',
    license             => 'perl',
    dist_author         => 'Alex White <vvu@geekfarm.org>',
    dist_version_from   => 'lib/Net/CascadeCopy.pm',
    requires => {
        'Test::More' => 0,
        'version'    => 0,
    },
    add_to_cleanup      => [ 'Net-CascadeCopy-*' ],
);

$builder->create_build_script();
