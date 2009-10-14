use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'Net::CascadeCopy',
    AUTHOR              => 'Alex White <vvu@geekfarm.org>',
    VERSION_FROM        => 'lib/Net/CascadeCopy.pm',
    ABSTRACT_FROM       => 'lib/Net/CascadeCopy.pm',
    EXE_FILES           => [ qw[ scripts/ccp ] ],
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More'        => 0,
        'version'           => 0,
        'Log::Log4perl'     => 0,
        'Getopt::Long'      => 0,
        'Pod::Usage'        => 0,
        'Proc::Queue'       => 0,
        'Class::Std::Utils' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'Net-CascadeCopy-*' },
);
