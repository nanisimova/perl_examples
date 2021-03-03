package App;
use Moose;
use namespace::autoclean;

use Catalyst::Runtime 5.80;

use Catalyst qw/
    -Debug
    ConfigLoader
    Static::Simple
/;

extends 'Catalyst';

our $VERSION = '0.01';

__PACKAGE__->config(
    name => 'App',
    disable_component_resolution_regex_fallback => 1,
    enable_catalyst_header => 1,
    encoding => 'UTF-8',
    'View::TT' => {
        INCLUDE_PATH => [
            __PACKAGE__->path_to( 'root', 'src' ),
        ],
    },
);

__PACKAGE__->setup();

1;
