package App::View::TT;
use Moose;
use namespace::autoclean;

extends 'Catalyst::View::TT';

__PACKAGE__->config(
    TEMPLATE_EXTENSION => '.tt',
    render_die => 1,
    CATALYST_VAR => 'c',
    ENCODING     => 'utf-8',
);

1;
