package App::Model::DBI;

use base 'Catalyst::Model::DBI';

__PACKAGE__->config(
    dsn           => 'DBI:Pg:dbname=gaz;host=localhost',
    username      => 'root',
    password      => '12345',
    options       => { AutoCommit => 1 },
    loglevel      => 1,
    mysql_enable_utf8 => 1
);

1;


