package App::Controller::Root;
use Moose;
use namespace::autoclean;

BEGIN { extends 'Catalyst::Controller' }

__PACKAGE__->config(namespace => '');

sub index :Path :Args(0) {
    my ( $self, $c ) = @_;

    my $email = $c->request->params->{email};

    if (defined $email) {
        my $dbh = $c->model('DBI')->dbh;

        my $query = qq{
            SELECT *
            FROM(
                SELECT created, int_id, str FROM public.log WHERE address = ?
                UNION
                SELECT created, int_id, str FROM public.message
                WHERE int_id IN (SELECT distinct(int_id) FROM public.log WHERE address = ?)
            ) tmp
            ORDER BY int_id, created LIMIT 100
        };

        my $sth = $dbh->prepare(qq{$query});
        my $rv = $sth->execute($email, $email);
        $c->stash->{rows} = $sth->rows;
        my $array = $sth->fetchall_arrayref();
        $c->stash->{list} = $array;
    }

    $c->stash->{template} = 'index.tt';
    $c->forward('View::TT');
}

sub default :Path {
    my ( $self, $c ) = @_;
    $c->response->body( 'Page not found' );
    $c->response->status(404);
}

sub end : ActionClass('RenderView') {}

__PACKAGE__->meta->make_immutable;

1;
