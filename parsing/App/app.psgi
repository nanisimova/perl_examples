use strict;
use warnings;

use App;

my $app = App->apply_default_middlewares(App->psgi_app);
$app;

