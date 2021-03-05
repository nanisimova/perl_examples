use strict;
use DBI;
#use Time::Local;
#use Time::Piece;
use Data::Dumper;

my $regexp_base = '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+(.*)$';

my $dbh = DBI->connect("DBI:Pg:dbname=gaz;host=localhost;port=5432","root","12345", { AutoCommit => 1 })
or die "Connect to database 'gaz' failed";

open(my $fh, "<", "out");

while ( my $line = <$fh> ) {
    chomp($line);
    process($line);

}

close($fh);
$dbh->disconnect();


sub process {
    my $line = shift;
    my @parsed;
    my @str_for_insert = $line =~ "$regexp_base";

    # 2012-02-13 14:49:49 1RwtTh-000JAQ-QP <= <> R=1RwCmb-000Kcp-VB U=mailnull P=local S=1425
    @parsed = $line =~ '^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+([a-zA-Z0-9]+-[a-zA-Z0-9]+-[a-zA-Z0-9]+)\s+\<\=\s+\<\>.*';
    if ( scalar @parsed == 2 ) {
        $dbh->do( qq{ INSERT INTO log ( created, int_id, str ) VALUES ( ?, ?, ? ); }, undef, $parsed[0], $parsed[1], $str_for_insert[0] );
        return;
    }

    # 2012-02-13 14:41:06 1RwtLG-000M0q-UZ <= ysxeuila@rushost.ru H=rtmail.rushost.ru [109.70.26.4] P=esmtp S=2291 id=rt-3.8.8-21162-1329129666-656.3913218-6-0@rushost.ru
    @parsed = $line =~ '^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+([a-zA-Z0-9]+-[a-zA-Z0-9]+-[a-zA-Z0-9]+)\s+\<\=\s+(\S+@\S+)\s+.*';
    if ( scalar @parsed == 3 ) {
        my @int_id = $line =~ 'id=(\S+)';
        if ( scalar @int_id > 0 ) {
            $dbh->do( qq{ INSERT INTO message ( created, id, int_id, str ) VALUES ( ?, ?, ?, ? ); }, undef, $parsed[0], $int_id[0], $parsed[1], $str_for_insert[0] );
            return;
        }
    }

    # 2012-02-13 14:39:23 1RwtEg-0002zL-Mh ** thauocarief@yahoo.com R=dnslookup T=remote_smtp: SMTP error from remote mail server after end of data: host mta7.am0.yahoodns.net [66.94.237.64]: 554 delivery error: dd Sorry your message to thauocarief@yahoo.com cannot be delivered. This account has been disabled or discontinued [#102]. - mta1229.mail.mud.yahoo.com
    # 2012-02-13 14:42:27 1Rvi19-000Eqz-OX == rurqo@asciel.msk.su R=dnslookup T=remote_smtp defer (60): Operation timed out
    # 2012-02-13 14:39:57 1RwtJY-0009RI-E4 -> ldtyzggfqejxo@mail.ru R=dnslookup T=remote_smtp H=mxs.mail.ru [94.100.176.20] C="250 OK id=1RwtK9-0004SS-Fm"
    # 2012-02-13 14:39:28 1RwtHE-0004d8-JE => omyizgutrx@perm.comstar-r.ru R=dnslookup T=remote_smtp H=mail2.perm.comstar-r.ru [195.222.159.238] C="250 2.0.0 Ok: queued as ACC5184541"
    @parsed = $line =~ '^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+([a-zA-Z0-9]+-[a-zA-Z0-9]+-[a-zA-Z0-9]+)\s+(\=\=|\*\*|\-\>|\=\>)\s+(\S+@\S+)\s+.*';
    if ( scalar @parsed == 4 ) {
        $dbh->do( qq{ INSERT INTO log ( created, int_id, str, address ) VALUES ( ?, ?, ?, ? ); }, undef, $parsed[0], $parsed[1], $str_for_insert[0], $parsed[3] );
        return;
    }

    # 2012-02-13 14:39:22 1RwtJa-000AGs-7A => :blackhole: <tpxmuwr@somehost.ru> R=blackhole_router
    @parsed = $line =~ '^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+([a-zA-Z0-9]+-[a-zA-Z0-9]+-[a-zA-Z0-9]+)\s+\=\>.*\<(\S+@\S+)\>\s+.*';
    if ( scalar @parsed == 3 ) {
        $dbh->do( qq{ INSERT INTO log ( created, int_id, str, address ) VALUES ( ?, ?, ?, ? ); }, undef, $parsed[0], $parsed[1], $str_for_insert[0], $parsed[2] );
        return;
    }

    #2012-02-13 14:46:12 1QMLXK-000Hjs-2Q Spool file is locked (another process is handling this message)
    #2012-02-13 14:45:32 1RwclY-00082W-N9 glenic.com [109.70.26.36] Operation timed out
    #2012-02-13 14:39:22 1Rm0kE-00027I-IY Completed
    @parsed = $line =~ '^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+([a-zA-Z0-9]+-[a-zA-Z0-9]+-[a-zA-Z0-9]+)\s+(?!\*\*)(?!\=\=)(?!\-\>)(?!\<\=)(?!\=\>).*';
    if ( scalar @parsed == 2 ) {
        $dbh->do( qq{ INSERT INTO log ( created, int_id, str ) VALUES ( ?, ?, ? ); }, undef, $parsed[0], $parsed[1], $str_for_insert[0] );
        return;
    }

    #2012-02-13 14:39:47 SMTP connection from mail.somehost.com [84.154.134.45] closed by QUIT && etc.
    print("ERROR ", $line, "\n")
}





