import re
import datetime
import psycopg2

regexp_base = r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+(.*)$'
# 2012-02-13 14:49:49 1RwtTh-000JAQ-QP <= <> R=1RwCmb-000Kcp-VB U=mailnull P=local S=1425
regexp1 = r'^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})\s+([a-zA-Z0-9]+-[a-zA-Z0-9]+-[a-zA-Z0-9]+)\s+\<\=\s+\<\>.*'

# 2012-02-13 14:41:06 1RwtLG-000M0q-UZ <= ysxeuila@rushost.ru H=rtmail.rushost.ru [109.70.26.4] P=esmtp S=2291 id=rt-3.8.8-21162-1329129666-656.3913218-6-0@rushost.ru
regexp2 = r'^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})\s+([a-zA-Z0-9]+-[a-zA-Z0-9]+-[a-zA-Z0-9]+)\s+\<\=\s+(\S+@\S+)\s+.*'


# 2012-02-13 14:39:23 1RwtEg-0002zL-Mh ** thauocarief@yahoo.com R=dnslookup T=remote_smtp: SMTP error from remote mail server after end of data: host mta7.am0.yahoodns.net [66.94.237.64]: 554 delivery error: dd Sorry your message to thauocarief@yahoo.com cannot be delivered. This account has been disabled or discontinued [#102]. - mta1229.mail.mud.yahoo.com
# 2012-02-13 14:42:27 1Rvi19-000Eqz-OX == rurqo@asciel.msk.su R=dnslookup T=remote_smtp defer (60): Operation timed out
# 2012-02-13 14:39:57 1RwtJY-0009RI-E4 -> ldtyzggfqejxo@mail.ru R=dnslookup T=remote_smtp H=mxs.mail.ru [94.100.176.20] C="250 OK id=1RwtK9-0004SS-Fm"
# 2012-02-13 14:39:28 1RwtHE-0004d8-JE => omyizgutrx@perm.comstar-r.ru R=dnslookup T=remote_smtp H=mail2.perm.comstar-r.ru [195.222.159.238] C="250 2.0.0 Ok: queued as ACC5184541"
regexp3 = r'^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})\s+([a-zA-Z0-9]+-[a-zA-Z0-9]+-[a-zA-Z0-9]+)\s+(\=\=|\*\*|\-\>|\=\>)\s+(\S+@\S+)\s+.*'

# 2012-02-13 14:39:22 1RwtJa-000AGs-7A => :blackhole: <tpxmuwr@somehost.ru> R=blackhole_router
regexp4 = r'^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})\s+([a-zA-Z0-9]+-[a-zA-Z0-9]+-[a-zA-Z0-9]+)\s+\=\>.*\<(\S+@\S+)\>\s+.*'


#2012-02-13 14:46:12 1QMLXK-000Hjs-2Q Spool file is locked (another process is handling this message)
#2012-02-13 14:45:32 1RwclY-00082W-N9 glenic.com [109.70.26.36] Operation timed out
#2012-02-13 14:39:22 1Rm0kE-00027I-IY Completed
regexp5 = r'^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})\s+([a-zA-Z0-9]+-[a-zA-Z0-9]+-[a-zA-Z0-9]+)\s+(?!\*\*)(?!\=\=)(?!\-\>)(?!\<\=)(?!\=\>).*'

#2012-02-13 14:39:47 SMTP connection from mail.somehost.com [84.154.134.45] closed by QUIT
regexp6 = r'^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})\s+(?!.*[a-zA-Z0-9]+-[a-zA-Z0-9]+-[a-zA-Z0-9]+).*\s+.*'


def add_log(ts, args):
    global cur, conn
    fields = ['int_id', 'str', 'address']
    query = "INSERT INTO log (created, " + ", ".join([fields[i] for i in range(len(args))]) + ") VALUES (to_timestamp(%s), " + ", ".join(['%s' for _ in range(len(args))]) + ")"

    cur.execute(
        query, (
            int(datetime.datetime(*[int(i) for i in ts]).timestamp()),
            *args
        )
    )
    conn.commit()


def add_message(ts, args):
    global cur, conn
    fields = ['id', 'int_id', 'str']
    query = "INSERT INTO message (created, " + ", ".join([fields[i] for i in range(len(args))]) + ") VALUES (to_timestamp(%s), " + ", ".join(['%s' for _ in range(len(args))]) + ")"

    cur.execute(
        query, (
            int(datetime.datetime(*[int(i) for i in ts]).timestamp()),
            *args
        )
    )
    conn.commit()



def process(line):
    line = line.rstrip("\n")

    global regexp_base, regexp1, regexp2, regexp3, regexp4, regexp5, regexp6

    queued_as = re.search(regexp1, line)
    if queued_as is not None:
        g = queued_as.groups()
        if len(g) == 7:
            add_log(g[0:6], [g[6], re.search(regexp_base, line).groups()[0]])
        return


    queued_as = re.search(regexp2, line)
    if queued_as is not None:
        g = queued_as.groups()
        int_id = re.search(r'id=(\S+)', line)
        if len(g) == 8 and int_id is not None:
            add_message(g[0:6], [int_id.groups()[0], g[6], re.search(regexp_base, line).groups()[0]])
        return


    queued_as = re.search(regexp3, line)
    if queued_as is not None:
        g = queued_as.groups()
        if len(g) == 9:
            add_log(g[0:6], [g[6], re.search(regexp_base, line).groups()[0], g[8]])
        return


    queued_as = re.search(regexp4, line)
    if queued_as is not None:
        g = queued_as.groups()
        if len(g) == 8:
            add_log(g[0:6], [g[6], re.search(regexp_base, line).groups()[0], g[7]])
        return


    queued_as = re.search(regexp5, line)
    if queued_as is not None:
        g = queued_as.groups()
        if len(g) == 7:
            add_log(g[0:6], [g[6], re.search(regexp_base, line).groups()[0]])
        return

    queued_as = re.search(regexp6, line)
    if queued_as is not None:
        g = queued_as.groups()
        print(re.search(regexp_base, line).groups()[0])
        return

    print("ERROR ", line)


# подключаемся к БД и сохраняем каждую строку файла, кроме строк вида
# "SMTP connection from mail.somehost.com [84.154.134.45] closed by QUIT"

conn = psycopg2.connect(host="localhost", port = 5432, database="gaz", user="root", password="12345")
cur = conn.cursor()

fd = open('out', 'r')

for line in fd:
    process(line)

fd.close()

cur.close()
conn.close()



