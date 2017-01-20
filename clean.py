#!/usr/bin/env python3

import aiomysql
import asyncio
import sys
import os
from hashlib import md5
from envparse import env

assert sys.version_info >= (3, 4), "Require python version >= 3.4"

envfile = os.path.join(os.getcwd(), '.env')
if os.path.isfile(envfile):
    env.read_envfile(envfile)

DOMAIN_ZONE = env('DOMAIN_ZONE', default='domain.test')
DOMAIN_ID = env.int('DOMAIN_ID', default=1)
DOMAIN_TTL = env.int('DOMAIN_TTL', default=3600)
MYSQL_USER = env('MYSQL_USER', default='pdns')
MYSQL_PASSWORD = env('MYSQL_PASSWORD')
MYSQL_HOST = env('MYSQL_HOST', default='localhost')
MYSQL_DB = env('MYSQL_DB', default='domain_test')


# Файл с запроса
# select hostid, max(updated) as uuu from machine group by hostid having max(updated) < '2016-06-0' order by uuu;
# на notify
OLD_MACHINE_FILE = '/root/old_machine_id.txt'


@asyncio.coroutine
def main():
    query2 = "DELETE FROM records WHERE name = %s"
    conn = yield  from aiomysql.connect(
            host=MYSQL_HOST, port=3306,
            user=MYSQL_USER, password=MYSQL_PASSWORD,
            db=MYSQL_DB, loop=loop, echo=True,
    )

    f = open(OLD_MACHINE_FILE)
    old_ids = f.readlines()
    f.close()

    for line in old_ids:
        raw_id = line.split()[0]
        id = md5(raw_id.encode()).hexdigest()[0:7]
        name = 'l%s.%s' % (id, DOMAIN_ZONE)
        cur = yield from conn.cursor()
        query1 = "SELECT * FROM records WHERE name = %s"
        yield from cur.execute(query1, name)
        r = yield from cur.fetchone()
        yield from cur.close()
        if r:
            print("Name %s exists" % str(r))
            if len(sys.argv) > 1 and sys.argv[1] == '-f':
                print("Removing")
                cur = yield from conn.cursor()
                yield from cur.execute(query2, name)
                r = yield from cur.fetchone()
                yield from cur.close()
                yield from conn.commit()

#        else:
#            print("Name %s not exists" % name)
    yield from conn.ensure_closed()



if __name__ == '__main__':
     loop = asyncio.get_event_loop()
     loop.run_until_complete(main())
