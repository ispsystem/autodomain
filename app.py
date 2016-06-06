#!/usr/bin/env python3

import sys
import asyncio
import aiomysql
import logging
import os
from envparse import env
from time import time
from aiohttp import web, ClientSession, TCPConnector

assert sys.version_info >= (3, 4), "Require python version >= 3.4"

envfile = os.path.join(os.getcwd(), '.env')
if os.path.isfile(envfile):
    env.read_envfile(envfile)

KEY = env('KEY', default='SoMeKeY')
DOMAIN_ZONE = env('DOMAIN_ZONE', default='domain.test')
DOMAIN_ID = env.int('DOMAIN_ID', default=1)
DOMAIN_TTL = env.int('DOMAIN_TTL', default=3600)
MYSQL_USER = env('MYSQL_USER', default='pdns')
MYSQL_PASSWORD = env('MYSQL_PASSWORD')
MYSQL_HOST = env('MYSQL_HOST', default='localhost')
MYSQL_DB = env('MYSQ_DB', default='domain_test')

MYSQL_POOL_MINSIZE = 2
MYSQL_POOL_MAXSIZE = 40


@asyncio.coroutine
def authorize(app, handler):
    """Checking valid key"""
    @asyncio.coroutine
    def middleware(request):
        if not request.GET.get('key') == KEY:
            raise web.HTTPForbidden(body=b'Invalid key\n')
            return handler(request)
        else:
            return (yield from handler(request))
    return middleware

@asyncio.coroutine
def is_record_exists(name, conn):
    cur = yield from conn.cursor()
    query = """SELECT id FROM records WHERE name = %s"""
    yield from cur.execute(query, (name,))
    r = yield from cur.fetchone()
    yield from cur.close()
    if r:
        return True
    else:
        return False

@asyncio.coroutine
def create_record(name, ip, conn):
    cur = yield from conn.cursor()
    cur_time = int(time())
    # change_date используется powerdns для автоматиеческого обновления SOA
    # Если SOA указан равный нулю, то используется последний из change_date
    # Бояться одинаковых значений не стоит, так как powerdns всё равно инфорамацию отдаёт
    # с некоторой задержкой, в результате гарантировано все значения с одним timestamp будут в памяти
    query = """INSERT INTO records (domain_id, name, type, content, ttl, prio, change_date)""" \
            """values(%s, %s, 'A', %s, %s, 0, %s)"""
    yield from cur.execute(query, (DOMAIN_ID, name, ip, DOMAIN_TTL, cur_time))
    yield from cur.close()

@asyncio.coroutine
def update_record(name, ip, conn):
    cur = yield from conn.cursor()
    cur_time = int(time())
    query = """UPDATE records SET content=%s , change_date = %s  WHERE name=%s and domain_id=%s"""
    yield from cur.execute(query, (ip, cur_time, name, DOMAIN_ID))
    yield from cur.close()

@asyncio.coroutine
def create_domain(request):
    id = request.GET.get('id')
    name = 'l%s.%s' % (id, DOMAIN_ZONE)
    ip = request.GET.get('ip')
    with (yield from request.app.pool) as conn:
        try:
            if (yield from is_record_exists(name, conn)):
                logging.debug('Domain %s exist' % name)
                yield from update_record(name, ip, conn)
            else:
                logging.debug('Domain %s not exist' % name)
                yield from create_record(name, ip, conn)
                text = 'Created'
            yield from conn.commit()
            return web.Response(text=name)
        except Exception as exc:
            yield from conn.rollback()
            logging.exception(exc)
            return web.HTTPInternalServerError()

@asyncio.coroutine
def remove_domain(request):
    id = request.GET.get('id')
    name = 'l%s.%s' % (id, DOMAIN_ZONE)
    with (yield from request.app.pool) as conn:
        try:
            cur = yield from conn.cursor()
            query = """DELETE FROM records WHERE name = %s"""
            yield from cur.execute(query, (name,))
            query = """ UPDATE records SET change_date = (""" \
                    """     SELECT max(t.max_date) FROM (""" \
                    """         SELECT MAX(change_date)+1 as max_date FROM records""" \
                    """     ) as t""" \
                    """ ) """ \
                    """ WHERE records.type = 'SOA' AND records.domain_id = %s;"""
            yield from cur.execute(query, (DOMAIN_ID,))
            yield from conn.commit()
            return web.Response(text='OK')
        except Exception as exc:
            yield from conn.rollback()
            logging.exception(exc)
            return web.HTTPInternalServerError()
        finally:
            yield from cur.close()


@asyncio.coroutine
def connect(loop):
    return (yield from aiomysql.create_pool(
            host=MYSQL_HOST, port=3306,
            user=MYSQL_USER, password=MYSQL_PASSWORD,
            db=MYSQL_DB, loop=loop, echo=True,
            minsize=MYSQL_POOL_MINSIZE, maxsize=MYSQL_POOL_MAXSIZE
        ))

if __name__ == '__main__':

    loop = asyncio.get_event_loop()
    logging.getLogger().setLevel(logging.DEBUG)

    app = web.Application(middlewares=[authorize], loop=loop)

    for r in ['/create', '/create/', '/create.php', '/create.html']:
        app.router.add_route('GET', r, create_domain)
    for r in ['/delete', '/delete/', '/delete.php', '/delete.html']:
        app.router.add_route('GET', r, remove_domain)

    pool = loop.run_until_complete(connect(loop))
    app.pool = pool

    web.run_app(app)
    sys.exit(0)

    # Next code unused
    handler = app.make_handler()
    srv = loop.run_until_complete(loop.create_server(
        handler, '127.0.0.1', '8080', ssl=None
    ))

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        logging.info('Stopping')
    finally:
        srv.close()
        loop.run_until_complete(srv.wait_closed())
        app.pool.close()
        loop.run_until_complete(app.pool.wait_closed())
        loop.run_until_complete(app.shutdown())
        loop.run_until_complete(handler.finish_connections(2))
        loop.run_until_complete(app.cleanup())
        loop.close()
