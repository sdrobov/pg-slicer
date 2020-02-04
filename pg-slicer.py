#!/usr/bin/env python3
import argparse
from os import getenv

import psycopg2


class Options:
    def __init__(self):
        self.host = None
        self.port = None
        self.user = None
        self.password = None
        self.DBNAME = None
        self.limit = None
        self.no_privileges = None
        self.no_publications = None
        self.no_subscriptions = None


def parse_cli_args():
    parser = argparse.ArgumentParser(description="PostgreSQL DB data slicer", add_help=False)
    parser.add_argument('-h', '--host', dest='host', default=getenv('PGS_HOST'))
    parser.add_argument('-p', '--port', type=int, dest='port', default=getenv('PGS_PORT'))
    parser.add_argument('-U', '--user', dest='user', default=getenv('PGS_USER'))
    parser.add_argument('-W', '--password', dest='password', default=getenv('PGS_PASSWORD'))
    parser.add_argument('-l', '--limit', type=int, dest='limit', default=getenv('PGS_LIMIT', 100))
    parser.add_argument('--no-privileges', action='store_true', dest='no_privileges',
                        default=getenv('PGS_NO_PRIVILEGES'))
    parser.add_argument('--no-publications', action='store_true', dest='no_publications',
                        default=getenv('PGS_NO_PUBLICATIONS'))
    parser.add_argument('--no-subscriptions', action='store_true', dest='no_subscriptions',
                        default=getenv('PGS_NO_SUBSCRIPTIONS'))
    parser.add_argument('--help', action='help')
    parser.add_argument('DBNAME')

    return parser.parse_args(namespace=Options)


def build_dsn(options):
    params = {
        'dbname': options.DBNAME
    }

    if options.user:
        params['user'] = options.user

    if options.password:
        params['password'] = options.password

    if options.host:
        params['host'] = options.host

    if options.port:
        params['port'] = options.port

    return ' '.join([f'{key}={value}' for key, value in params.items()])


def describe_table(table_name, cursor):
    cursor.execute('SELECT c.oid FROM pg_catalog.pg_class c '
                   'LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace '
                   'WHERE c.relname OPERATOR(pg_catalog.~) %s '
                   'AND pg_catalog.pg_table_is_visible(c.oid)', (f'^({table_name})$',))
    oid = cursor.fetchone()[0]

    cursor.execute('SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, '
                   'c.relrowsecurity, c.relforcerowsecurity, c.relhasoids, c.reltablespace, '
                   'CASE WHEN c.reloftype = 0 THEN \'\' '
                   'ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, '
                   'c.relreplident '
                   'FROM pg_catalog.pg_class c '
                   'LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid) '
                   'WHERE c.oid = %s', (oid,))
    strange_fields = cursor.fetchone()

    cursor.execute('SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), '
                   '(SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128) '
                   'FROM pg_catalog.pg_attrdef d '
                   'WHERE d.adrelid = a.attrelid '
                   'AND d.adnum = a.attnum '
                   'AND a.atthasdef), a.attnotnull, a.attnum, (SELECT c.collname '
                   'FROM pg_catalog.pg_collation c, pg_catalog.pg_type t '
                   'WHERE c.oid = a.attcollation '
                   'AND t.oid = a.atttypid '
                   'AND a.attcollation <> t.typcollation) AS attcollation '
                   'FROM pg_catalog.pg_attribute a '
                   'WHERE a.attrelid = %s '
                   'AND a.attnum > 0 '
                   'AND NOT a.attisdropped '
                   'ORDER BY a.attnum', (oid,))
    columns = cursor.fetchall()

    cursor.execute('SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, '
                   'i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), '
                   'pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, '
                   'condeferred, i.indisreplident, c2.reltablespace '
                   'FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i '
                   'LEFT JOIN pg_catalog.pg_constraint con ON ('
                   'conrelid = i.indrelid '
                   'AND conindid = i.indexrelid '
                   'AND contype IN (\'p\',\'u\',\'x\')'
                   ')'
                   'WHERE c.oid = %s '
                   'AND c.oid = i.indrelid '
                   'AND i.indexrelid = c2.oid '
                   'ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname', (oid,))
    indexes = cursor.fetchall()

    cursor.execute('SELECT conname, confrelid::pg_catalog.regclass, '
                   'pg_catalog.pg_get_constraintdef(r.oid, true) as condef '
                   'FROM pg_catalog.pg_constraint r '
                   'WHERE r.conrelid = %s '
                   'AND r.contype = \'f\' '
                   'ORDER BY 1', (oid,))
    parents = cursor.fetchall()

    cursor.execute('SELECT conname, conrelid::pg_catalog.regclass, '
                   'pg_catalog.pg_get_constraintdef(c.oid, true) as condef '
                   'FROM pg_catalog.pg_constraint c '
                   'WHERE c.confrelid = %s '
                   'AND c.contype = \'f\' '
                   'ORDER BY 1', (oid,))
    children = cursor.fetchall()

    table_description = {
        'columns': [],
        'indexes': [],
        'relations': [],
    }

    for column in columns:
        table_description['columns'].append({
            'name': column[0],
            'type': column[1],
            'default': column[2],
            'not_null': column[3],
            'position': column[4],
        })

    for index in indexes:
        table_description['indexes'].append({
            'name': index[0],
            'is_primary': index[1],
            'is_unique': index[2],
            'create_query': index[5],
            'constraint_query': index[6],
        })

    for parent in parents:
        table_description['relations'].append({
            'table': parent[1],
            'fk_name': parent[0],
            'create_query': parent[2],
            'relation': 'parent',
        })

    for child in children:
        table_description['relations'].append({
            'table': child[1],
            'fk_name': child[0],
            'create_query': child[2],
            'relation': 'child',
        })

    return table_description


def main():
    options = parse_cli_args()
    dsn = build_dsn(options)
    connection = psycopg2.connect(dsn)
    cursor = connection.cursor()

    cursor.execute('SELECT table_name FROM information_schema.tables '
                   'WHERE table_schema = \'public\'')

    table_names = [row[0] for row in cursor.fetchall()]

    tables = {}
    for table_name in table_names:
        tables[table_name] = describe_table(table_name, cursor)

    print(tables)


if __name__ == '__main__':
    main()
