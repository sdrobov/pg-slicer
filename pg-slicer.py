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
                   'pg_catalog.pg_get_constraintdef(r.oid, true) as condef, \'parent\' '
                   'FROM pg_catalog.pg_constraint r '
                   'WHERE r.conrelid = %s '
                   'AND r.contype = \'f\' '
                   'ORDER BY 1', (oid,))
    parents = cursor.fetchall()

    cursor.execute('SELECT conname, conrelid::pg_catalog.regclass, '
                   'pg_catalog.pg_get_constraintdef(c.oid, true) as condef, \'child\' '
                   'FROM pg_catalog.pg_constraint c '
                   'WHERE c.confrelid = %s '
                   'AND c.contype = \'f\' '
                   'ORDER BY 1', (oid,))
    children = cursor.fetchall()

    relations = parents + children

    return {
        'columns': [{
            'name': column[0],
            'type': column[1],
            'default': column[2],
            'not_null': column[3],
            'position': column[4],
        } for column in columns],
        'indexes': [{
            'name': index[0],
            'is_primary': index[1],
            'is_unique': index[2],
            'create_query': index[5],
            'constraint_query': index[6],
        } for index in indexes],
        'relations': [{
            'table': relation[1],
            'fk_name': relation[0],
            'create_query': relation[2],
            'relation': relation[3],
        } for relation in relations],
    }


def get_table_names(cursor):
    cursor.execute('SELECT table_name FROM information_schema.tables '
                   'WHERE table_schema = \'public\'')

    return [row[0] for row in cursor.fetchall()]


def get_root_tables(tables):
    root_rables = []

    for name, props in tables.items():
        root_table = True
        for relation in props['relations']:
            if relation['relation'] == 'parent':
                root_table = False

                break

        if root_table:
            root_rables.append(name)

    return root_rables


def generate_create_table(table_name, tables):
    table_query = f'CREATE TABLE IF NOT EXISTS {table_name} (\n'

    column_queries = []
    index_queries = [index['create_query'] for index in tables[table_name]['indexes'] if
                     not index['constraint_query']]
    constraint_queries = [index['constraint_query'] for index in tables[table_name]['indexes'] if
                          index['constraint_query']]

    for column in tables[table_name]['columns']:
        col_name = column['name']
        col_type = column['type']
        col_default = column['default']
        col_not_null = column['not_null']

        column_query = f'{col_name} {col_type}'
        column_query += f' DEFAULT {col_default}' if col_default else ''
        column_query += ' NOT NULL' if col_not_null else ''

        column_queries.append(column_query)

    table_query += ',\n'.join(column_queries)
    table_query += ',\n' + ',\n'.join(constraint_queries) if constraint_queries else ''
    table_query += ');\n'
    table_query += ';\n'.join(index_queries) + ';'

    return table_query


def generate_schema(tables):
    root_table_names = get_root_tables(tables)
    processed_tables = []

    for table_name in root_table_names:
        create_table = generate_create_table(table_name, tables)


def main():
    options = parse_cli_args()
    dsn = build_dsn(options)
    connection = psycopg2.connect(dsn)
    cursor = connection.cursor()
    table_names = get_table_names(cursor)
    tables = {table_name: describe_table(table_name, cursor) for table_name in table_names}

    generate_schema(tables)


if __name__ == '__main__':
    main()
