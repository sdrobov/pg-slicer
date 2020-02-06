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
                   'FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND '
                   'd.adnum = a.attnum AND a.atthasdef), a.attnotnull, a.attnum, '
                   '(SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t '
                   'WHERE c.oid = a.attcollation AND t.oid = a.atttypid '
                   'AND a.attcollation <> t.typcollation) AS attcollation, '
                   '\'\'::pg_catalog.char AS attidentity, NULL AS indexdef, NULL AS attfdwoptions, '
                   'a.attstorage, CASE WHEN a.attstattarget=-1 THEN NULL ELSE a.attstattarget END '
                   'AS attstattarget, pg_catalog.col_description(a.attrelid, a.attnum) '
                   'FROM pg_catalog.pg_attribute a '
                   'WHERE a.attrelid = %s AND a.attnum > 0 AND NOT a.attisdropped '
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

    table_description = {
        'columns': [{
            'name': column[0],
            'type': column[1],
            'default': column[2],
            'not_null': column[3],
            'position': column[4],
            'comment': column[11],
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
            'src': None,
            'dest': None,
        } for relation in relations],
    }

    for relation in table_description['relations']:
        if relation['relation'] != 'parent':
            continue

        cursor.execute('SELECT conkey, confkey '
                       'FROM pg_catalog.pg_constraint '
                       'WHERE conname = %s', (relation['fk_name'],))

        row = cursor.fetchone()
        if row:
            [relation['src'], relation['dest']] = [row[0], row[1]]

    return table_description


def get_table_names(cursor):
    cursor.execute('SELECT c.relname '
                   'FROM pg_catalog.pg_class c '
                   'LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace '
                   'WHERE c.relkind IN (\'r\',\'p\',\'\') '
                   'AND n.nspname <> \'pg_catalog\' '
                   'AND n.nspname <> \'information_schema\' '
                   'AND n.nspname !~ \'^pg_toast\' '
                   'AND pg_catalog.pg_table_is_visible(c.oid)')

    return [row[0] for row in cursor.fetchall()]


def get_root_tables(tables):
    root_tables = []

    for table_name, table_data in tables.items():
        root_table = True
        for relation in table_data['relations']:
            if relation['relation'] == 'parent':
                root_table = False

                break

        if root_table and table_name not in root_tables:
            root_tables.append(table_name)

    return root_tables


def generate_create_table(table_name, tables):
    table_query = f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n'

    column_queries = []
    index_queries = [index['create_query'] for index in tables[table_name]['indexes'] if
                     not index['constraint_query']]
    constraint_queries = [index['constraint_query'] for index in tables[table_name]['indexes'] if
                          index['constraint_query']]
    comments = []

    for column in tables[table_name]['columns']:
        col_name = column['name']
        col_type = column['type']
        col_default = column['default']
        col_not_null = column['not_null']
        col_comment = column['comment']

        column_query = f'"{col_name}" {col_type}'
        column_query += f' DEFAULT {col_default}' if col_default else ''
        column_query += ' NOT NULL' if col_not_null else ''

        column_queries.append('\t' + column_query)

        if col_comment:
            comments.append(f'COMMENT ON COLUMN "{table_name}"."{col_name}" IS \'{col_comment}\'')

    table_query += ',\n'.join(column_queries)
    table_query += ',\n' + ',\n'.join(constraint_queries) if constraint_queries else ''
    table_query += '\n);\n'
    table_query += ';\n'.join(index_queries) + ';\n' if index_queries else ''

    for comment in comments:
        table_query += comment + ';\n'

    return table_query + '\n'


def generate_create_table_recursive(table_name, tables, processed_tables):
    table_data = tables[table_name]
    processed_tables.append(table_name)
    schema = ''
    parent_tables = [relation['table'] for relation in table_data['relations']
                     if relation['relation'] == 'parent'
                     and relation['table'] not in processed_tables]

    for parent_table in parent_tables:
        if parent_table in processed_tables:
            continue

        processed_tables.append(parent_table)
        schema += generate_create_table_recursive(parent_table, tables, processed_tables)

    schema += generate_create_table(table_name, tables)

    return schema


def generate_schema(tables, sequences):
    schema = ''
    root_table_names = get_root_tables(tables)
    processed_tables = []

    for sequence_name, sequence_data in sequences.items():
        schema += generate_sequence(sequence_name, sequence_data)

    for table_name in root_table_names:
        schema += generate_create_table(table_name, tables)
        processed_tables.append(table_name)

    while len(processed_tables) < len(tables.items()):
        for table_name, table_data in tables.items():
            if table_name in processed_tables:
                continue

            schema += generate_create_table_recursive(table_name, tables, processed_tables)

    return schema


def generate_sequence(sequence_name, sequence_data):
    schema = ''

    seq_inc = sequence_data['increment_by']
    seq_min = sequence_data['min_value']
    seq_max = sequence_data['max_value']
    seq_start = sequence_data['start_value']

    schema += f'CREATE SEQUENCE IF NOT EXISTS {sequence_name} ' \
              f'INCREMENT {seq_inc} ' \
              f'MINVALUE {seq_min} ' \
              f'MAXVALUE {seq_max} ' \
              f'START {seq_start}'

    if sequence_data['cache_value']:
        schema += ' CACHE ' + str(sequence_data['cache_value'])

    if sequence_data['is_cycled']:
        schema += ' CYCLE'

    schema += ';\n'

    return schema


def get_sequence_names(cursor):
    cursor.execute('SELECT c.relname '
                   'FROM pg_catalog.pg_class c '
                   'LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace '
                   'WHERE c.relkind IN (\'S\',\'\') '
                   'AND n.nspname <> \'pg_catalog\' '
                   'AND n.nspname <> \'information_schema\' '
                   'AND n.nspname !~ \'^pg_toast\' '
                   'AND pg_catalog.pg_table_is_visible(c.oid)')

    return [row[0] for row in cursor.fetchall()]


def describe_sequence(sequence_name, cursor):
    cursor.execute('SELECT start_value, min_value, max_value, increment_by, is_cycled, cache_value '
                   f'FROM {sequence_name}')
    row = cursor.fetchone()

    return {
        'start_value': row[0],
        'min_value': row[1],
        'max_value': row[2],
        'increment_by': row[3],
        'is_cycled': row[4],
        'cache_value': row[5],
    }


def generate_copy(cursor, table_name, limit, hashes, condition=None):
    query = f'SELECT * FROM {table_name}'
    query += f' WHERE {condition}' if condition else ''
    query += f' ORDER BY 1 DESC LIMIT {limit}'
    cursor.execute(query)

    for row in cursor.fetchall():
        row_id = row[0]
        if table_name not in hashes.keys():
            hashes[table_name] = {row_id: ''}
        elif row_id not in hashes[table_name].keys():
            hashes[table_name][row_id] = ''
        else:
            continue

        line = []
        for col in row:
            if col is None:
                line.append('\\N')
            elif type(col) is bool:
                line.append('t' if col else 'f')
            elif type(col) is not str:
                line.append(str(col))
            else:
                line.append(col.replace('\r\n', '\n').replace('\n', '\\r\\n').replace('\t', '\\t'))

        hashes[table_name][row_id] = line


def generate_condition(parent_table_data, tables, hashes):
    condition = []
    parent_table_name = parent_table_data['table']

    if parent_table_name not in hashes:
        return ''

    src = parent_table_data['src']
    dest = parent_table_data['dest']
    if not src or not dest:
        return ''

    col_name = None
    can_be_null = False
    for column in tables[parent_table_name]['columns']:
        if column['position'] == src:
            col_name = column['name']
            can_be_null = not column['not_null']

            break

    if not col_name:
        return ''

    for (row_id, row) in hashes[parent_table_name].keys():
        value = row[dest]
        if type(value) is not int:
            condition.append(f'\'{value}\'')
        else:
            condition.append(value)

    return f'{col_name} IN (%s)' % ','.join(condition)\
           + f' OR {col_name} IS NULL' if can_be_null else ''


def generate_copy_recursive(cursor, table_name, tables, limit, hashes):
    if table_name in hashes:
        return

    table_data = tables[table_name]
    parent_tables = [relation for relation in table_data['relations']
                     if relation['relation'] == 'parent']

    if parent_tables:
        for parent_table_data in parent_tables:
            parent_table_name = parent_table_data['table']
            if parent_table_name == table_name or parent_table_name in hashes:
                continue

            condition = generate_condition(parent_table_data, tables, hashes)
            generate_copy_recursive(cursor, parent_table_name, tables, limit, hashes)
            generate_copy(cursor, table_name, limit, hashes, condition)
    else:
        generate_copy(cursor, table_name, limit, hashes)


def generate_data(cursor, tables, limit):
    # root_table_names = get_root_tables(tables)
    hashes = {}
    data = ''

    # for root_table_name in root_table_names:
    #     generate_copy(cursor, root_table_name, limit, hashes)

    for table_name in tables.keys():
        generate_copy_recursive(cursor, table_name, tables, limit, hashes)

    for table_name in hashes.keys():
        data += f'COPY {table_name} FROM stdin;\n'
        for rows, row_data in hashes[table_name].items():
            data += '\t'.join(row_data) + '\n'
        data += '\\.\n\n'

    return data


def generate_extensions(cursor):
    cursor.execute('SELECT e.extname, n.nspname, c.description '
                   'FROM pg_catalog.pg_extension e '
                   'LEFT JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace '
                   'LEFT JOIN pg_catalog.pg_description c ON c.objoid= e.oid '
                   'AND c.classoid = \'pg_catalog.pg_extension\'::pg_catalog.regclass '
                   'ORDER BY n.nspname')

    extensions = []
    for row in cursor.fetchall():
        ext_name = row[0]
        ext_schema = row[1]
        ext_descr = row[2]
        extensions.append(f'CREATE EXTENSION IF NOT EXISTS "{ext_name}" WITH SCHEMA {ext_schema};\n'
                          f'COMMENT ON EXTENSION "{ext_name}" IS \'{ext_descr}\';\n')

    return '\n'.join(extensions) + '\n\n'


def main():
    options = parse_cli_args()
    dsn = build_dsn(options)
    connection = psycopg2.connect(dsn)
    cursor = connection.cursor()
    table_names = get_table_names(cursor)
    sequence_names = get_sequence_names(cursor)
    tables = {table_name: describe_table(table_name, cursor) for table_name in table_names}
    sequences = {sequence_name: describe_sequence(sequence_name, cursor) for sequence_name in
                 sequence_names}
    extensions = generate_extensions(cursor)
    schema = generate_schema(tables, sequences)
    data = generate_data(cursor, tables, options.limit)

    print(extensions + '\n\n' + schema + '\n\n' + data)


if __name__ == '__main__':
    main()
