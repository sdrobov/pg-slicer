#!/usr/bin/env python3

import psycopg2
from data_generator import DataGenerator
from options import Options
from schema_generator import SchemaGenerator


def build_dsn(options: Options) -> str:
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


def main():
    options = Options()
    options.make()
    dsn = build_dsn(options)
    connection = psycopg2.connect(dsn)
    connection.autocommit = True
    cursor = connection.cursor()
    schema_generator = SchemaGenerator(cursor)
    schema = schema_generator.generate_schema()
    data_generator = DataGenerator(cursor, schema_generator.tables, options)
    data = data_generator.generate_data()

    print(schema + '\n\n' + data)


if __name__ == '__main__':
    main()
