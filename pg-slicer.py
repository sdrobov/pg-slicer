#!/usr/bin/env python3

from urllib.parse import quote_plus

from sqlalchemy import create_engine

from data_generator import DataGenerator
from options import Options
from schema_generator import SchemaGenerator


def build_dsn(options: Options) -> str:
    dsn = 'postgresql://'

    if options.user:
        dsn += options.user

        if options.password:
            dsn += ':' + quote_plus(options.password)

        dsn += '@'

    if options.host:
        dsn += options.host

    if options.port:
        dsn += ':' + str(options.port)

    dsn += '/' + options.DBNAME

    return dsn


def main():
    options = Options()
    options.make()
    dsn = build_dsn(options)
    engine = create_engine(dsn)
    schema_generator = SchemaGenerator(engine)
    schema = schema_generator.generate_schema()
    print(schema)
    # data_generator = DataGenerator(engine, options)
    # data_generator.generate_data()


if __name__ == '__main__':
    main()
