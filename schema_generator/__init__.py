import re
from typing import List, Optional, Any

from sqlalchemy import MetaData, Table, inspect, text
from sqlalchemy.engine import Engine, ResultProxy
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql.ddl import CreateTable, CreateIndex, SetTableComment, SetColumnComment
from sqlalchemy_views import CreateView


class SchemaGenerator:
    sequences: List[ResultProxy]
    schema: str

    def __init__(self, engine: Engine):
        self.engine = engine
        self.tables = []
        self.sequences = []
        self.schema = ''

    def generate_schema(self):
        self.generate_extensions()
        self.generate_sequences()

        meta = MetaData()
        meta.reflect(bind=self.engine)
        inspector: Inspector = inspect(self.engine)

        for table in meta.sorted_tables:
            self.schema += str(CreateTable(table).compile(self.engine)) + '\n\n'

            for idx in table.indexes:
                self.schema += str(CreateIndex(idx).compile(self.engine)) + '\n\n'

            for c in table.c:
                if c.comment:
                    self.schema += str(SetColumnComment(c).compile(self.engine)) + '\n\n'

            if table.comment:
                self.schema += str(SetTableComment(table).compile(self.engine)) + '\n\n'

        for view_name in inspector.get_view_names():
            if re.compile(r'^pg.*').match(view_name):
                continue

            view = Table(view_name, MetaData())
            view_def = inspector.get_view_definition(view_name)
            create_view = CreateView(
                view,
                text(view_def),
                or_replace=True
            )

            self.schema += str(create_view.compile(self.engine)) + '\n\n'

        return self.schema

    def generate_sequence(self, sequence):
        self.schema += f'CREATE SEQUENCE IF NOT EXISTS {sequence} ' \
                       'INCREMENT 1 ' \
                       'MINVALUE 1 ' \
                       'START 0;\n\n'

    def generate_sequences(self):
        self.get_sequences()

        for sequence in self.sequences:
            self.generate_sequence(sequence)

    def get_sequences(self):
        result: ResultProxy = self.engine.execute('SELECT c.relname '
                                                  'FROM pg_catalog.pg_class c '
                                                  'LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace '
                                                  'WHERE c.relkind IN (\'S\',\'\') '
                                                  'AND n.nspname <> \'pg_catalog\' '
                                                  'AND n.nspname <> \'information_schema\' '
                                                  'AND n.nspname !~ \'^pg_toast\' '
                                                  'AND pg_catalog.pg_table_is_visible(c.oid)')

        self.sequences = [r[0] for r in result.fetchall() if r[0]]

    def generate_extensions(self):
        rows = self.engine.execute('SELECT e.extname, n.nspname, c.description '
                                   'FROM pg_catalog.pg_extension e '
                                   'LEFT JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace '
                                   'LEFT JOIN pg_catalog.pg_description c ON c.objoid= e.oid '
                                   'AND c.classoid = \'pg_catalog.pg_extension\'::pg_catalog.regclass '
                                   'ORDER BY n.nspname').fetchall()

        extensions = []
        for row in rows:
            ext_name = row[0]
            ext_schema = row[1]
            ext_descr = row[2]
            extensions.append(
                f'CREATE EXTENSION IF NOT EXISTS "{ext_name}" WITH SCHEMA {ext_schema};\n'
                f'COMMENT ON EXTENSION "{ext_name}" IS \'{ext_descr}\';\n')

        self.schema += '\n'.join(extensions) + '\n\n'

    def generate_views(self):
        self.engine.execute("""
SELECT c.relname, pg_catalog.pg_get_viewdef(c.oid, true), c.relkind FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('v', 'm')
  AND n.nspname <> 'pg_catalog'
  AND n.nspname <> 'information_schema'
  AND n.nspname !~ '^pg_toast'
  AND pg_catalog.pg_table_is_visible(c.oid)
  AND c.relname !~ '^pg_';
        """)

        for row in self.engine:
            view_name = row[0]
            view_query = row[1]

            query = '\nCREATE OR REPLACE'
            if row[2] == 'm':
                query += ' MATERIALIZED'
            query += f' VIEW {view_name} AS {view_query};\n\n'

            self.schema += query
