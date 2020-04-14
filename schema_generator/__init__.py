from typing import List, Optional, Any

from sqlalchemy import MetaData, Table, ForeignKey, inspect, Index
from sqlalchemy.engine import Engine, reflection, ResultProxy
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql.ddl import CreateTable, CreateIndex, SetTableComment, SetColumnComment


class SchemaGenerator:
    sequences: List[ResultProxy]
    schema: str

    def __init__(self, engine: Engine):
        self.engine = engine
        self.tables = []
        self.sequences = []
        self.schema = ''

    def describe_table(self, table_name: str):
        self.engine.execute('SELECT c.oid FROM pg_catalog.pg_class c '
                            'LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace '
                            'WHERE c.relname OPERATOR(pg_catalog.~) %s '
                            'AND pg_catalog.pg_table_is_visible(c.oid)', (f'^({table_name})$',))
        oid = self.engine.fetchone()[0]

        self.engine.execute('SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), '
                            '(SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128) '
                            'FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid '
                            'AND d.adnum = a.attnum AND a.atthasdef), a.attnotnull, a.attnum, '
                            '(SELECT c.collname FROM pg_catalog.pg_collation c, '
                            'pg_catalog.pg_type t WHERE c.oid = a.attcollation '
                            'AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) '
                            'AS attcollation, \'\'::pg_catalog.char AS attidentity, '
                            'NULL AS indexdef, NULL AS attfdwoptions, a.attstorage, '
                            'CASE WHEN a.attstattarget=-1 THEN NULL ELSE a.attstattarget '
                            'END AS attstattarget, pg_catalog.col_description(a.attrelid, '
                            'a.attnum) FROM pg_catalog.pg_attribute a WHERE a.attrelid = %s '
                            'AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum', (oid,))
        columns = self.engine.fetchall()

        self.engine.execute('SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, '
                            'i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), '
                            'pg_catalog.pg_get_constraintdef(con.oid, true), contype, '
                            'condeferrable, condeferred, i.indisreplident, c2.reltablespace '
                            'FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, '
                            'pg_catalog.pg_index i LEFT JOIN pg_catalog.pg_constraint con '
                            'ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype '
                            'IN (\'p\',\'u\',\'x\'))WHERE c.oid = %s AND c.oid = i.indrelid '
                            'AND i.indexrelid = c2.oid ORDER BY i.indisprimary DESC, '
                            'i.indisunique DESC, c2.relname', (oid,))
        indexes = self.engine.fetchall()

        self.engine.execute('SELECT conname, confrelid::pg_catalog.regclass, '
                            'pg_catalog.pg_get_constraintdef(r.oid, true) as condef, \'parent\' '
                            'FROM pg_catalog.pg_constraint r '
                            'WHERE r.conrelid = %s '
                            'AND r.contype = \'f\' '
                            'ORDER BY 1', (oid,))
        parents = self.engine.fetchall()

        self.engine.execute('SELECT conname, conrelid::pg_catalog.regclass, '
                            'pg_catalog.pg_get_constraintdef(c.oid, true) as condef, \'child\' '
                            'FROM pg_catalog.pg_constraint c '
                            'WHERE c.confrelid = %s '
                            'AND c.contype = \'f\' '
                            'ORDER BY 1', (oid,))
        children = self.engine.fetchall()

        return None

    def get_tables(self):
        self.engine.execute('SELECT c.relname '
                            'FROM pg_catalog.pg_class c '
                            'LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace '
                            'WHERE c.relkind IN (\'r\',\'p\',\'\') '
                            'AND n.nspname <> \'pg_catalog\' '
                            'AND n.nspname <> \'information_schema\' '
                            'AND n.nspname !~ \'^pg_toast\' '
                            'AND pg_catalog.pg_table_is_visible(c.oid)')

        self.tables = [self.describe_table(row[0]) for row in self.engine.fetchall() if row[0]]

    def get_root_tables(self) -> List[str]:
        root_tables = []

        for table in self.tables:
            root_table = True
            for relation in table.relations:
                if relation.is_parent():
                    root_table = False

                    break

            if root_table and table.name not in root_tables:
                root_tables.append(table.name)

        return root_tables

    def generate_create_table(self, table):
        table_query = f'CREATE TABLE IF NOT EXISTS "{table.name}" (\n'

        column_queries = []
        index_queries = [idx.create_query for idx in table.indexes if not idx.constraint_query]
        constraint_queries = [idx.constraint_query for idx in table.indexes if idx.constraint_query]
        comments = []

        for column in table.columns:
            column_query = f'"{column.name}" {column.type}'
            column_query += f' DEFAULT {column.default}' if column.default else ''
            column_query += ' NOT NULL' if not column.is_null else ''

            column_queries.append('\t' + column_query)

            if column.comment:
                comments.append(
                    f'COMMENT ON COLUMN "{table.name}"."{column.name}" IS \'{column.comment}\'')

        table_query += ',\n'.join(column_queries)
        table_query += ',\n' + ',\n'.join(constraint_queries) if constraint_queries else ''
        table_query += '\n);\n'
        table_query += ';\n'.join(index_queries) + ';\n' if index_queries else ''

        for comment in comments:
            table_query += comment + ';\n'

        self.schema += table_query + '\n'

    def generate_create_table_recursive(self, table, processed_tables: list):
        processed_tables.append(table.name)
        parent_tables = [relation.table_name for relation in table.relations
                         if relation.is_parent()
                         and relation.table_name not in processed_tables]

        for parent_table in parent_tables:
            if parent_table in processed_tables:
                continue

            processed_tables.append(parent_table)
            self.generate_create_table_recursive(self.get_table(parent_table), processed_tables)

        self.generate_create_table(table)

    def get_table(self, table_name: str) -> Optional[Table]:
        for table in self.tables:
            if table.name == table_name:
                return table

        return None

    def generate_schema(self):
        self.generate_extensions()
        self.generate_sequences()

        meta = MetaData()
        meta.reflect(bind=self.engine)

        for table in meta.sorted_tables:
            self.schema += str(CreateTable(table).compile(self.engine)) + '\n\n'

            for idx in table.indexes:
                self.schema += str(CreateIndex(idx).compile(self.engine)) + '\n\n'

            for c in table.c:
                if c.comment:
                    self.schema += str(SetColumnComment(c).compile(self.engine)) + '\n\n'

            if table.comment:
                self.schema += str(SetTableComment(table).compile(self.engine)) + '\n\n'

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
