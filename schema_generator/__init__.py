from typing import List, Optional

from psycopg2.extensions import cursor as _cursor


class Column:
    def __init__(self,
                 name: str,
                 type: str,
                 default: str,
                 not_null: bool,
                 position: int,
                 comment: str):
        self.name = name
        self.type = type
        self.default = default
        self.not_null = not_null
        self.position = position
        self.comment = comment


class Index:
    def __init__(self,
                 name: str,
                 is_primary: bool,
                 is_unique: bool,
                 create_query: str,
                 constraint_query: str):
        self.name = name
        self.is_primary = is_primary
        self.is_unique = is_unique
        self.create_query = create_query
        self.constraint_query = constraint_query


class Relation:
    def __init__(self,
                 table_name: str,
                 fk_name: str,
                 create_query: str,
                 relation: str,
                 src: int = None,
                 dest: int = None):
        self.table_name = table_name
        self.fk_name = fk_name
        self.create_query = create_query
        self.relation = relation
        self.src = src
        self.dest = dest

    def is_parent(self) -> bool:
        return self.relation == 'parent'

    def is_child(self) -> bool:
        return self.relation == 'child'


class Table:
    def __init__(self,
                 name: str,
                 columns: List[Column] = None,
                 indexes: List[Index] = None,
                 relations: List[Relation] = None):
        self.name = name
        self.columns = columns
        self.indexes = indexes
        self.relations = relations


class Sequence:
    def __init__(self,
                 name: str,
                 start_value: int = None,
                 min_value: int = None,
                 increment_by: int = None):
        self.name = name
        self.start_value = start_value
        self.min_value = min_value
        self.increment_by = increment_by


class SchemaGenerator:
    tables: List[Table]
    sequences: List[Sequence]
    schema: str

    def __init__(self, cursor: _cursor):
        self.cursor = cursor
        self.tables = []
        self.sequences = []
        self.schema = ''

    def describe_table(self, table_name: str):
        table = Table(table_name)

        self.cursor.execute('SELECT c.oid FROM pg_catalog.pg_class c '
                            'LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace '
                            'WHERE c.relname OPERATOR(pg_catalog.~) %s '
                            'AND pg_catalog.pg_table_is_visible(c.oid)', (f'^({table_name})$',))
        oid = self.cursor.fetchone()[0]

        self.cursor.execute('SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), '
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
        columns = self.cursor.fetchall()

        self.cursor.execute('SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, '
                            'i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), '
                            'pg_catalog.pg_get_constraintdef(con.oid, true), contype, '
                            'condeferrable, condeferred, i.indisreplident, c2.reltablespace '
                            'FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, '
                            'pg_catalog.pg_index i LEFT JOIN pg_catalog.pg_constraint con '
                            'ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype '
                            'IN (\'p\',\'u\',\'x\'))WHERE c.oid = %s AND c.oid = i.indrelid '
                            'AND i.indexrelid = c2.oid ORDER BY i.indisprimary DESC, '
                            'i.indisunique DESC, c2.relname', (oid,))
        indexes = self.cursor.fetchall()

        self.cursor.execute('SELECT conname, confrelid::pg_catalog.regclass, '
                            'pg_catalog.pg_get_constraintdef(r.oid, true) as condef, \'parent\' '
                            'FROM pg_catalog.pg_constraint r '
                            'WHERE r.conrelid = %s '
                            'AND r.contype = \'f\' '
                            'ORDER BY 1', (oid,))
        parents = self.cursor.fetchall()

        self.cursor.execute('SELECT conname, conrelid::pg_catalog.regclass, '
                            'pg_catalog.pg_get_constraintdef(c.oid, true) as condef, \'child\' '
                            'FROM pg_catalog.pg_constraint c '
                            'WHERE c.confrelid = %s '
                            'AND c.contype = \'f\' '
                            'ORDER BY 1', (oid,))
        children = self.cursor.fetchall()

        relations = parents + children

        table.columns = [Column(column[0], column[1], column[2], column[3], column[4], column[11])
                         for column in columns]
        table.indexes = [Index(index[0], index[1], index[2], index[5], index[6])
                         for index in indexes]
        table.relations = [Relation(relation[1], relation[0], relation[2], relation[3])
                           for relation in relations]

        for relation in table.relations:
            if relation.is_child():
                continue

            self.cursor.execute('SELECT conkey, confkey '
                                'FROM pg_catalog.pg_constraint '
                                'WHERE conname = %s', (relation.fk_name,))

            row = self.cursor.fetchone()
            if row:
                [[relation.src], [relation.dest]] = [row[0], row[1]]

        return table

    def get_tables(self):
        self.cursor.execute('SELECT c.relname '
                            'FROM pg_catalog.pg_class c '
                            'LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace '
                            'WHERE c.relkind IN (\'r\',\'p\',\'\') '
                            'AND n.nspname <> \'pg_catalog\' '
                            'AND n.nspname <> \'information_schema\' '
                            'AND n.nspname !~ \'^pg_toast\' '
                            'AND pg_catalog.pg_table_is_visible(c.oid)')

        self.tables = [self.describe_table(row[0]) for row in self.cursor.fetchall() if row[0]]

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

    def generate_create_table(self, table: Table):
        table_query = f'CREATE TABLE IF NOT EXISTS "{table.name}" (\n'

        column_queries = []
        index_queries = [idx.create_query for idx in table.indexes if not idx.constraint_query]
        constraint_queries = [idx.constraint_query for idx in table.indexes if idx.constraint_query]
        comments = []

        for column in table.columns:
            column_query = f'"{column.name}" {column.type}'
            column_query += f' DEFAULT {column.default}' if column.default else ''
            column_query += ' NOT NULL' if column.not_null else ''

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

    def generate_create_table_recursive(self, table: Table, processed_tables: list):
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
        self.get_sequences()

        for sequence in self.sequences:
            self.generate_sequence(sequence)

        self.get_tables()
        processed_tables = []

        for table_name in self.get_root_tables():
            self.generate_create_table(self.get_table(table_name))
            processed_tables.append(table_name)

        while len(processed_tables) < len(self.tables):
            for table in self.tables:
                if table.name in processed_tables:
                    continue

                self.generate_create_table_recursive(table, processed_tables)

        return self.schema

    def generate_sequence(self, sequence: Sequence):
        self.schema += f'CREATE SEQUENCE IF NOT EXISTS {sequence.name} ' \
                       f'INCREMENT {sequence.increment_by} ' \
                       f'MINVALUE {sequence.min_value} ' \
                       f'START {sequence.start_value};\n\n'

    def get_sequences(self):
        self.cursor.execute('SELECT c.relname '
                            'FROM pg_catalog.pg_class c '
                            'LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace '
                            'WHERE c.relkind IN (\'S\',\'\') '
                            'AND n.nspname <> \'pg_catalog\' '
                            'AND n.nspname <> \'information_schema\' '
                            'AND n.nspname !~ \'^pg_toast\' '
                            'AND pg_catalog.pg_table_is_visible(c.oid)')

        self.sequences = [self.describe_sequence(r[0]) for r in self.cursor.fetchall() if r[0]]

    def describe_sequence(self, sequence_name: str) -> Sequence:
        sequence = Sequence(sequence_name, min_value=1, increment_by=1)
        try:
            self.cursor.execute('SELECT currval(%s::regclass)', (sequence_name,))
            row = self.cursor.fetchone()
        except:
            row = [1]

        sequence.start_value = row[0]

        return sequence

    def generate_extensions(self):
        self.cursor.execute('SELECT e.extname, n.nspname, c.description '
                            'FROM pg_catalog.pg_extension e '
                            'LEFT JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace '
                            'LEFT JOIN pg_catalog.pg_description c ON c.objoid= e.oid '
                            'AND c.classoid = \'pg_catalog.pg_extension\'::pg_catalog.regclass '
                            'ORDER BY n.nspname')

        extensions = []
        for row in self.cursor:
            ext_name = row[0]
            ext_schema = row[1]
            ext_descr = row[2]
            extensions.append(
                f'CREATE EXTENSION IF NOT EXISTS "{ext_name}" WITH SCHEMA {ext_schema};\n'
                f'COMMENT ON EXTENSION "{ext_name}" IS \'{ext_descr}\';\n')

        self.schema += '\n'.join(extensions) + '\n\n'
