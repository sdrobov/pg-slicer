from psycopg2.extensions import cursor as _cursor


class SchemaGenerator:
    def __init__(self, cursor: _cursor):
        self.cursor = cursor
        self.tables = {}
        self.sequences = {}
        self.schema = ''

    def describe_table(self, table_name: str):
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

            self.cursor.execute('SELECT conkey, confkey '
                                'FROM pg_catalog.pg_constraint '
                                'WHERE conname = %s', (relation['fk_name'],))

            row = self.cursor.fetchone()
            if row:
                [relation['src'], relation['dest']] = [row[0], row[1]]

        return table_description

    def get_tables(self):
        self.cursor.execute('SELECT c.relname '
                            'FROM pg_catalog.pg_class c '
                            'LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace '
                            'WHERE c.relkind IN (\'r\',\'p\',\'\') '
                            'AND n.nspname <> \'pg_catalog\' '
                            'AND n.nspname <> \'information_schema\' '
                            'AND n.nspname !~ \'^pg_toast\' '
                            'AND pg_catalog.pg_table_is_visible(c.oid)')

        self.tables = {row[0]: self.describe_table(row[0]) for row in self.cursor.fetchall()
                       if row[0]}

    def get_root_tables(self) -> list:
        root_tables = []

        for table_name, table_data in self.tables.items():
            root_table = True
            for relation in table_data['relations']:
                if relation['relation'] == 'parent':
                    root_table = False

                    break

            if root_table and table_name not in root_tables:
                root_tables.append(table_name)

        return root_tables

    def generate_create_table(self, table_name: str):
        table_query = f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n'

        column_queries = []
        index_queries = [index['create_query'] for index in self.tables[table_name]['indexes'] if
                         not index['constraint_query']]
        constraint_queries = [index['constraint_query'] for index in
                              self.tables[table_name]['indexes'] if index['constraint_query']]
        comments = []

        for column in self.tables[table_name]['columns']:
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
                comments.append(
                    f'COMMENT ON COLUMN "{table_name}"."{col_name}" IS \'{col_comment}\'')

        table_query += ',\n'.join(column_queries)
        table_query += ',\n' + ',\n'.join(constraint_queries) if constraint_queries else ''
        table_query += '\n);\n'
        table_query += ';\n'.join(index_queries) + ';\n' if index_queries else ''

        for comment in comments:
            table_query += comment + ';\n'

        self.schema += table_query + '\n'

    def generate_create_table_recursive(self, table_name: str, processed_tables: list):
        table_data = self.tables[table_name]
        processed_tables.append(table_name)
        parent_tables = [relation['table'] for relation in table_data['relations']
                         if relation['relation'] == 'parent'
                         and relation['table'] not in processed_tables]

        for parent_table in parent_tables:
            if parent_table in processed_tables:
                continue

            processed_tables.append(parent_table)
            self.generate_create_table_recursive(parent_table, processed_tables)

        self.generate_create_table(table_name)

    def generate_schema(self):
        self.generate_extensions()
        self.get_sequences()

        for sequence_name, sequence_data in self.sequences.items():
            self.generate_sequence(sequence_name, sequence_data)

        self.get_tables()
        processed_tables = []

        for table_name in self.get_root_tables():
            self.generate_create_table(table_name)
            processed_tables.append(table_name)

        while len(processed_tables) < len(self.tables.items()):
            for table_name, table_data in self.tables.items():
                if table_name in processed_tables:
                    continue

                self.generate_create_table_recursive(table_name, processed_tables)

        return self.schema

    def generate_sequence(self, sequence_name: str, sequence_data: dict):
        seq_inc = sequence_data['increment_by']
        seq_min = sequence_data['min_value']
        seq_start = sequence_data['start_value']

        self.schema += f'CREATE SEQUENCE IF NOT EXISTS {sequence_name} ' \
                       f'INCREMENT {seq_inc} ' \
                       f'MINVALUE {seq_min} ' \
                       f'START {seq_start};\n\n'

    def get_sequences(self):
        self.cursor.execute('SELECT c.relname '
                            'FROM pg_catalog.pg_class c '
                            'LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace '
                            'WHERE c.relkind IN (\'S\',\'\') '
                            'AND n.nspname <> \'pg_catalog\' '
                            'AND n.nspname <> \'information_schema\' '
                            'AND n.nspname !~ \'^pg_toast\' '
                            'AND pg_catalog.pg_table_is_visible(c.oid)')

        self.sequences = {row[0]: self.describe_sequence(row[0]) for row in self.cursor.fetchall()
                          if row[0]}

    def describe_sequence(self, sequence_name: str) -> dict:
        try:
            self.cursor.execute('SELECT currval(%s::regclass)', (sequence_name,))
            row = self.cursor.fetchone()
        except:
            row = [1]

        return {
            'start_value': row[0],
            'min_value': 1,
            'increment_by': 1,
        }

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
