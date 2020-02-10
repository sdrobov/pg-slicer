import json
from typing import Optional

from psycopg2.extensions import cursor as _cursor

from options import Options


class DataGenerator:
    def __init__(self, cursor: _cursor, tables: dict, options: Options):
        self.cursor = cursor
        self.tables = tables
        self.options = options
        self.hashes = {}

    def generate_copy(self, table_name: str, condition: str = None) -> None:
        if table_name not in self.hashes.keys():
            self.hashes[table_name] = {}

        if table_name in self.options.custom_conditions.keys():
            condition = self.options.custom_conditions[table_name]

        if table_name in self.options.custom_limits.keys():
            limit = self.options.custom_limits[table_name]
        elif table_name in self.options.dump_full:
            limit = None
        else:
            limit = self.options.limit

        query = f'SELECT * FROM {table_name}'
        query += f' WHERE {condition}' if condition else ''
        query += f' ORDER BY 1 DESC'
        query += f' LIMIT {limit}' if limit else ''
        self.cursor.execute(query)

        for row in self.cursor:
            if row[0] in self.hashes[table_name].keys():
                continue

            line = []
            for col in row:
                if col is None:
                    line.append('\\N')
                elif type(col) is bool:
                    line.append('t' if col else 'f')
                elif type(col) is dict:
                    line.append(json.dumps(col))
                elif type(col) is not str:
                    line.append(str(col)
                                .replace('\\', '\\\\')
                                .replace('\r\n', '\n')
                                .replace('\n', '\\r\\n')
                                .replace('\t', '\\t'))
                else:
                    line.append(col
                                .replace('\\', '\\\\')
                                .replace('\r\n', '\n')
                                .replace('\n', '\\r\\n')
                                .replace('\t', '\\t'))

            self.hashes[table_name][row[0]] = line

    def prepare_condition(self, parent_table_data: dict) -> str:
        values = []
        parent_table_name = parent_table_data['table']

        if parent_table_name not in self.hashes:
            return ''

        src = parent_table_data['src']
        dest = parent_table_data['dest']
        if not src or not dest:
            return ''

        (col_name, can_be_null) = self.get_key_column(src, self.tables[parent_table_name])
        if not col_name:
            return ''

        for row in self.hashes[parent_table_name].values():
            value = row[dest]
            if type(value) is not int:
                values.append(f'\'{value}\'')
            else:
                values.append(value)

        return self.generate_condition(col_name, values, can_be_null)

    @staticmethod
    def generate_condition(col_name: str, values: list = None, can_be_null: bool = False) -> str:
        null_part = f' {col_name} IS NULL' if can_be_null else ''
        cond_part = f' {col_name} IN (%s)' % ','.join(values) if values else ''

        if cond_part and null_part:
            cond_part += ' OR'

        return cond_part + null_part

    @staticmethod
    def get_key_column(pos: int, table_data: dict) -> tuple:
        col_name = None
        can_be_null = False
        for column in table_data['columns']:
            if column['position'] == pos:
                col_name = column['name']
                can_be_null = not column['not_null']

                break

        return col_name, can_be_null

    def generate_copy_recursive(self, table_name: str) -> None:
        if table_name in self.hashes:
            return

        table_data = self.tables[table_name]

        parent_tables = []
        for relation in table_data['relations']:
            if relation['relation'] == 'parent':
                (col_name, can_be_null) = self.get_key_column(relation['src'], table_data)
                if not can_be_null:
                    parent_tables.append(relation)

        conditions = []
        for parent_table_data in parent_tables:
            parent_table_name = parent_table_data['table']
            if parent_table_name != table_name and parent_table_name not in self.hashes.keys():

                self.generate_copy_recursive(parent_table_name)
                condition = self.prepare_condition(parent_table_data)
                if condition:
                    conditions.append(f'({condition})')

        condition = ' OR '.join(conditions) if len(conditions) > 0 else None
        self.generate_copy(table_name, condition)

    def generate_data(self) -> str:
        data = ''

        for table_name in self.tables.keys():
            self.generate_copy_recursive(table_name)

        for table_name in self.hashes.keys():
            data += f'COPY {table_name} FROM stdin;\n'
            for rows, row_data in self.hashes[table_name].items():
                data += '\t'.join(row_data) + '\n'
            data += '\\.\n\n'

        return data
