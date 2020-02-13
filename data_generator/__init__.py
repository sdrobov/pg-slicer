import json
from typing import Optional, List

from psycopg2.extensions import cursor as _cursor

from options import Options
from schema_generator import SchemaGenerator, Table, Relation, Column


class DataGenerator:
    def __init__(self, cursor: _cursor, schema: SchemaGenerator, options: Options):
        self.cursor = cursor
        self.schema = schema
        self.options = options
        self.hashes = {}

    def generate_copy(self, table: Table, condition: str = None) -> None:
        if table.name not in self.hashes.keys():
            self.hashes[table.name] = {}

        if table.name in self.options.custom_conditions.keys():
            condition = self.options.custom_conditions[table.name]

        if table.name in self.options.custom_limits.keys():
            limit = self.options.custom_limits[table.name]
        elif table.name in self.options.dump_full:
            limit = None
        else:
            limit = self.options.limit

        query = f'SELECT * FROM {table.name}'
        query += f' WHERE {condition}' if condition else ''
        query += f' ORDER BY 1 DESC'
        query += f' LIMIT {limit}' if limit else ''
        self.cursor.execute(query)

        for row in self.cursor:
            if row[0] in self.hashes[table.name].keys():
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

            self.hashes[table.name][row[0]] = line

    def prepare_condition(self, table: Table, relation: Relation) -> str:
        values = []

        if relation.table_name not in self.hashes:
            return ''

        if not relation.src or not relation.dest:
            return ''

        rel_column = self.get_column_at(table, relation.src)
        if not rel_column or not rel_column.not_null:
            return ''

        for row in self.hashes[relation.table_name].values():
            value = row[relation.dest - 1]
            if type(value) is not int:
                values.append(f'\'{value}\'')
            else:
                values.append(value)

        return self.generate_condition(rel_column, values)

    @staticmethod
    def generate_condition(column: Column, values: list = None) -> str:
        null_part = f' {column.name} IS NULL' if not column.not_null else ''
        cond_part = f' {column.name} IN (%s)' % ','.join(values) if values else ''

        if cond_part and null_part:
            cond_part += ' OR'

        return cond_part + null_part

    @staticmethod
    def get_column_at(table: Table, pos: int) -> Optional[Column]:
        for column in table.columns:
            if column.position == pos:
                return column

        return None

    def generate_copy_recursive(self, table_name: str) -> None:
        table = self.schema.get_table(table_name)
        conditions = []

        for relation in table.relations:
            if relation.is_child():
                continue

            rel_column = self.get_column_at(table, relation.src)
            if not rel_column or not rel_column.not_null:
                continue

            condition = self.prepare_condition(table, relation)
            if condition:
                conditions.append(f'({condition})')

        condition = ' OR '.join(conditions) if len(conditions) > 0 else None
        self.generate_copy(table, condition)

    def generate_data(self) -> str:
        data = ''

        table_layers = [[table for table in self.schema.get_root_tables()]]
        processed_tables = table_layers[0]
        while len(processed_tables) < len(self.schema.tables):
            new_layer = []
            new_layer_names = []

            for table in self.schema.tables:
                if table.name in processed_tables:
                    continue

                add_to_new_layer = True

                for relation in table.relations:
                    if relation.is_child():
                        continue

                    if relation.table_name in processed_tables:
                        continue

                    rel_column = self.get_column_at(table, relation.src)
                    if not rel_column or not rel_column.not_null:
                        continue

                    if relation.table_name in new_layer_names \
                            or relation.table_name not in processed_tables:
                        add_to_new_layer = False

                        break

                if add_to_new_layer:
                    new_layer.append(table.name)
                    processed_tables.append(table.name)

            if new_layer:
                table_layers.append(new_layer)
            else:
                break

        for table_layer in table_layers:
            for table in table_layer:
                self.generate_copy_recursive(table)

        for table_name in self.hashes.keys():
            data += f'COPY {table_name} FROM stdin;\n'
            for rows, row_data in self.hashes[table_name].items():
                data += '\t'.join(row_data) + '\n'
            data += '\\.\n\n'

        return data
