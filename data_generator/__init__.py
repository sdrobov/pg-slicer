import json
from typing import Optional, List, Dict, Any

from sqlalchemy import Table
from sqlalchemy.engine import Engine

from options import Options


class DataGenerator:
    engine: Engine
    options: Options
    hashes: Dict[str, Dict[int, List]]

    def __init__(self, engine: Engine, options: Options):
        self.engine = engine
        self.options = options
        self.hashes = {}

    def do_select_with_condition(self, table: Table, where: str = None) -> None:
        if table.name in self.hashes.keys():
            if table.name in self.options.custom_conditions.keys() \
                    or table.name in self.options.dump_full:
                return

            if table.name in self.options.custom_limits.keys() and \
                    len(self.hashes[table.name].keys()) >= self.options.custom_limits[table.name]:
                return
        else:
            self.hashes[table.name] = {}

        if table.name in self.options.custom_conditions.keys():
            where = self.options.custom_conditions[table.name]

        if not where:
            where = '1=1'

        if table.name in self.options.custom_limits.keys():
            limit = self.options.custom_limits[table.name]
        elif table.name in self.options.dump_full:
            limit = 'ALL'
        else:
            limit = self.options.limit

        query = f'SELECT DISTINCT * FROM {table.name} WHERE {where} ORDER BY 1 DESC LIMIT {limit}'
        self.engine.execute(query)

        for row in self.engine:
            key = hash(frozenset(row))
            if key in self.hashes[table.name].keys():
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

            self.hashes[table.name][key] = line

    def prepare_condition(self, table: Table, relation) -> str:
        values = []

        if relation.table_name not in self.hashes.keys():
            return ''

        if not relation.src or not relation.dest:
            return ''

        rel_column = self.get_column_at(table, relation.src)
        if not rel_column or rel_column.is_null:
            return ''

        for row in self.hashes[relation.table_name].values():
            value = row[relation.dest - 1]
            if type(value) is not int:
                values.append(f'\'{value}\'')
            else:
                values.append(value)

        return self.generate_condition(rel_column, values)

    @staticmethod
    def generate_condition(column, values: list = None) -> str:
        null_part = f' {column.name} IS NULL' if column.is_null else ''
        cond_part = f' {column.name} IN (%s)' % ','.join(values) if values else ''

        if cond_part and null_part:
            cond_part += ' OR'

        return cond_part + null_part

    @staticmethod
    def get_column_at(table: Table, pos: int) -> Optional[Any]:
        for column in table.columns:
            if column.position == pos:
                return column

        return None

    def select_from(self, table_name: str) -> None:
        table = self.schema.get_table(table_name)
        conditions = []

        for relation in table.relations:
            if relation.is_child():
                continue

            rel_column = self.get_column_at(table, relation.src)
            if not rel_column or rel_column.is_null:
                continue

            condition = self.prepare_condition(table, relation)
            if condition:
                conditions.append(f'({condition})')

        condition = ' OR '.join(conditions) if len(conditions) > 0 else None
        self.do_select_with_condition(table, condition)

    def generate_data(self):
        table_layers = [[table for table in self.schema.get_root_tables()]]
        processed_tables = [table for table in self.schema.get_root_tables()]
        while len(processed_tables) < len(self.schema.tables):
            new_layer = []

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
                    if not rel_column or rel_column.is_null:
                        continue

                    if relation.table_name in new_layer \
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
                self.select_from(table)

                if len(self.hashes[table].keys()) > 0:
                    print(f'COPY {table} FROM stdin;')
                    for row in self.hashes[table].values():
                        if row:
                            print('\t'.join(row))
                    print('\\.\n')
