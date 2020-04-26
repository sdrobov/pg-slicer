from typing import List, Dict, Any

from sqlalchemy import Table, MetaData, text
from sqlalchemy.engine import Engine

from options import Options


class DataGenerator:
    engine: Engine
    options: Options
    hashes: Dict[str, Dict[int, List[Any]]]

    def __init__(self, engine: Engine, options: Options):
        self.engine = engine
        self.options = options
        self.hashes = {}

    def select_from(self, table: Table) -> None:
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
        else:
            where = text('1=1')

        if table.name in self.options.custom_limits.keys():
            limit = self.options.custom_limits[table.name]
        elif table.name in self.options.dump_full:
            limit = text('ALL')
        else:
            limit = self.options.limit

        select = table.select(
            whereclause=where,
            limit=limit,
            order_by=text('1 DESC')
        )

        result = self.engine.execute(select).fetchall()

        if len(result):
            print(f'COPY {table.name} FROM stdin;')
            for row in result:
                print('\t'.join([str(val) for val in row]))
            print('\\.\n')

    def generate_data(self):
        meta = MetaData()
        meta.reflect(bind=self.engine)

        for table in meta.sorted_tables:
            self.select_from(table)
