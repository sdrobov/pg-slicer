import argparse
from os import path, getenv

import yaml

from definitions import CONFIG_PATH


class Options:
    def __init__(self):
        self.host = None
        self.port = None
        self.user = None
        self.password = None
        self.DBNAME = None
        self.limit = None
        self.no_privileges = None
        self.no_publications = None
        self.no_subscriptions = None
        self.dump_full = []
        self.custom_limits = {}
        self.custom_conditions = {}

    def make(self):
        self.parse_cli_args()

        if path.exists(CONFIG_PATH) and path.isfile(CONFIG_PATH):
            self.parse_yaml()

    def parse_yaml(self):
        with open(CONFIG_PATH, 'r') as yml_config:
            try:
                config = yaml.safe_load(yml_config)

                if (not self.host or not self.port or not self.user or not self.password) \
                        and 'connection' in config:
                    connection = config['connection']

                    if not self.host and 'host' in connection and connection['host'] != '~':
                        self.host = connection['host']

                    if not self.port and 'port' in connection and connection['port'] != '~':
                        self.port = connection['port']

                    if not self.user and 'user' in connection and connection['user'] != '~':
                        self.user = connection['user']

                    if not self.password and 'password' in connection \
                            and connection['password'] != '~':
                        self.password = connection['password']

                if 'dump' in config:
                    dump_config = config['dump']

                    if not self.limit and 'limit' in dump_config:
                        self.limit = dump_config['limit']

                    if 'tables' in dump_config:
                        for table_name, table_config in dump_config['tables'].items():
                            if 'limit' in table_config:
                                if table_config['limit'] == '*':
                                    self.dump_full.append(table_name)
                                else:
                                    self.custom_limits[table_name] = table_config['limit']

                            if 'condition' in table_config:
                                self.custom_conditions[table_name] = table_config['condition']
            except yaml.YAMLError:
                return

    def parse_cli_args(self):
        parser = argparse.ArgumentParser(description="PostgreSQL DB data slicer", add_help=False)
        parser.add_argument('-h', '--host', dest='host', default=getenv('PGHOST'))
        parser.add_argument('-p', '--port', type=int, dest='port', default=getenv('PGPORT'))
        parser.add_argument('-U', '--user', dest='user', default=getenv('PGUSER'))
        parser.add_argument('-W', '--password', dest='password')
        parser.add_argument('-l', '--limit', type=int, dest='limit', default=100)
        parser.add_argument('--dump-full', action='append', dest='dump_full')
        parser.add_argument('--no-privileges', action='store_true', dest='no_privileges')
        parser.add_argument('--no-publications', action='store_true', dest='no_publications')
        parser.add_argument('--no-subscriptions', action='store_true', dest='no_subscriptions')
        parser.add_argument('--help', action='help')
        parser.add_argument('DBNAME', default=getenv('PGDATABASE'))

        parser.parse_args(namespace=self)
