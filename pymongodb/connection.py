from __future__ import absolute_import

import pandas as pd
import json

from io import StringIO

from pymongo import MongoClient

from .log import logger
from .cursor import MongoCursor
from .types import dataframe_column_types_dict, data_type_function_dict, get_data_type


class MongoConnection(object):
    def __init__(self, username=None, password=None, host=None, port=None, database=None, **kwargs):
        self.host = host
        self.database = database
        self.port = port or 8080
        self.username = username
        self.password = password
        self.limit = kwargs['limit'] if 'limit' in kwargs else 100000
        self.client = MongoClient(f'mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}')
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._closed:
            self.close()

    def close(self):
        if not self._closed and self.client:
            self.client.close()
        self._closed = True
        logger.debug('Connection closed.')

    def commit(self):
        logger.debug('Commit.')
        # self.xl.close()

    def rollback(self):
        logger.debug('Rollback.')

    def list_schemas(self):
        return [str(s) for s in self.client.list_database_names()]

    def list_tables(self, schema=None):
        return [str(s) for s in self.client[schema].list_collection_names()]

    def list_columns(self, schema, table_name):
        one_row = self.client[schema][table_name].find_one()
        return [{
            'name': str(k),
            'type': dataframe_column_types_dict[get_data_type(one_row[k])],
            'nullable': False,
            'default': None
        } for k in one_row]

    def cursor(self):
        return MongoCursor(self)

    def _load_df(self, schema_name, table_name):
        json_data = {'columns': [e['name'] for e in self.list_columns(schema_name, table_name)], 'data': []}
        for e in self.client[schema_name][table_name].find().limit(self.limit):
            row_data = []
            for ev in e.values():
                try:
                    row_data.append(data_type_function_dict[get_data_type(ev)](ev))
                except:
                    row_data.append('')
            json_data['data'].append(row_data)
        return pd.read_json(StringIO(json.dumps(json_data)), encoding='utf-8', orient='split')

    def all_tables(self, source_tables=[]):
        table_dict, schema_names = {}, set([db if db else tb.split('.')[0] for db, tb in source_tables])
        for schema_name, table_name in source_tables:
            if not schema_name:
                schema_name, table_name = table_name.split('.')[0], table_name.split('.')[1]
            try:
                table_dict.update({f'{schema_name}.{table_name}': self._load_df(schema_name, table_name)})
            except Exception as e:
                logger.error(e)
        return table_dict


def connect(username=None, password=None, host=None, port=None, database=None, **kwargs):
    return MongoConnection(username, password, host, port, database, **kwargs)
