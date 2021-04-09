# !/usr/bin python
# -*- coding: utf-8 -*-
# Copyright 2021.
# Author: mclovinxie <mclovin.xxh@gmail.com>
# Created on 2021/3/24


from sqlalchemy.engine import default
from sqlalchemy.sql.compiler import *


class MongoIdentifierPreparer(IdentifierPreparer):
    def __init__(self, dialect, initial_quote='', final_quote=None, escape_quote='', omit_schema=False):
        super(MongoIdentifierPreparer, self).__init__(dialect, initial_quote, final_quote, escape_quote, omit_schema)


class MongoDialect(default.DefaultDialect):
    name = 'pymongodb'
    driver = 'pymongodb'
    default_paramstyle = 'pyformat'
    supports_sequences = True
    sequences_optional = True
    supports_native_decimal = True
    supports_default_values = True
    supports_native_boolean = True
    preparer = MongoIdentifierPreparer

    def __init__(self, **kwargs):
        super(MongoDialect, self).__init__(self, **kwargs)

    def initialize(self, connection):
        pass

    @classmethod
    def dbapi(cls):
        return __import__('pymongodb')

    def create_connect_args(self, url):
        opts = url.translate_connect_args()
        args = {
            'username': opts.get('username'),
            'password': opts.get('password', ""),
            'host': opts.get('host', 'localhost'),
            'port': opts.get('port', 0),
            'database': opts.get('database', '')
        }
        args.update(url.query)
        return [], args

    def get_schema_names(self, connection, **kw):
        return connection.connect().connection.connection.list_schemas()

    def get_table_names(self, connection, schema=None, **kw):
        return connection.connect().connection.connection.list_tables(schema)

    def has_table(self, connection, table_name, schema=None):
        return table_name in self.get_table_names(connection, schema)

    def has_sequence(self, connection, sequence_name, schema=None):
        return False

    def get_columns(self, connection, table_name, schema=None, **kw):
        return connection.connect().connection.connection.list_columns(schema, table_name)

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_view_names(self, connection, schema=None, **kw):
        return []

    def get_pk_constraint(self, conn, table_name, schema=None, **kw):
        return {}

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        return []

    def do_ping(self, connection):
        return True


dialect = MongoDialect
