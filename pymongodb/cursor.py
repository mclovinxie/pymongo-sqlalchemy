# !/usr/bin python
# -*- coding: utf-8 -*-
# Copyright 2021.
# Author: mclovinxie <mclovin.xxh@gmail.com>
# Created on 2021/3/24


from __future__ import absolute_import

from ps_parser import PandasSqlParser
import gc

from .types import dataframe_column_types_dict, types


class MongoCursor(object):
    def __init__(self, connection):
        self.connection = connection
        self.rowcount = -1
        self.fetched_rows = 0
        self._context = None
        self.result = None
        self._description = None
        self._closed = False

    def __del__(self):
        self.close()

    @property
    def description(self):
        if not self._description:
            self._description = zip(self.column_names, self.column_typenames)
        return self._description

    def close(self):
        if self._closed:
            return
        del self.result
        gc.collect()
        self._closed = True

    def fetchone(self):
        if self.fetched_rows < self.rowcount:
            row = [v for v in self.result.iloc[self.fetched_rows]]
            self.fetched_rows += 1
            return row
        else:
            return None

    def fetchmany(self, size=None):
        fetched_rows = self.fetched_rows
        self.fetched_rows += size
        rows = [self.fetchone() for _ in range(fetched_rows, self.fetched_rows)]
        return rows
        # return self.result[fetched_rows:self.fetched_rows]

    def fetchall(self):
        rows = []
        while True:
            row = self.fetchone()
            if not row:
                break
            else:
                rows.append(row)
        self.fetched_rows = self.rowcount
        return rows

    def execute(self, operation, parameters={}):
        # sql = operation % parameters
        sql = operation.format(**parameters)
        psp = PandasSqlParser(sql)
        # table_names = set()
        # for db_name, tb_name in psp.source_tables(True):
        #     table_names.add(tb_name)
        context = self.connection.all_tables(psp.source_tables(True))
        self.result = psp.execute(context)
        for df in context.values():
            del df
        gc.collect()
        self.rowcount = len(self.result)
        self.fetched_rows = 0
        # self.result = self.result.values
        return self.rowcount

    def executemany(self, operation, seq_params=[]):
        result = []
        for param in seq_params:
            self.execute(operation, param)
            result.extend(self.result)
        self.result = result
        self.rowcount = len(self.result)
        self.fetched_rows = 0
        return self.rowcount

    @property
    def column_names(self):
        return [str(c) for c in self.result.columns]

    @property
    def column_typenames(self):
        return [str(dataframe_column_types_dict.get(t.kind, types.String)) for t in self.result.dtypes]
