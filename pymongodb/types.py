# !/usr/bin python
# -*- coding: utf-8 -*-
# Copyright 2021.
# Author: mclovinxie <mclovin.xxh@gmail.com>
# Created on 2021/3/24


import bson
import types as tps
from sqlalchemy import types


dataframe_column_types_dict = {
    'b': types.Boolean,
    'i': types.Integer,
    'u': types.Integer,
    'f': types.Float,
    'c': types.Float,
    'm': types.TIMESTAMP,
    'M': types.DateTime,
    'O': types.String,
    'S': types.String,
    'U': types.String,
    'V': types.String
}


data_type_function_dict = {
    'b': bool,
    'i': int,
    'u': int,
    'f': float,
    'c': float,
    'm': str,
    'M': str,
    'O': str,
    'S': str,
    'U': str,
    'V': str
}


def get_data_type(v):
    if isinstance(v, int):
        return 'i'
    elif isinstance(v, str):
        return 'S'
    elif isinstance(v, float):
        return 'f'
    elif isinstance(v, bool):
        return 'b'
    elif isinstance(v, (dict, list, bson.objectid.ObjectId)):
        return 'S'
    return 'S'
