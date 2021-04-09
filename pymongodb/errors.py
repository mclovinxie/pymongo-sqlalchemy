# !/usr/bin python
# -*- coding: utf-8 -*-
# Copyright 2021.
# Author: mclovinxie <mclovin.xxh@gmail.com>
# Created on 2021/3/24


from __future__ import absolute_import


class Error(Exception):
    def __init__(self, msg):
        super(Error, self).__init__(msg)
        self.msg = msg
