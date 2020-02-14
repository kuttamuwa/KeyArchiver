# -*- coding: utf8 -*-
# Author, Formatter : Umut Ucok

__author__ = 'Umut Ucok'
__supported__ = ('ORACLE', 'MSSQLSERVER', 'POSTGRESQL', 'MSACCESS', 'SQLITE', 'MONGO')


class BaseDBManager:
    def __init__(self, connector):
        self.connector = connector
