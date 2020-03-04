# -*- coding: utf8 -*-
# Author, Formatter : Umut Ucok

__author__ = 'Umut Ucok'
__supported__ = ('ORACLE', 'MSSQLSERVER', 'POSTGRESQL', 'MSACCESS', 'SQLITE', 'MONGO')

import sys

import numpy as np
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import scoped_session, sessionmaker

required_arguments_for_rdbms = ('username', 'password', 'dbname', 'port', 'ip')
required_arguments_for_filebase = ['path']
required_arguments_for_nosql = required_arguments_for_rdbms


class BaseDBConnector:
    """
        Let me explain base and target terms:
        Base database will be used to verify. It seems there is no different choosing base and target. However,
        you may face important case:
        Let's asssume that we had stored data in base db. I said had stored because after exporting target database,
        that data was removed. So if the data still populates in target database. We'll mark this as inserted.

        There is no way to understand that was a row removed in the past or inserted in target. We would have to
        create archiving trigger mechanism.

    """

    def __init__(self, *args, **kwargs):
        self._username, self._password, self._dbname, self._port, self._ip = args
        self._path = kwargs.get('path')

        self.dbengine = None
        self.connection = None
        self.transaction = None

        self.dbsession = None

        self.versioned = None

        if kwargs.get('version') is None:
            self.versioned = False
        else:
            self.versioned = True

        if kwargs.get('sde_engine') is None:
            self.sde_exist = False

        else:
            self.sde_exist = True

        if kwargs.get('start'):
            self.make_engine()

    def _accessLimitations(self, capability):
        if 'ACCESS' in self.__class__.__name__:
            raise NotImplementedError(f"Access için {capability} özelliği eklenmemiştir.")

    def change_version_onsession(self):
        self._accessLimitations(self.change_version_onsession.__name__)

        if self.dbsession is None:
            self.create_session()

        vsql = self.generate_changeversion_sql()
        self.dbsession.execute(vsql)

    def import_config_file(self, path):
        raise NotImplementedError()

    def export_connection_config(self, folder):
        raise NotImplementedError()

    def check_changeversion_sql(self, vsql):
        try:
            self.dbengine.execute(vsql)
            return True
        except Exception:
            e = sys.exc_info()[0]
            return [False, e]

    def create_session(self):
        """
        It creates database session. Why? Because if you use esri registered table, versioned mechanism you need to
        write like CALL version_util.SET_CURRENT_VERSION({}). To do this you need to execute 2 sql queries. One for
        changing version and other one to query your data.

        :return: it fills self.dbsession
        """
        if self.dbengine is None:
            self.make_engine()

        session = scoped_session(sessionmaker(bind=self.dbengine))
        self.dbsession = session()
        self.dbsession.autocommit = True

        self.change_version_onsession()

    def create_engine(self):
        raise NotImplementedError()

    def find_gis_datatype_oftable(self, tablename):
        raise NotImplementedError()

    def generate_changeversion_sql(self):
        raise NotImplementedError()

    def test_connection(self):
        raise NotImplementedError()

    def make_engine(self):
        self.create_engine()

    def create_connection(self):
        self.connection = self.dbengine.connect()

    def create_transaction(self):
        if self.connection is None:
            self.create_connection()

        self.transaction = self.connection.begin()

    def close_connection(self):
        self.connection.close()

    @staticmethod
    def _read_sql_file(sql_file, **kwargs):
        sql_lines = ""
        try:
            with open(sql_file, 'r', encoding='utf-8', errors='strict') as sql_reader:
                sql_lines = sql_reader.read()

        except FileNotFoundError:
            raise FileNotFoundError(f"{sql_file} okunamadi. Lutfen dosyanin varligindan emin olunuz")

        except Exception as err:
            Exception(f"{sql_file} dosyasi okunurken bir hatayla karsilasildi. \n"
                      f"Hata: {err.args}")

        if kwargs.get('uncomment'):
            sql_lines = [i for i in sql_lines.split('\n') if not str(i).startswith('--')]
            sql_lines = "".join(sql_lines)

        return sql_lines

    def execute_sql(self, sqlClause):
        raise NotImplementedError()

    def execute_sqlfile(self, sqlFile):
        raise NotImplementedError()

    def add_users(self, **users):
        """
        {'project' : 'password', 'demo': 'password'}
        :param users:
        :return:
        """
        raise NotImplementedError()

    def delete_users(self, *users):
        raise NotImplementedError()

    def delete_table(self, *tables):
        raise NotImplementedError()

    def alter_password(self, username, newPassword):
        raise NotImplementedError()

    def read_create_table_sql(self):
        pass

    def add_row_df(self, df, tablename, **kwargs):
        kwargs['index'] = False

        try:
            df.to_sql(tablename, self.dbengine, **kwargs)
        except ProgrammingError as prgerr:
            if prgerr.args[0].count('UndefinedColumn') > 0:
                raise BaseDBErrors.ColumnNotExists

    def clone_database(self, sourceDB, targetDBName, **kwargs):
        """
        It is different than dumpDatabase. It clones the database as seperate user which is targetName param
        in the same server.
        :param sourceDB: name of the database you want to be cloned
        :param targetDBName: target database NAME which will be created and data in sourceDB will be imported.
        :param kwargs:
            filter: todo : fill later
        :return:
        """
        raise NotImplementedError()

    def export_shp(self, tablename, path):
        raise NotImplementedError()

    def import_shp(self, path, table):
        raise NotImplementedError()

    def create_table(self, tablename, *columns, **kwargs):
        raise NotImplementedError(f"This feature is not implemented for {self.get_db_brand()}")

    def get_db_brand(self):
        if str(self.dbengine.dbengine.engine.name).lower().count('postgresql'):
            return __supported__[2]
        elif str(self.dbengine.dbengine.engine.name).lower().count('oracle'):
            return __supported__[0]
        elif str(self.dbengine.dbengine.engine.name).lower().count('sqlserver'):
            return __supported__[1]
        elif str(self.dbengine.dbengine.engine.name).lower().count('mongo'):
            return __supported__[-1]
        elif str(self.dbengine.dbengine.engine.name).lower().count('access'):
            raise NotImplementedError('We dont like Access.')

    @staticmethod
    def get_required_arguments_for_rdbms():
        return required_arguments_for_rdbms

    @staticmethod
    def get_required_arguments_for_filebase():
        return required_arguments_for_filebase

    @staticmethod
    def get_required_arguments_for_nosql():
        return required_arguments_for_nosql

    class _KeyArchiverTable:
        tables = {'index', 'KEY', 'DESCRIPTION', 'DATE'}
        types = {np.uint32, np.str, np.str, np.datetime_as_string}


class BaseDBErrors(Exception):
    def __init__(self, msg):
        raise Exception(msg)

    class ColumnNotExists(Exception):
        pass
