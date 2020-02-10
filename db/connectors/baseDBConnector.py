# -*- coding: utf8 -*-
# Author, Formatter : Umut Ucok

__author__ = 'Umut Ucok'
__supported__ = ('Oracle', 'Microsoft SQL Server', 'PostgreSQL', 'Microsoft Access')

from sqlalchemy.orm import scoped_session, sessionmaker
import sys


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
        self.__username, self.__password, self.__dbname, self.__port, self.__ip = args

        self.dbengine = None
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
        pass

    def export_connection_config(self, folder):
        pass

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
        pass

    def find_gis_datatype_oftable(self, tablename):
        pass

    def generate_changeversion_sql(self):
        pass

    def test_connection(self):
        pass

    def make_engine(self):
        self.create_engine()

    def close_connection(self):
        pass

    def execute_sql(self, sqlClause):
        pass

    def execute_sqlfile(self, sqlFile):
        pass

    def add_users(self, **users):
        """
        {'project' : 'password', 'demo': 'password'}
        :param users:
        :return:
        """
        pass

    def delete_users(self, *users):
        pass

    def alter_password(self, username, newPassword):
        pass

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
        pass

    def export_shp(self, tablename, path):
        pass

    def import_shp(self, path, table):
        pass


class BaseDBErrors(Exception):
    def __init__(self):
        pass
