import os
from abc import ABC

import pyodbc

from .baseDBConnector import BaseDBConnector


class MSAccessConnector(BaseDBConnector, ABC):
    def import_config_file(self, path):
        pass

    def export_connection_config(self, folder):
        pass

    def generate_changeversion_sql(self):
        pass

    def test_connection(self):
        pass

    def execute_sql(self, sqlClause):
        pass

    def execute_sqlfile(self, sqlFile):
        pass

    def add_users(self, **users):
        pass

    def delete_users(self, *users):
        pass

    def delete_table(self, *tables):
        pass

    def alter_password(self, username, newPassword):
        pass

    def clone_database(self, sourceDB, targetDBName, **kwargs):
        pass

    def export_shp(self, tablename, path):
        pass

    def import_shp(self, path, table):
        pass

    def create_table(self, tablename, *columns, **kwargs):
        pass

    def __init__(self, path, **kwargs):
        if kwargs.get('warnings') is not False:
            self.give_warnings()

        super().__init__(path=path)

    def create_engine(self):
        """

        :return: it fills dbengine with sql alchemy engine.
        """

        path = self._dbname
        if not os.access(path, os.F_OK):
            Warning('Verilen pathe ulasamadik, ama bir deneyelim. Path: %s' % path)

        try:
            acc_engine = pyodbc.connect('DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={}'.format(path))
            self.dbengine = acc_engine
        except Exception as err:
            raise AssertionError('Access baglantisi yapilamadi. Verilen path : \n'
                                 '%s . Belirlenen adreste olduguna ve yetkiniz olduguna emin olunuz. \n'
                                 'Error : %s' % (path, err))

    def find_gis_datatype_oftable(self, tablename):
        self._accessLimitations(self.find_gis_datatype_oftable.__name__)

    @classmethod
    def give_warnings(cls):
        print('I HATE ACCESS. BUT YOU CAN USE IT WITH MANY LIMITED FUNCTIONALITY ')