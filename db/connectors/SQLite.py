from abc import ABC
from os import access, R_OK, W_OK
from os.path import isfile
from sqlite3 import OperationalError

import pandas as pd
from sqlalchemy import create_engine

from .baseDBConnector import BaseDBConnector, BaseDBErrors


class SQLite(BaseDBConnector, ABC):
    def __init__(self, path, **kwargs):
        super().__init__(None, None, None, None, None, path=path, **kwargs)
        self.create_engine()
        self.sqls = kwargs.get('sql')

    def import_config_file(self, path):
        pass

    def read_create_table_sql(self):
        pass

    def export_connection_config(self, folder):
        pass

    def find_gis_datatype_oftable(self, tablename):
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

    def chech_exist(self):
        return isfile(self._path)

    def create_sqlite_engine(self):
        sqlite_engine = create_engine(f'sqlite:////{self._path}', echo=True)
        try:
            sqlite_engine.connect()
            self.dbengine = sqlite_engine
        except OperationalError as err:
            # somehow it crash?
            raise BaseDBErrors(
                "SQLite veritabani baglantisi yapilamadi ! Baglanti bilgilerini kontrol ediniz.\n"
                f"Hata : {err}")

    def create_engine(self):
        if not self.chech_exist():
            # there is no db
            # lets create one
            # check your permission
            if access(self._path, W_OK):
                self.create_sqlite_engine()
            else:
                # if we dont have writing permission
                raise BaseDBErrors(
                    f"There is no permission to create {self._path} sqlite db file. Please check your permission !"
                )
        else:
            # there is a db
            if not access(self._path, R_OK):
                raise BaseDBErrors(
                    f"There is no permission to read {self._path} sqlite db file. Please check your permission !"
                )
            else:
                self.create_sqlite_engine()

    def create_table(self, tablename, *columns, **kwargs):
        df = pd.DataFrame(data=None, columns=columns)
        df.to_sql(tablename, self.dbengine, **kwargs)
