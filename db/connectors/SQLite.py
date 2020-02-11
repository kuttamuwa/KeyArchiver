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

    def chech_exist(self):
        return isfile(self._path)

    def create_engine(self):
        if not self.chech_exist():
            # there is no db
            # lets create one
            # check your permission
            if access(self._path, W_OK):
                sqlite_engine = create_engine(f'sqlite:////{self._path}', echo=True)
                try:
                    sqlite_engine.connect()
                    self.dbengine = sqlite_engine
                except OperationalError as err:
                    # somehow it crash?
                    raise BaseDBErrors(
                        "SQLite veritabani baglantisi yapilamadi ! Baglanti bilgilerini kontrol ediniz.\n"
                        f"Hata : {err}")
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

    def create_table(self, tablename, *columns, **kwargs):
        df = pd.DataFrame(data=None, columns=columns)
        df.to_sql(tablename, self.dbengine, **kwargs)
