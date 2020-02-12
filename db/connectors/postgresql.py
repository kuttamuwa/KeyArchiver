from abc import ABC

import pandas as pd
from psycopg2 import OperationalError
from sqlalchemy import create_engine
from sqlalchemy import text

from .baseDBConnector import BaseDBConnector, BaseDBErrors


class PostgreSQLConnector(BaseDBConnector, ABC):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.create_engine()

    def create_engine(self):
        """

        :return: it fills dbengine with sql alchemy engine.
        """
        if self._port is None:
            self._port = 5432

        pgengine = create_engine(
            'postgresql://{}:{}@{}:{}/{}'.format(self._username, self._password, self._ip, self._port,
                                                 self._dbname))

        try:
            pgengine.connect()
            self.dbengine = pgengine

        except OperationalError as err:
            raise BaseDBErrors("PostgreSQL veritabani baglantisi yapilamadi ! Baglanti bilgilerini kontrol ediniz.\n"
                               f"Hata : {err}")

        except Exception as err:
            raise Exception("PostgreSQL veritabani baglantisi yapilamadi ! Baglanti bilgilerini kontrol edniz ! \n"
                            f"Hata : {err}")

    def find_gis_datatype_oftable(self, tablename):
        sql = f'SELECT ST_GeometryType(shape) FROM {tablename}'
        st_type = str(self.dbsession.execute(sql).fetchall()[0][
                          0]).upper()

        return st_type

    def create_table(self, tablename, *columns, **kwargs):
        df = pd.DataFrame(data=None, columns=columns)
        df.to_sql(tablename, self.dbengine, **kwargs)

    def delete_table(self, *tables):
        self.create_connection()
        self.create_transaction()

        for table in tables:
            try:
                self.connection.execute(text(f'DROP TABLE IF EXISTS {table} CASCADE '))
                self.transaction.commit()
            except Exception as err:
                self.transaction.rollback()
                print(f"error raised for this table: {table} \n"
                      f"Error : {err}")
