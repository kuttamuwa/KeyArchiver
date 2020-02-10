from .baseDBConnector import BaseDBConnector

from sqlalchemy import create_engine


class PostgreSQLConnector(BaseDBConnector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def create_engine(self):
        """

        :return: it fills dbengine with sql alchemy engine.
        """
        if self.port is None:
            self.port = 5432

        pgengine = create_engine(
            'postgresql://{}:{}@{}:{}/{}'.format(self.__username, self.__password, self.__ip, self.port,
                                                 self.__dbname))

        try:
            pgengine.connect()
            self.dbengine = pgengine

        except:
            raise AssertionError(
                "PostgreSQL veritabani baglantisi yapilamadi ! Baglanti bilgilerini kontrol ediniz.")

    def find_gis_datatype_oftable(self, tablename):
        sql = f'SELECT ST_GeometryType(shape) FROM {tablename}'
        st_type = str(self.dbsession.execute(sql).fetchall()[0][
                          0]).upper()

        return st_type