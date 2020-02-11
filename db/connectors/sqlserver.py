from abc import ABC

from sqlalchemy import create_engine

from .baseDBConnector import BaseDBConnector, BaseDBErrors
from .baseDBConnector import OSInfos
import pyodbc
import urllib


class SQLServerConnector(BaseDBConnector, ABC):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.versioned = False
        self.sde_exist = False

    def create_engine(self):
        """
                related with constructors
                :return: sql alchemy database connection engine
        """
        if self._port is None:
            self._port = 1433

        try:
            sqlengine = create_engine("mssql+pyodbc://{}:{}@{}:{}/{}".format(self._username, self._password, self._ip,
                                                                             self._port, self._dbname))


            sqlengine.connect()
            print("SQL Server baglanti basarili")
            self.dbengine = sqlengine

        except pyodbc.InterfaceError as err:
            if err.args.count('driver'):
                print("baglanti basarisiz oldu, driver göstererek ilerleyeceğiz.")
                sqlengine = create_engine("mssql+pyodbc://sa:sa.123@10.0.1.228:1433/test_cbs"
                                          "?driver=ODBC+Driver+17+for+SQL+Server")

                try:
                    sqlengine.connect()
                    print("SQL Server baglantisi basarili ")
                    self.dbengine = sqlengine

                except Exception as e:
                    raise AssertionError("SQL Server veritabanina baglanilamadi. Lutfen baglanti bilgilerini kontrol "
                                         "ediniz. \n Hata : %s" % e)
            else:
                raise BaseDBErrors(f'Unexpected errors : {err}')

    def find_gis_datatype_oftable(self, tablename):
        sql = f"SELECT SHAPE.STGeometryType() FROM {tablename}"
        st_type = str(self.dbsession.execute(sql).fetchall()[0][
                          0]).upper()

        return st_type

    def generate_changeversion_sql(self):
        vsql = f"EXEC dbo.SET_CURRENT_VERSION {self.version}"
        if self.check_changeversion_sql(vsql):
            return vsql

        else:
            print(
                "SQL SERVER veritabaniniz DBO uzerinden degil SDE uzerinden gidiyor sanirim. SDE deneyelim. \n"
                "Az once denedigimiz change version sql cumlecigi : %s" %
                "EXEC dbo.SET_CURRENT_VERSION {}".format(self.version))
            vsql = "EXEC sde.SET_CURRENT_VERSION {}".format(self.version)
            if self.check_changeversion_sql(vsql):
                return vsql

            else:
                raise AssertionError('SQL Server veritabaninda versiyon degistirme komutumuz basarisiz oldu. '
                                     'Dilerseniz bir de siz bakin ve komut satiri uzerinden deneyiniz. '
                                     'Version change SQL : %s \n '
                                     'Hata : %s' % ("EXEC sde.SET_CURRENT_VERSION {}".format(self.version),
                                                    self.check_changeversion_sql(vsql)[-1]))

    @staticmethod
    def get_odbc_driver():
        try:
            with open('/etc/odbcinst.ini') as reader:
                line = reader.readline()
                # todo : fil it

        except:
            pass
