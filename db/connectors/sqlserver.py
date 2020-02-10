from .baseDBConnector import BaseDBConnector
from sqlalchemy import create_engine


class SQLServerConnector(BaseDBConnector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.username, self.password, self.dbname, self.port, self.ip, self.dbbrand = args

        self.versioned = False
        self.sde_exist = False

    def create_engine(self):
        """
                related with constructors
                :return: sql alchemy database connection engine
        """
        if self.port is None:
            self.port = 1433

        try:
            sqlengine = create_engine("mssql+pymssql://{}:{}@{}:{}/{}".format(self.username, self.password, self.ip,
                                                                              self.port, self.dbname))
            sqlengine.connect()
            print("SQL Server baglanti basarili")
            self.dbengine = sqlengine

        except:
            print("baglanti basarisiz oldu, baska bir yontemle deniyoruz..")
            sqlengine = create_engine("mssql+pyodbc://{}:{}@{}:{}/{}".format(self.username, self.password, self.ip,
                                                                             self.port, self.dbname))
            self.dbengine = sqlengine
            try:
                sqlengine.connect()
                print("SQL Server baglantisi basarili ")
                self.dbengine = sqlengine

            except Exception as e:
                raise AssertionError("SQL Server veritabanina baglanilamadi. Lutfen baglanti bilgilerini kontrol "
                                     "ediniz. \n Hata : %s" % e)

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