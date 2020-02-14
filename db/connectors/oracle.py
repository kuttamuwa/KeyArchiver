# import cx_Oracle
import os
import sys
from abc import ABC

from sqlalchemy import create_engine

from .baseDBConnector import BaseDBConnector


class OracleConnector(BaseDBConnector, ABC):
    def export_connection_config(self, folder):
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # we need sid or tns name or service name
        try:
            self.sid = kwargs['sid']
            self.schema_name = kwargs['schema_name']
        except KeyError:
            raise ImportError('You did not specify sid but your database brand is Oracle. You need to write your '
                              'tns or service name or service id on sid argument')

    def create_engine(self):
        """

        difference between others (mssqlserver, postgre) it reads service name instead of dbname.
        :return:
        """

        try:
            oraengine = create_engine(
                'oracle://{}:{}@{}:{}/{}'.format(self._username, self._password, self._ip, self._port,
                                                 self.sid))
            oraengine.connect()
            self.dbengine = oraengine

        except:
            print("SID yerine TNS'den gitmeyi deneyecegiz.")
            oraengine = create_engine('oracle+cx_oracle://{}:{}@{}'.format(self._username, self._password, self.sid))

            try:
                oraengine.connect()
                self.dbengine = oraengine

            except:
                print("himm belki de tns'yi bizim olusturmamiz gerekiyor. Deneyelim.")
                try:
                    dsn_tns = cx_Oracle.makedsn(self._ip, self._port, self.sid)
                    con = cx_Oracle.connect(self._username, self._password, dsn_tns)
                    self.dbengine = con
                except Exception:
                    e = sys.exc_info()[1]
                    raise AssertionError(
                        "Maalesef Oracle Veritabanina baglanti yapilamadi. Lutfen baglanti bilgilerini"
                        " kontrol ediniz ve tekrar deneyiniz. Hata : \n"
                        "%s" % e)

    def find_gis_datatype_oftable(self, tablename):
        sql = f"SELECT SDE.ST_GeometryType(SHAPE) FROM {tablename} WHERE ROWNUM = 1"
        st_type = str(self.dbsession.execute(sql).fetchall()[0][
                          0]).upper()[3:]

        return st_type

    def generate_changeversion_sql(self):
        vsql = f"CALL version_util.SET_CURRENT_VERSION({self.version})"
        if self.check_changeversion_sql(vsql):
            return vsql

        else:
            raise AssertionError('Versiyon degistirme SQL cumlecigimiz dogru calismadi. '
                                 'Dilerseniz bir de siz bakin : %s \n '
                                 'Hata mesaji : %s' % (vsql, self.check_changeversion_sql(vsql)[-1]))

    def check_tnsnamesora(self):
        # todo : burayı sonra doldur
        pass

    def convert_tnsnamesora_toengine(self, **kwargs):
        pass

    def import_config_file(self, path):
        if not os.access(path, 'R'):
            raise OracleErrors(msg="DOES NOT EXST")

        if not path.endswith(".ora"):
            raise OracleErrors(msg="ORA CANNOT BE FOUND")

        self.check_tnsnamesora()


class OracleErrors(Exception):
    def __init__(self, errcode, **kwargs):
        self.errcode = errcode
