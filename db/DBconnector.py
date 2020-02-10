# -*- coding: utf8 -*-
# Author, Formatter : Umut Ucok, Universal

__author__ = 'Umut Ucok'
__supported__ = ('Oracle', 'Microsoft SQL Server', 'PostgreSQL', 'Microsoft Access')


from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import os
import sys
import pyodbc
import cx_Oracle


class DBConnector(object):
    def __init__(self, *args, **kwargs):
        """
        Let me explain base and target terms:
        Base database will be used to verify. It seems there is no different choosing base and target. However,
        you may face important case:
        Let's asssume that we had stored data in base db. I said had stored because after exporting target database,
        that data was removed. So if the data still populates in target database. We'll mark this as inserted.

        There is no way to understand that was a row removed in the past or inserted in target. We would have to
        create archiving trigger mechanism.

        """
        self.username, self.password, self.dbname, self.port, self.ip, self.dbbrand = args

        self.versioned = False
        self.sde_exist = False

        if self.dbbrand == 'ORACLE':
            # we need sid or tns name or service name
            try:
                self.sid = kwargs['sid']
                self.schema_name = kwargs['schema_name']
            except KeyError:
                raise ImportError('You did not specify sid but your database brand is Oracle. You need to write your '
                                  'tns or service name or service id on sid argument')

        try:
            self.version = kwargs['version']
            self.versioned = True

        except KeyError:
            print("You don't want ESRI, all right :)")
            self.versioned = False

        try:
            self.sde_engine = kwargs['sde_engine']
            self.sde_exist = True
        except KeyError:
            print("You dont have sde file. Okay.")
            self.sde_exist = False

        self.dbengine = None
        self.dbsession = None

        self.make_engine()
        # self.create_session()

    def create_pgengine(self):
        """

        :return: it fills dbengine with sql alchemy engine.
        """
        if self.port is None:
            self.port = 5432

        pgengine = create_engine(
            'postgresql://{}:{}@{}:{}/{}'.format(self.username, self.password, self.ip, self.port,
                                                 self.dbname))

        try:
            pgengine.connect()
            self.dbengine = pgengine

        except:
            raise AssertionError(
                "PostgreSQL veritabani baglantisi yapilamadi ! Baglanti bilgilerini kontrol ediniz.")

    def create_msaccess_engine(self):
        """

                :return: it fills dbengine with sql alchemy engine.
        """

        path = self.dbname
        if not os.access(path, os.F_OK):
            Warning('Verilen pathe ulasamadik, ama bir deneyelim. Path: %s' % path)

        try:
            acc_engine = pyodbc.connect('DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={}'.format(path))
            self.dbengine = acc_engine
        except:
            raise AssertionError('Access baglantisi yapilamadi. Verilen path : \n'
                                 '%s . Belirlenen adreste olduguna ve yetkiniz olduguna emin olunuz.' % path)

    def create_oraclengine(self):
        """
        difference between others (mssqlserver, postgre) it reads service name instead of
        dbname.
        :return: sql alchemy database connection engine
        """

        try:
            oraengine = create_engine(
                'oracle://{}:{}@{}:{}/{}'.format(self.username, self.password, self.ip, self.port,
                                                 self.sid))
            oraengine.connect()
            self.dbengine = oraengine

        except:
            print("SID yerine TNS'den gitmeyi deneyecegiz.")
            oraengine = create_engine('oracle+cx_oracle://{}:{}@{}'.format(self.username, self.password, self.sid))

            try:
                oraengine.connect()
                self.dbengine = oraengine

            except:
                print("himm belki de tns'yi bizim olusturmamiz gerekiyor. Deneyelim.")
                try:
                    dsn_tns = cx_Oracle.makedsn(self.ip, self.port, self.sid)
                    con = cx_Oracle.connect(self.username, self.password, dsn_tns)
                    self.dbengine = con
                except Exception:
                    e = sys.exc_info()[1]
                    raise AssertionError(
                        "Maalesef Oracle Veritabanina baglanti yapilamadi. Lutfen baglanti bilgilerini"
                        " kontrol ediniz ve tekrar deneyiniz. Hata : \n"
                        "%s" % e)

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

    def find_gis_datatype_oftable(self, tablename):
        if self.dbbrand == 'ORACLE':
            sql = """SELECT SDE.ST_GeometryType(SHAPE) FROM {} WHERE ROWNUM = 1""".format(tablename)
            st_type = str(self.dbsession.execute(sql).fetchall()[0][
                              0]).upper()[3:]

        elif self.dbbrand == 'SQLSERVER':
            sql = """SELECT SHAPE.STGeometryType() FROM {}""".format(tablename)
            st_type = str(self.dbsession.execute(sql).fetchall()[0][
                              0]).upper()

        elif self.dbbrand == "POSTGRESQL":
            sql = """SELECT ST_GeometryType(shape) FROM {}""".format(tablename)
            st_type = str(self.dbsession.execute(sql).fetchall()[0][
                              0]).upper()

        else:
            raise ValueError('ST Type cozulemedi.')

        return st_type

    def generate_changeversion_sql(self):
        if self.dbbrand == "ORACLE":
            vsql = "CALL version_util.SET_CURRENT_VERSION({})".format(str(self.version))
            if self.check_changeversion_sql(vsql):
                return vsql

            else:
                raise AssertionError('Versiyon degistirme SQL cumlecigimiz dogru calismadi. '
                                     'Dilerseniz bir de siz bakin : %s \n '
                                     'Hata mesaji : %s' % (vsql, self.check_changeversion_sql(vsql)[-1]))

        elif self.dbbrand == "SQLSERVER":
            vsql = "EXEC dbo.SET_CURRENT_VERSION {}".format(self.version)
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
                    raise AssertionError('SQL Server veritabaninda versiyon degistirme komutumuz basariya ugradi. '
                                         'Dilerseniz bir de siz bakin ve komut satiri uzerinden deneyiniz. '
                                         'Version change SQL : %s \n '
                                         'Hata : %s' % ("EXEC sde.SET_CURRENT_VERSION {}".format(self.version),
                                                        self.check_changeversion_sql(vsql)[-1]))

    def check_changeversion_sql(self, vsql):
        try:
            self.dbengine.execute(vsql)
            return True
        except Exception:
            e = sys.exc_info()[0]
            return [False, e]

    def change_version_onsession(self):
        if self.dbsession is None:
            self.create_session()

        vsql = self.generate_changeversion_sql()
        self.dbsession.execute(vsql)

    def create_msengine(self):
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

    def make_engine(self):
        if self.dbbrand.upper() == 'ORACLE':
            self.create_oraclengine()
        elif self.dbbrand.upper() == 'SQLSERVER':
            self.create_msengine()
        elif self.dbbrand.upper() == 'POSTGRESQL':
            self.create_pgengine()
        elif self.dbbrand.upper() == 'MSACCESS':
            self.create_msaccess_engine()
