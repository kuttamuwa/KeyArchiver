from abc import ABC
from os.path import isfile, abspath

import pandas as pd

from config.ConfigReader import ConfiguresReader
from db.connectors.SQLite import SQLite
from db.connectors.baseDBConnector import BaseDBConnector
from db.connectors.baseDBConnector import __supported__ as dbsupporteds
from db.connectors.mongo import MongoConnector
from db.connectors.msaccess import MSAccessConnector
from db.connectors.oracle import OracleConnector
from db.connectors.postgresql import PostgreSQLConnector
from db.connectors.sqlserver import SQLServerConnector


class ArchiveDBConnection(BaseDBConnector, ABC):
    __tablename__ = 'KEYARCHIVER'
    __columnnames__ = ('KEY', 'DESCRIPTION', 'DATE')
    __config__ = f"{ConfiguresReader.__modulepath__}/confs.ini"

    def __init__(self):
        self._configreader = self.config_read()
        dbinfos = self._get_dbinfos_config()

        if isinstance(dbinfos, str) == 1:
            super().__init__(None, None, None, None, None, path=dbinfos)  # uname, pass, dbname, port, ip
        else:
            super().__init__(*dbinfos)

        self.create_engine()
        self.create_archive_tables()

    def config_read(self):
        if not isfile(self.__config__):
            print(f"config dosyası {self.__config__} pathinde bulunamadı. SQLite kullanılacak.")
            return None
        else:
            return ConfiguresReader(self.__config__)

    def create_engine(self):
        if self.brand not in dbsupporteds:
            raise NotImplementedError(f"Brand not in {dbsupporteds} ! \n"
                                      f"Please set the db brand which we support")
        # ('ORACLE', 'MSSQLSERVER', 'POSTGRESQL', 'MSACCESS')

        if self.brand == 'MSACCESS':
            # todo : access'e bakıver
            self.dbengine = MSAccessConnector(self._path)

        elif self.brand == 'ORACLE':
            self.dbengine = OracleConnector(self._username, self._password, self._dbname, self._port, self._ip,
                                            start=True)

        elif self.brand == 'MSSQLSERVER':
            self.dbengine = SQLServerConnector(self._username, self._password, self._dbname, self._port, self._ip,
                                               start=True)

        elif self.brand == 'POSTGRESQL':
            self.dbengine = PostgreSQLConnector(self._username, self._password, self._dbname, self._port, self._ip,
                                                start=True)

        elif self.brand == 'MONGO':
            self.dbengine = MongoConnector(self._username, self._password, self._dbname, self._port, self._ip,
                                           start=True)
        elif self.brand == 'SQLITE':
            self.dbengine = SQLite(self._path, start=True)

    def get_archive_table(self):
        return self.dbengine.dbengine.dialect.has_table(connection=self.dbengine.dbengine,
                                                        table_name=self.__tablename__)

    def create_archive_tables(self):
        if not self.get_archive_table():
            self.dbengine.create_table(self.__tablename__, *self.__columnnames__, if_exists='append')

    def _get_dbinfos_config(self):
        if self._configreader is not None:
            section = self._configreader.read_section('db')
            username = str(section.get('username'))
            password = str(section.get('password'))
            dbname = str(section.get('dbname'))
            port = int(section.get('port'))
            ip = str(section.get('ip')).lower()
            self.brand = str(section.get('brand')).upper()

            return username, password, dbname, port, ip
        else:
            # sqlite
            self.brand = 'SQLITE'
            return f"{abspath('.')}/keyarchiver.db"

    def add_row(self, key, value):
        df = pd.DataFrame({
            self.__columnnames__[0]: key,
            self.__columnnames__[1]: value,
            self.__columnnames__[2]: pd.to_datetime('today', errors='coerce')
        }, index=[0])
        df.to_sql(self.__tablename__, self.dbengine.dbengine, if_exists='append', )


class ArchiveTableNotExists(BaseException):
    pass
