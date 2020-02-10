from abc import ABC

from db.connectors.baseDBConnector import BaseDBConnector
from db.connectors.baseDBConnector import __supported__ as dbsupporteds
from db.connectors.msaccess import MSAccessConnector
from db.connectors.mongo import MongoConnector
from db.connectors.oracle import OracleConnector
from db.connectors.postgresql import PostgreSQLConnector
from db.connectors.sqlserver import SQLServerConnector
from config.ConfigReader import ConfiguresReader
import pandas as pd


class ArchiveDBConnection(BaseDBConnector, ABC):
    __tablename__ = 'KEYARCHIVER'
    __columnnames__ = ('KEY', 'DESCRIPTION')
    __config__ = f"{ConfiguresReader.__modulepath__}/confs.ini"

    def __init__(self):
        self._configreader = self.config_read()
        self.dbinfos = self._get_dbinfos_config()
        super().__init__(*self.dbinfos)
        self.create_engine()
        self.create_archive_tables()

    def config_read(self):
        return ConfiguresReader(self.__config__)

    def create_engine(self):
        if self.brand not in dbsupporteds:
            raise NotImplementedError(f"Brand not in {dbsupporteds} ! \n"
                                      f"Please set the db brand which we support")
        # ('ORACLE', 'MSSQLSERVER', 'POSTGRESQL', 'MSACCESS')

        if self.brand == 'MSACCESS':
            # todo : access'e bakıver
            self.dbengine = MSAccessConnector(None)

        elif self.brand == 'ORACLE':
            self.dbengine = OracleConnector(self._username, self._password, self._dbname, self._port, self._ip,
                                            start=False)

        elif self.brand == 'MSSQLSERVER':
            self.dbengine = SQLServerConnector(self._username, self._password, self._dbname, self._port, self._ip,
                                               start=False)

        elif self.brand == 'POSTGRESQL':
            self = PostgreSQLConnector(self._username, self._password, self._dbname, self._port, self._ip,
                                       start=True)

        elif self.brand == 'MONGO':
            self.dbengine = MongoConnector(self._username, self._password, self._dbname, self._port, self._ip,
                                           start=False)

    def get_archive_table(self):
        return self.dbengine.dbengine.dialect.has_table(connection=self.dbengine.dbengine,
                                                        table_name=self.__tablename__)

    def create_archive_tables(self):
        if not self.get_archive_table():
            self.create_table(self.__tablename__, *self.__columnnames__, if_exists='append')

    def _get_dbinfos_config(self):
        section = self._configreader.read_section('db')
        username = str(section.get('username'))
        password = str(section.get('password'))
        dbname = str(section.get('dbname'))
        port = int(section.get('port'))
        ip = str(section.get('ip')).lower()
        self.brand = str(section.get('brand')).upper()

        return username, password, dbname, port, ip


class ArchiveTableNotExists(BaseException):
    pass
