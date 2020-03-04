import pathlib
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
from db.connectors.postgresql import BaseDBErrors
from db.connectors.postgresql import PostgreSQLConnector
from db.connectors.sqlserver import SQLServerConnector


class ArchiveDBConnection(BaseDBConnector, ABC):
    __tablename__ = 'KEYARCHIVER'
    __columnnames__ = ('KEY', 'DESCRIPTION', 'DATE')
    __config__ = f"{ConfiguresReader.__modulepath__}/confs.ini"
    __db_mechanism__ = None  # it will be filebase or db

    def __init__(self):
        self._configreader = self.config_read()
        dbinfos, _ = self._get_dbinfos_config()

        if isinstance(dbinfos, str) == 1:
            # filebase
            super().__init__(None, None, None, None, None, path=dbinfos)  # uname, pass, dbname, port, ip
        else:
            # db
            super().__init__(*dbinfos)

        self.create_engine()
        self.create_archive_tables()

    def config_read(self):
        if not isfile(self.__config__):
            print(f"config dosyası {self.__config__} pathinde bulunamadı. SQLite kullanılacak.")
            return None
        else:
            return ConfiguresReader(self.__config__)

    @classmethod
    def get_db_mechanism(cls):
        return cls.__db_mechanism__

    @classmethod
    def set_db_mechanism(cls, value):
        if isinstance(value, str):
            cls.__db_mechanism__ = value

    def create_engine(self):
        if self.get_db_mechanism() == 'DB':
            if self.brand not in dbsupporteds:
                raise NotImplementedError(f"Brand not in {dbsupporteds} ! \n"
                                          f"Please set the db brand which we support")
            # ('ORACLE', 'MSSQLSERVER', 'POSTGRESQL', 'MSACCESS')

            if self.brand == 'MSACCESS':
                self.dbengine = MSAccessConnector(self._path, warnings=False)

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

            else:
                raise NotImplementedError("Another DB Brand is not supported yet. Please see what we support: \n"
                                          f"{dbsupporteds}")
        else:
            if self.brand == 'SQLITE':
                self.dbengine = SQLite(self._path, start=True)

            else:
                raise NotImplementedError("Another filebase db is not supported for this version yet ! \n"
                                          "Please try sqlite : )")

    def get_archive_table(self):
        return self.dbengine.dbengine.dialect.has_table(connection=self.dbengine.dbengine,
                                                        table_name=self.__tablename__)

    def create_archive_tables(self):
        sqls = self._get_dbinfos_sql()
        create_sql = self._read_sql_file(sqls.get('create'))

        try:
            self.dbengine.dbengine.execute(create_sql)

        except Exception as err:
            raise Exception("An error occured while creating tables. You may try create them yourself."
                            "Check your sql file in _sqls//archiver_table for your db brand which can be checked"
                            " on confs.ini \n"
                            f"Error: {err.args}")

    def _get_dbinfos_connection_string(self):
        section = self._configreader.read_section('db')

        # it may be filebase db
        if section.get('path') is not None:
            self.set_db_mechanism('FILEBASE')
            self.brand = str(section.get('brand')).upper()
            return section['path']

        else:
            self.set_db_mechanism('DB')

            username = str(section.get('username'))
            password = str(section.get('password'))
            dbname = str(section.get('dbname'))
            port = int(section.get('port'))
            ip = str(section.get('ip')).lower()
            self.brand = str(section.get('brand')).upper()

            return username, password, dbname, port, ip

    def _get_dbinfos_sql(self):
        section = self._configreader.read_section('sql')
        create_table_sql = pathlib.Path(section.get('create_table_sql'))

        return {'create': create_table_sql}

    def _get_dbinfos_config(self):
        if self._configreader is not None:
            return self._get_dbinfos_connection_string(), self._get_dbinfos_sql()
        else:
            # sqlite - automatically will be created
            # todo: must be refactored via function which has to check db was created or not
            return f"{abspath('.')}/keyarchiver.db"

    def recreate_archive_tables(self):
        self.delete_archive_tables()
        self.create_archive_tables()

    def delete_archive_tables(self):
        self.dbengine.delete_table(self.__tablename__)

    def add_row(self, key, value):
        df = pd.DataFrame({
            self.__columnnames__[0]: key,
            self.__columnnames__[1]: value,
            self.__columnnames__[2]: pd.to_datetime('today', errors='coerce')
        }, index=[0])

        try:
            self.dbengine.add_row_df(df, self.__tablename__, if_exists='append', method=None)
        except BaseDBErrors.ColumnNotExists:
            self.recreate_archive_tables()
            self.dbengine.add_row_df(df, self.__tablename__, if_exists='append')


class ArchiveTableNotExists(BaseException):
    pass
