import unittest

from config.ConfigReader import ConfiguresReader
from db.connectors import baseDBConnector as bcon
from db.connectors.SQLite import SQLite
from db.connectors.mongo import MongoConnector
from db.connectors.msaccess import MSAccessConnector
from db.connectors.oracle import OracleConnector
from db.connectors.postgresql import PostgreSQLConnector
from db.connectors.sqlserver import SQLServerConnector


class DBConnectionTest(unittest.TestCase):
    config_folder = r"/home/umut/PycharmProjects/Archiver/config"

    def test_connect_sqlserver(self):
        config_file = f"{self.config_folder}/confs_sqlserver.ini"
        config_object = ConfiguresReader(config_file)
        section = config_object.read_section('db')
        self._notempty_ini(section, 'rdbms')
        username, password, dbname, port, ip, path, version, sde_engine = config_object.format_for_dbconnector()
        SQLServerConnector(username, password, dbname, port, ip, version=version, sde_engine=sde_engine, start=True)

    def test_connect_oracle(self):
        config_file = f"{self.config_folder}/confs_oracle.ini"
        config_object = ConfiguresReader(config_file)
        section = config_object.read_section('db')
        self._notempty_ini(section, 'rdbms')
        username, password, dbname, port, ip, path, version, sde_engine = config_object.format_for_dbconnector()
        OracleConnector(username, password, dbname, port, ip, version=version, sde_engine=sde_engine)

    def test_connect_postgres(self):
        config_file = f"{self.config_folder}/confs_postgres.ini"
        config_object = ConfiguresReader(config_file)
        section = config_object.read_section('db')
        self._notempty_ini(section, 'rdbms')
        username, password, dbname, port, ip, path, version, sde_engine = config_object.format_for_dbconnector()
        PostgreSQLConnector(username, password, dbname, port, ip, version=version, sde_engine=sde_engine)

    def test_connect_mongo(self):
        config_file = f"{self.config_folder}/confs_mongo.ini"
        config_object = ConfiguresReader(config_file)
        section = config_object.read_section('db')
        self._notempty_ini(section, 'nosql')
        username, password, dbname, port, ip, path, version, sde_engine = config_object.format_for_dbconnector()
        MongoConnector(username, password, dbname, port, ip)

    def test_connect_sqlite(self):
        config_file = f"{self.config_folder}/confs_sqlite.ini"
        config_object = ConfiguresReader(config_file)
        section = config_object.read_section('db')
        self._notempty_ini(section, 'filebase')
        username, password, dbname, port, ip, path, version, sde_engine = config_object.format_for_dbconnector()
        SQLite(path=path)

    def test_connect_access(self):
        config_file = f"{self.config_folder}/confs_sqlite.ini"
        config_object = ConfiguresReader(config_file)
        section = config_object.read_section('db')
        self._notempty_ini(section, 'filebase')
        username, password, dbname, port, ip, path, version, sde_engine = config_object.format_for_dbconnector()
        MSAccessConnector(path=path, version=version, sde_engine=sde_engine)

    @staticmethod
    def _notempty_ini(section, tip):
        if tip == 'rdbms':
            required_arguments_for_rdbms = bcon.required_arguments_for_rdbms
            for key in required_arguments_for_rdbms:
                if list(section.keys()).count(key) == 0:
                    raise Exception(f'There is no {key} in config keys !')
        elif tip == 'nosql':
            required_arguments_for_nosql = bcon.required_arguments_for_nosql
            for key in required_arguments_for_nosql:
                if list(section.keys()).count(key) == 0:
                    raise Exception(f'There is no {key} in config keys !')
        elif tip == 'filebase':
            required_arguments_for_nosql = bcon.required_arguments_for_filebase
            for key in required_arguments_for_nosql:
                if list(section.keys()).count(key) == 0:
                    raise Exception(f'There is no {key} in config keys !')

        else:
            raise Exception('There is no test for another db type?')


class DBCreateDBTest(unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()
