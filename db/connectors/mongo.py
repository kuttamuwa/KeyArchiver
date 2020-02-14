from abc import ABC

from pymongo import MongoClient, errors as mongoerrors

from db.connectors.baseDBConnector import BaseDBConnector


class MongoConnector(BaseDBConnector, ABC):
    def import_config_file(self, path):
        pass

    def export_connection_config(self, folder):
        pass

    def find_gis_datatype_oftable(self, tablename):
        pass

    def generate_changeversion_sql(self):
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

    def create_engine(self):
        dbengine = MongoClient(self._ip, self._port, serverSelectionTimeoutMS=2000)
        try:
            dbengine.server_info()
            self.dbengine = dbengine

        except mongoerrors.ServerSelectionTimeoutError as e:
            raise Exception("MongoDB bağlantısı sağlanamadı. Lütfen hatayı inceleyiniz : \n"
                            "{}".format(e.errors))

    def close_connection(self):
        self.dbengine.close()

#
# if __name__ == '__main__':
#     # self.username, self.password, self.dbname, self.port, self.ip, self.dbbrand = args
#     mc = MongoConnector(None, None, "unit", 27017, "10.0.0.64", start=True)
