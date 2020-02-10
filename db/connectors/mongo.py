from db.connectors.baseDBConnector import BaseDBConnector
from pymongo import MongoClient, errors as mongoerrors


class MongoConnector(BaseDBConnector):
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
