class BaseCreateDB:
    def __init__(self, dbengine, name, *args, **kwargs):
        self.dbengine = dbengine
        self.name = name

    def create_db(self):
        pass

    def clone_db(self):
        pass


class CreateMongoDB(BaseCreateDB):
    def __init__(self, dbengine, name, *args, **kwargs):
        super().__init__(dbengine, name, *args, **kwargs)


class CreatePostgresDB(BaseCreateDB):
    def __init__(self, dbengine, name, *args, **kwargs):
        super().__init__(dbengine, name, *args, **kwargs)

    def create_db(self):
        conn = self.dbengine.connect()
        conn.execute("commit")
        conn.execute(f"create database {self.name}")


class SQLite(BaseCreateDB):
    def __init__(self, dbengine, name, *args, **kwargs):
        super().__init__(dbengine, name, *args, **kwargs)
