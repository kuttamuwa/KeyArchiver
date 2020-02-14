from abc import ABC

from db.managers.baseDBManager import BaseDBManager


class SQLite(BaseDBManager, ABC):
    def __init__(self, connector):
        super().__init__(connector)
