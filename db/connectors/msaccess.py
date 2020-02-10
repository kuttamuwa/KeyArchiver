from abc import ABC

from .baseDBConnector import BaseDBConnector

import os
import pyodbc


class MSAccessConnector(BaseDBConnector, ABC):
    def __init__(self, path):
        super().__init__(path=path)

    def create_engine(self):
        """

        :return: it fills dbengine with sql alchemy engine.
        """

        path = self._dbname
        if not os.access(path, os.F_OK):
            Warning('Verilen pathe ulasamadik, ama bir deneyelim. Path: %s' % path)

        try:
            acc_engine = pyodbc.connect('DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={}'.format(path))
            self.dbengine = acc_engine
        except:
            raise AssertionError('Access baglantisi yapilamadi. Verilen path : \n'
                                 '%s . Belirlenen adreste olduguna ve yetkiniz olduguna emin olunuz.' % path)

    def find_gis_datatype_oftable(self, tablename):
        self._accessLimitations(self.find_gis_datatype_oftable.__name__)