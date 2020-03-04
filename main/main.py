# -*- coding: utf8 -*-

"""
This is the main program

# Author, Formatter : Umut Ucok
"""

from os.path import abspath

from db.ArchiverDB import ArchiveDBConnection
from logger.logging import LoggingObject
from trackers.KeyboardTracker import KeyboardTrackManager


class Main:
    archiver_db = ArchiveDBConnection()
    logger = LoggingObject('KEYARCHIVER', 'INFO', abspath('.'))
    KeyboardTrackManager.logger = logger

    def __init__(self):
        self.ktrack = KeyboardTrackManager()


if __name__ == '__main__':
    m = Main()
