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
        # self.keyboard_thread = threading.Thread(name="keyboard", target=self.create_keyboard_tracker)
        # self.keyboard_thread.start()
        self.ktrack = KeyboardTrackManager()
        # self.create_keyboard_tracker()

    # def create_keyboard_tracker(self):
    #     self.ktrack = KeyboardTrackManager()
    # self.ktrack.setName(KeyboardTrackManager.__name__)


if __name__ == '__main__':
    m = Main()
