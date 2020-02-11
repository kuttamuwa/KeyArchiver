# -*- coding: utf8 -*-

"""
This is the main program

# Author, Formatter : Umut Ucok
"""

import threading
from os.path import abspath

from db.ArchiverDB import ArchiveDBConnection
from logger.logging import LoggingObject
from trackers.KeyboardTracker import KeyboardTrackManager


class Main:
    archiver_db = ArchiveDBConnection()
    logger = LoggingObject('KEYARCHIVER', LoggingObject.get_info_level(), abspath('.'))
    KeyboardTrackManager.logger = logger

    def __init__(self):
        self.keyboard_thread = threading.Thread(name="keyboard", target=self.create_keyboard_tracker)
        self.keyboard_thread.start()
        self.ktrack = None

    def create_keyboard_tracker(self):
        self.ktrack = KeyboardTrackManager()


if __name__ == '__main__':
    m = Main()
    print(m.keyboard_thread)
