# -*- coding: utf8 -*-

"""
This is the main program

# Author, Formatter : Umut Ucok
"""

from pynput.keyboard import Key, Listener
from trackers.CopyManager import CopyManager
from trackers.KeyboardTracker import KeyboardTrackManager
from gui.popup import RecordWindow
from sysinfos.shortcutKeys import Shortcuts  # it validates the supported os also
from db.ArchiverDB import ArchiveDBConnection

import threading


class Main:
    archiver_db = ArchiveDBConnection()

    def __init__(self):
        self.keyboard_thread = threading.Thread(name="keyboard", target=self.create_keyboard_tracker)
        self.keyboard_thread.start()

    def create_keyboard_tracker(self):
        ktrack = KeyboardTrackManager()
        ktrack._dbengine = self.archiver_db


if __name__ == '__main__':
    m = Main()
    print(m.keyboard_thread)