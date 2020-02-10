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
import threading


class Main:
    def __init__(self):
        self.keyboard_tracker = KeyboardTrackManager()


if __name__ == '__main__':
    m = Main()
    # m.keyboard_tracker.