from pynput.keyboard import Key, Listener
from trackers.CopyManager import CopyManager
from gui.popup import RecordWindow
from threading import Thread


class KeyboardTrackManager(Thread):
    shortcut_count_limit = 3

    def __init__(self):
        super().__init__()
        self.shortcut_key = Key.ctrl  # default
        self.shortcut_counter = 0  # default
        self.create_listener()
        self.popup = None
        self._dbengine = None

    def create_listener(self):
        with Listener(on_press=self.on_press_event) as listener:
            listener.join()

    def _create_popup(self):
        self.popup = RecordWindow()

    def on_press_event(self, key):
        if key == self.shortcut_key:
            self.shortcut_counter += 1
        else:
            self.shortcut_counter = 0

        self.action()

    def action(self):
        if self.shortcut_counter >= self.shortcut_count_limit:
            print(CopyManager.get_copied_text())
            self._create_popup()
            self.shortcut_counter = 0