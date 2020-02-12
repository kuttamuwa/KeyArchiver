from pynput.keyboard import Key, Listener

from db.ArchiverDB import ArchiveDBConnection
from gui.popup import RecordWindow
from trackers.CopyManager import CopyManager


class KeyboardTrackManager:  # (Thread)
    shortcut_count_limit = 3
    logger = None

    def __init__(self):
        # super().__init__()
        self.shortcut_key = Key.ctrl  # default
        self.shortcut_counter = 0  # default
        self.popup = None
        self._dbengine = ArchiveDBConnection()

        self.create_listener()

    def create_listener(self):
        with Listener(on_press=self.on_press_event) as listener:
            listener.join()

    def _create_popup(self):
        RecordWindow.state_on()
        self.popup = RecordWindow()
        # RecordWindow.state_off()
        return self.popup.description

    def on_press_event(self, key):
        if key == self.shortcut_key:
            self.shortcut_counter += 1
        else:
            self.shortcut_counter = 0

        self.action()

    def action(self):
        if self.shortcut_counter >= self.shortcut_count_limit:
            description = self._create_popup()
            if description is not None:
                self._dbengine.add_row(CopyManager.get_copied_text(), description)

            self.shortcut_counter = 0
