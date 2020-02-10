from pynput.mouse import Controller


class MouseTrackManager:
    def __init__(self):
        self.mouse = Controller()

    def get_mouse_position(self):
        return self.mouse.position
