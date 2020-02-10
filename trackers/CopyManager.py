from pyperclip import paste


class CopyManager:
    def __init__(self):
        pass

    @staticmethod
    def get_copied_text():
        return paste()
