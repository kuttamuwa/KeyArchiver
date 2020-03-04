import tkinter as tk

from trackers.MouseTracker import MouseTrackManager


class _BaseWindow(tk.Tk):
    def __init__(self):
        super().__init__()
        self.wm_title("Archiver Pop-Up")
        self.open_with_mouse_position()

    def main_window_settings(self):
        self.bind('<Escape>', self.close)

    def open_with_mouse_position(self):
        mousegeom = MouseTrackManager().get_mouse_position()
        self.geometry(f"+{mousegeom[0]}+{mousegeom[1]}")

    def close_event(self, event):
        self.withdraw()
        self.destroy()


class RecordWindow(tk.Frame):  # , metaclass=MetaSingleton
    state = None

    def __init__(self, **kw):
        self.master = _BaseWindow()
        super().__init__(master=self.master, **kw)

        self.keyword_entry = None
        self.description = None
        self._initsettings()
        self._add_tag_label()
        self._save_tag_button()
        while self.state:
            tk.mainloop()

    def _initsettings(self):
        self.pack(side='top')

    @classmethod
    def state_off(cls):
        cls.state = False

    @classmethod
    def state_on(cls):
        cls.state = True

    def _add_tag_label(self):
        lbl = tk.Label(self, text="Lütfen keyword giriniz: ")
        lbl.grid(row=0)
        self.keyword_entry = tk.Entry(self)
        self.keyword_entry.grid(row=0, column=1)
        self.keyword_entry.bind('<Return>', self._save_keyboard_event)
        self.keyword_entry.bind('<KP_Enter>', self._save_keyboard_event)
        self.keyword_entry.bind('<Escape>', self.close_popup_event)
        self.pack()

    def close_popup(self):
        self.state_off()
        self.master.destroy()

    def close_popup_event(self, e):
        self.close_popup()

    def _save_tag_button(self):
        btn = tk.Button(self.master)
        btn.config(text="SAVE", command=self._save_keyboard_event)
        btn.bind("<Return>", self._save_keyboard_event)

    def _save_keyboard_event(self, e):
        print(self.keyword_entry.get())
        if self.keyword_entry.get() != "":
            self.description = self.keyword_entry.get()
            self.close_popup_event(e)
