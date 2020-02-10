import tkinter as tk
from trackers.MouseTracker import MouseTrackManager


class _BaseWindow(tk.Tk):
    def __init__(self):
        super().__init__()
        self.wm_title("Archiver Pop-Up")
        self.open_with_mouse_position()

    def open_with_mouse_position(self):
        mousegeom = MouseTrackManager().get_mouse_position()
        self.geometry(f"+{mousegeom[0]}+{mousegeom[1]}")


class RecordWindow(tk.Frame):
    def __init__(self, **kw):
        self.master = _BaseWindow()
        super().__init__(master=self.master, **kw)
        self.state = True

        self.keyword_entry = None
        self._add_tag_label()
        self._save_tag_button()
        while self.state:
            tk.mainloop()

    def _initsettings(self):
        self.pack(side='top')

    def _add_tag_label(self):
        lbl = tk.Label(self, text="Lütfen keyword giriniz: ")
        lbl.grid(row=0)
        self.keyword_entry = tk.Entry(self)
        self.keyword_entry.grid(row=0, column=1)
        self.keyword_entry.bind('<Return>', self._save_keyboard_event)
        self.pack()

    def close_popup(self):
        self.state = False
        self.master.destroy()

    def _save_tag_button(self):
        btn = tk.Button(self.master)
        btn.config(text="SAVE", command=self._save_keyboard_event)
        btn.bind("<Return>", self._save_keyboard_event)

    def _save_keyboard_event(self, event):
        print(self.keyword_entry.get())
        # todo : save to db
        self.close_popup()