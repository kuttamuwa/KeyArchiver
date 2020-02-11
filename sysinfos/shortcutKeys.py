import sys


class Shortcuts:
    opsystem = None
    if 'posix' in sys.builtin_module_names:
        opsystem = '*nix'
    elif 'nt' in sys.builtin_module_names:
        opsystem = 'win'
    else:
        raise SystemError("We don't know this operating system !")
