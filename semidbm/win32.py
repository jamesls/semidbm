import ctypes
from ctypes.wintypes import LPVOID, DWORD


LPCTSTR = ctypes.c_wchar_p
LPTSTR = LPCTSTR
kernel32 = ctypes.windll.kernel32
kernel32.ReplaceFile.argtypes = [
    LPCTSTR, LPCTSTR, LPCTSTR, DWORD, LPVOID, LPVOID]


def rename(src, dst):
    # Atomic renames in windows!
    # Equivalent to os.rename() in POSIX.
    rc = kernel32.ReplaceFile(LPCTSTR(src), LPCTSTR(dst), None, 0, None, None)
    if rc == 0:
        # While some sort of error is better than nothing,
        # I think there's a way to get a better error message
        # from another win32 call.
        raise OSError("can't rename file, error: %s" % kernel32.GetLastError())
