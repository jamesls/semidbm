import os
import sys
try:
    import __builtin__
except ImportError:
    import builtins as __builtin__

file_open = __builtin__.open


try:
    str_type = unicode
except NameError:
    # Python 3.x.
    str_type = str


DATA_OPEN_FLAGS = os.O_RDWR | os.O_CREAT | os.O_APPEND
if sys.platform.startswith('win'):
    # On windows we need to specify that we should be
    # reading the file as a binary file so it doesn't
    # change any line ending characters.
    DATA_OPEN_FLAGS = DATA_OPEN_FLAGS | os.O_BINARY
