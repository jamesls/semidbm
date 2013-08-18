import os
import mmap
import struct


from semidbm.loaders import DBMLoader, _DELETED
from semidbm.exceptions import DBMLoadError
from semidbm import compat


_MAPPED_LOAD_PAGES = 300


class MMapLoader(DBMLoader):
    def __init__(self):
        pass

    def iter_keys(self, filename):
        # yields keyname, offset, size
        f = compat.file_open(filename, 'rb')
        header = f.read(8)
        self._verify_header(header)
        contents = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        remap_size = mmap.ALLOCATIONGRANULARITY * _MAPPED_LOAD_PAGES
        # We need to track the max_index to use as the upper bound
        # in the .find() calls to be compatible with python 2.6.
        # There's a bug in python 2.6 where if an offset is specified
        # along with a size of 0, then the size for mmap() is the size
        # of the file instead of the size of the file - offset.  To
        # fix this, we track this ourself and make sure we never go passed
        # max_index.  If we don't do this, python2.6 will crash with
        # a bus error (python2.7 works fine without this workaround).
        # See http://bugs.python.org/issue10916 for more info.
        max_index = os.path.getsize(filename)
        file_size_bytes = max_index
        num_resizes = 0
        current = 8
        try:
            while current != max_index:
                try:
                    key_size, val_size = struct.unpack(
                        '!ii', contents[current:current+8])
                except struct.error:
                    raise DBMLoadError()
                key = contents[current+8:current+8+key_size]
                if len(key) != key_size:
                    raise DBMLoadError()
                offset = (remap_size * num_resizes) + current + 8 + key_size
                if offset + val_size > file_size_bytes:
                    # If this happens then the index is telling us
                    # to read past the end of the file.  What we need
                    # to do is stop reading from the index.
                    return
                yield (key, offset, val_size)
                if val_size == _DELETED:
                    val_size = 0
                # Also need to skip past the 4 byte checksum, hence
                # the '+ 4' at the end
                current = current + 8 + key_size + val_size + 4
                if current >= remap_size:
                    contents.close()
                    num_resizes += 1
                    offset = num_resizes * remap_size
                    # Windows python2.6 bug.  You can't specify a length of
                    # 0 with an offset, otherwise you get a WindowsError, not
                    # enough storage is available to process this command.
                    # Couldn't find an issue for this, but the workaround
                    # is to specify the actual length of the mmap'd region
                    # which is the total size minus the offset we want.
                    contents = mmap.mmap(f.fileno(), file_size_bytes - offset,
                                         access=mmap.ACCESS_READ,
                                         offset=offset)
                    current -= remap_size
                    max_index -= remap_size
        finally:
            contents.close()
            f.close()
