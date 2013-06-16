import os
import struct

from semidbm.loaders import DBMLoader, _DELETED


class SimpleFileLoader(DBMLoader):
    def __init__(self):
        pass

    def iter_keys(self, filename):
        # yields keyname, offset, size
        with open(filename, 'rb') as f:
            header = f.read(8)
            self._verify_header(header)
            current_offset = 8
            file_size_bytes = os.path.getsize(filename)
            while True:
                current_contents = f.read(8)
                current_offset += 8
                if len(current_contents) < 8:
                    break
                key_size, val_size = struct.unpack(
                    '!ii', current_contents)
                key = f.read(key_size)
                value_offset = current_offset + key_size
                if value_offset + val_size > file_size_bytes:
                    return
                yield (key, value_offset, val_size)
                if val_size == _DELETED:
                    val_size = 0
                # 4 bytes is for the checksum.
                skip_ahead = key_size + val_size + 4
                current_offset += skip_ahead
                if current_offset > file_size_bytes:
                    return
                f.seek(current_offset)
