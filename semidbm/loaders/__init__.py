import struct


from semidbm.exceptions import DBMLoadError


# Major, Minor version.
FILE_FORMAT_VERSION = (1, 1)
FILE_IDENTIFIER = b'\x53\x45\x4d\x49'
_DELETED = -1


class DBMLoader(object):
    def __init__(self):
        pass

    def iter_keys(self, filename):
        """Load the keys given a filename.

        Subclasses need to implement this method that accepts a filename and
        iterates over the keys associated with the data file.  Each yielded
        item should contain a tuple of::

            (key_name, offset, size)

        Where key_name is the name of the key (bytes), offset is the integer
        offset within the file of the value associated with the key, and size
        is the size of the value in bytes.
        """
        raise NotImplementedError("iter_keys")

    def _verify_header(self, header):
        sig = header[:4]
        if sig != FILE_IDENTIFIER:
            raise DBMLoadError("File is not a semibdm db file.")
        major, minor = struct.unpack('!HH', header[4:])
        if major != FILE_FORMAT_VERSION[0]:
            raise DBMLoadError(
                'Incompatible file version (got: v%s, can handle: v%s)' % (
                    (major, FILE_FORMAT_VERSION[0])))
