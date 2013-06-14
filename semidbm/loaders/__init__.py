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
