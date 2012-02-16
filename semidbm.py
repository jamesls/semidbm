"""An only semi-dumb DBM.

This module is an attempt to do slightly better than the
standard library's dumbdbm.  It keeps a similar design
to dumbdbm while improving and fixing some of dumbdbm's
problems.

"""
import os
import mmap
import __builtin__

_open = __builtin__.open
_DELETED = -1
__version__ = '0.3.0'


class DBMError(Exception):
    pass


class DBMLoadError(DBMError):
    pass


# This basically works by keeping two files, one (the data file) that only
# contains the data of the values associated with keys, and one (the index
# file) which stores the offsets in the data file for the keys.  To add a new
# value to the db, the value is written to the data file and the index file is
# appended with the key name, the offset into the data file where the
# associated data was written, and the size of the value.  The format of the
# index is a very simple format: <size>:<item> where <size> is the length of
# the item, and <item> is either the key, the offset, or the size of the value.
# For example, adding a new entry in the index might look like this:
#
# 3:foo3:1242:12\n
#
# This will be read in as the tuple ('foo', '124', '12') which is interpreted
# as the value for the key 'foo' is located 124 bytes into the data file and is
# 12 bytes long.

class _SemiDBM(object):
    """
    
    :param dbdir: The directory containing the dbm files.  If the directory
        does not exist it will be created.
    :param compact_on_open: If this value is True, the index is compacted
        (if needed) when the db is loaded.

    """
    def __init__(self, dbdir, compact_on_open=True):
        self._dbdir = dbdir
        self._data_filename = os.path.join(dbdir, 'data')
        self._index_filename = os.path.join(dbdir, 'data' + os.extsep + 'idx')
        self._index = None
        self._index_file = None
        self._data_file = None
        self._load_db(compact_on_open)

    def _create_db_dir(self):
        if not os.path.exists(self._dbdir):
            os.makedirs(self._dbdir)

    def _load_db(self, compact_index):
        self._create_db_dir()
        self._index = self._load_index(self._index_filename, compact_index)
        # buffering=0 makes the file objects unbuffered.
        self._index_file = _open(self._index_filename, 'ab', buffering=0)
        self._data_file = _open(self._data_filename, 'ab+', buffering=0)

    def _load_index(self, filename, compact_on_open):
        # This method is only used upon instantiation to populate
        # the in memory index.
        if not os.path.exists(filename):
            return {}
        contents = _open(filename, 'r')
        try:
            return self._load_index_from_fileobj(contents, compact_on_open)
        except ValueError:
            raise DBMLoadError("Bad index file: %s" % filename)

    def _load_index_from_fileobj(self, contents, compact_on_open):
        index = {}
        needs_compaction = False
        for key_name, offset, size in self._read_index(contents):
            size = int(size)
            offset = int(offset)
            if size == _DELETED:
                # This is a deleted item so we need to make sure that this
                # value is not in the index.  We know that the key is already
                # in the index, because a delete is only written to the index
                # if the key already exists in the db.
                del index[key_name]
                needs_compaction = True
            else:
                if key_name in index:
                    needs_compaction = True
                    index[key_name] = (offset, size)
                else:
                    index[key_name] = (offset, size)
        if compact_on_open and needs_compaction:
            self._compact_index(index)
        return index

    def _compact_index(self, index):
        new_index_filename = self._index_filename + os.extsep + 'new'
        # Should probably account for if the file already exists.
        f = _open(new_index_filename, 'w')
        for key in index:
            offset, value_length = index[key]
            f.write('%s:%s%s:%s%s:%s\n' % (
                len(str(key)), key, len(str(offset)), offset,
                len(str(value_length)), value_length))
        f.flush()
        os.fsync(f.fileno())
        f.close()
        os.rename(new_index_filename, self._index_filename)

    def _read_index(self, contents):
        for line in contents:
            start = 0
            items = []
            for _ in xrange(3):
                end = line.find(':', start)
                item_length = int(line[start:end])
                items.append(line[end + 1:end + item_length + 1])
                start = end + item_length + 1
            yield items

    def __getitem__(self, key):
        data_file = self._data_file
        offset, size = self._index[key]
        data_file.seek(offset)
        return data_file.read(size)

    def __setitem__(self, key, value):
        # Write the new data out at the end of the file.
        # Returns the offset of where this data is located
        # in the file.
        _len = len
        _str = str
        data_file = self._data_file
        data_file.write(value)
        offset = data_file.tell() - _len(value)
        value_length = _len(value)
        # Update the index file.
        self._index_file.write('%s:%s%s:%s%s:%s\n' % (
            _len(_str(key)), key, _len(_str(offset)), offset,
            _len(_str(value_length)), value_length))
        # Update the in memory index.
        self._index[key] = (offset, value_length)

    def __contains__(self, key):
        return key in self._index

    def __delitem__(self, key):
        offset = self._index[key][0]
        _len = len
        _str = str
        self._index_file.write('%s:%s%s:%s%s:%s\n' % (
            _len(_str(key)), key, _len(_str(offset)), offset,
            _len(_str(_DELETED)), _DELETED))
        del self._index[key]

    def __iter__(self):
        for key in self._index:
            yield key

    def keys(self):
        """Return all they keys in the db.

        The keys are returned in an arbitrary order.

        """
        return self._index.keys()

    def close(self, compact=False):
        """Close the db.

        The data is synced to disk and the db is closed.
        Once the db has been closed, no further reads or writes
        are allowed.

        :param compact: Indicate whether or not to compact the db
            before closing the db.

        """
        if compact:
            self.compact()
        self.sync()
        self._index_file.close()
        self._data_file.close()

    def sync(self):
        """Sync the db to disk.

        This will flush any of the existing buffers and
        fsync the data to disk.

        You should call this method to guarantee that the data
        is written to disk.  This method is also called whenever
        the dbm is `close()`'d.

        """
        # The files are opened unbuffered so we don't technically
        # need to flush the file objects.
        self._data_file.flush()
        self._index_file.flush()
        os.fsync(self._data_file.fileno())
        os.fsync(self._index_file.fileno())

    def compact(self):
        """Compact the db to reduce space.

        This method will compact the data file and the index file.
        This is needed because of the append only nature of the index
        and data files.  This method will read the index and data file
        and write out smaller but equivalent versions of these files.

        As a general rule of thumb, the more non read updates you do,
        the more space you'll save when you compact.

        Note that by default, the index is always compacted everytime
        the db is opened, but not the data file.

        """
        # Basically, compaction works by opening a new db, writing
        # all the keys from this db to the new db, renaming the
        # new db to the filenames associated with this db, and
        # reopening the files associated with this db.  This
        # implementation can certainly be more efficient, but compaction
        # is really slow anyways.
        new_db = self.__class__(self._data_filename + os.extsep + 'compact')
        for key in self._index:
            new_db[key] = self[key]
        new_db.close()
        os.rename(new_db._index_filename, self._index_filename)
        os.rename(new_db._data_filename, self._data_filename)
        os.rmdir(new_db._dbdir)
        # The index is already compacted so we don't need to compact it.
        self._load_db(compact_index=False)


class _SemiDBMReadOnly(_SemiDBM):
    def __delitem__(self, key):
        self._method_not_allowed('delitem')

    def __setitem__(self, key, value):
        self._method_not_allowed('setitem')

    def sync(self):
        self._method_not_allowed('sync')

    def compact(self):
        self._method_not_allowed('compact')

    def _method_not_allowed(self, method_name):
        raise DBMError("Can't %s: db opened in read only mode." % method_name)

    def close(self, compact=False):
        self._index_file.close()
        self._data_file.close()


class _SemiDBMReadOnlyMMap(_SemiDBMReadOnly):
    def __init__(self, dbdir, compact_on_open=True):
        self._data_map = None
        super(_SemiDBMReadOnlyMMap, self).__init__(dbdir, compact_on_open)

    def _load_db(self, compact_index):
        self._create_db_dir()
        self._index = self._load_index(self._index_filename, compact_index)
        # buffering=0 makes the file objects unbuffered.
        self._index_file = _open(self._index_filename, 'ab', buffering=0)
        self._data_file = _open(self._data_filename, 'ab+', buffering=0)
        if os.path.getsize(self._data_filename) > 0:
            self._data_map = self._mmap_datafile(self._data_file)

    def _mmap_datafile(self, data_file):
        mapped = mmap.mmap(data_file.fileno(), 0, mmap.MAP_SHARED,
                           mmap.PROT_READ)
        return mapped

    def __getitem__(self, key):
        offset, size = self._index[key]
        return self._data_map[offset:offset+size]

    def close(self, compact=False):
        super(_SemiDBMReadOnlyMMap, self).close()
        if self._data_map is not None:
            self._data_map.close()


class _SemiDBMReadWrite(_SemiDBM):
    def _load_db(self, compact_index):
        if not os.path.isfile(self._index_filename):
            raise DBMError("Not a file: %s" % self._index_filename)
        if not os.path.isfile(self._data_filename):
            raise DBMError("Not a file: %s" % self._data_filename)

        super(_SemiDBMReadWrite, self)._load_db(compact_index)


class _SemiDBMNew(_SemiDBM):
    def _load_db(self, compact_index):
        self._create_db_dir()
        self._remove_files_in_dbdir()
        index_file = _open(self._index_filename, 'w')
        data_file = _open(self._data_filename, 'w')
        index_file.close()
        data_file.close()
        super(_SemiDBMNew, self)._load_db(compact_index)

    def _remove_files_in_dbdir(self):
        # We want to create a new DB so we need to remove
        # all of the existing files in the dbdir.
        if os.path.exists(self._data_filename):
            os.remove(self._data_filename)
        if os.path.exists(self._index_filename):
            os.remove(self._index_filename)


def open(filename, flag='r', mode=0666):
    """Open a semidbm database.

    :param filename: The name of the db.  Note that for semidbm,
        this is actually a directory name.  The argument is named
        `filename` to be compatible with the dbm interface.

    :param flag: Specifies how the db should be opened.  `flag` can be any of these values

        +---------+-------------------------------------------+
        | Value   | Meaning                                   |
        +=========+===========================================+
        | ``'r'`` | Open existing database for reading only   |
        |         | (default)                                 |
        +---------+-------------------------------------------+
        | ``'w'`` | Open existing database for reading and    |
        |         | writing                                   |
        +---------+-------------------------------------------+
        | ``'c'`` | Open database for reading and writing,    |
        |         | creating it if it doesn't exist           |
        +---------+-------------------------------------------+
        | ``'n'`` | Always create a new, empty database, open |
        |         | for reading and writing                   |
        +---------+-------------------------------------------+

    :param mode: Not currently used (provided to be compatible with
        the dbm interface).

    """
    if flag == 'r':
        return _SemiDBMReadOnly(filename)
    elif flag == 'c':
        return _SemiDBM(filename)
    elif flag == 'w':
        return _SemiDBMReadWrite(filename)
    elif flag == 'n':
        return _SemiDBMNew(filename)
    else:
        raise ValueError("flag argument must be 'r', 'c', 'w', or 'n'")
