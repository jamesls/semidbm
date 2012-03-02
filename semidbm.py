"""An only semi-dumb DBM.

This module is an attempt to do slightly better than the
standard library's dumbdbm.  It keeps a similar design
to dumbdbm while improving and fixing some of dumbdbm's
problems.

"""
import os
import sys
import mmap
from binascii import crc32
import __builtin__

__version__ = '0.4.0'
_open = __builtin__.open

_DELETED = -1
_MAPPED_LOAD_PAGES = 300


class DBMError(Exception):
    pass


class DBMLoadError(DBMError):
    pass


class DBMChecksumError(DBMError):
    pass


# The basic idea is to have a single data file which data is only ever
# appended to.  The basic format is:
#
# <size>:key<size>:<checksum><value>
#
# For example, the key value pair "key: value" would be stored as:
#
#     3:key15:0494360628value
#
# When the data file is loaded, an index of key -> (offset, size)
# is created.  So if the above line was the only key in the db, the
# index would be:
#
#     {'key': (8, 15)}
#
# Which says that the value of key is located at offset 8 with a size of
# 15 (this is the value + sizeof checksum). The checksum is always a 10
# digit integer.


class _SemiDBM(object):
    """

    :param dbdir: The directory containing the dbm files.  If the directory
        does not exist it will be created.

    """
    def __init__(self, dbdir, renamer=None, verify_checksums=False):
        if renamer is None:
            self._renamer = _Renamer()
        else:
            self._renamer = renamer
        self._dbdir = dbdir
        self._data_filename = os.path.join(dbdir, 'data')
        # The in memory index, mapping of key to (offset, size).
        self._index = None
        self._data_fd = None
        self._verify_checksums = verify_checksums
        self._current_offset = 0
        self._load_db()

    def _create_db_dir(self):
        if not os.path.exists(self._dbdir):
            os.makedirs(self._dbdir)

    def _load_db(self):
        self._create_db_dir()
        self._index = self._load_index(self._data_filename)
        self._data_fd = os.open(self._data_filename,
                                 os.O_RDWR|os.O_CREAT|os.O_APPEND)
        self._current_offset = os.lseek(self._data_fd, 0, os.SEEK_END)

    def _load_index(self, filename):
        # This method is only used upon instantiation to populate
        # the in memory index.
        if not os.path.exists(filename) or os.path.getsize(filename) == 0:
            return {}
        try:
            return self._load_index_from_fileobj(filename)
        except ValueError, e:
            raise DBMLoadError("Bad index file %s: %s" % (filename, e))

    def _load_index_from_fileobj(self, filename):
        index = {}
        for key_name, offset, size in self._read_index(filename):
            size = int(size)
            offset = int(offset)
            if size == _DELETED:
                # This is a deleted item so we need to make sure that this
                # value is not in the index.  We know that the key is already
                # in the index, because a delete is only written to the index
                # if the key already exists in the db.
                del index[key_name]
            else:
                if key_name in index:
                    index[key_name] = (offset, size)
                else:
                    index[key_name] = (offset, size)
        return index

    def _read_index(self, filename):
        # yields keyname, offset, size
        f = _open(filename, 'r')
        contents = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        remap_size = mmap.ALLOCATIONGRANULARITY * _MAPPED_LOAD_PAGES
        num_resizes = 0
        current = 0
        try:
            while True:
                key_index = contents.find(':', current)
                if key_index == -1:
                    break
                keysize = int(contents[current:key_index])
                key = contents[key_index+1:key_index+1+keysize]
                current = key_index + keysize
                val_index = contents.find(':', current)
                valsize = int(contents[current+1:val_index])
                yield key, (remap_size * num_resizes) + val_index + 1, valsize
                if valsize == _DELETED:
                    valsize = 1
                current = val_index + valsize + 1
                if current >= remap_size:
                    contents.close()
                    num_resizes += 1
                    contents = mmap.mmap(f.fileno(), 0,
                                         access=mmap.ACCESS_READ,
                                         offset=num_resizes * remap_size)
                    current -= remap_size
        finally:
            contents.close()
            f.close()

    def __getitem__(self, key):
        offset, size = self._index[key]
        os.lseek(self._data_fd, offset, os.SEEK_SET)
        checksum_data = os.read(self._data_fd, size)
        if self._verify_checksums:
            return self._verify_checksum_data(checksum_data)
        else:
            return checksum_data[10:]

    def _verify_checksum_data(self, checksum_data):
        checksum = int(checksum_data[:10])
        value = checksum_data[10:]
        if crc32(value) & 0xffffffff != checksum:
            raise DBMChecksumError("Corrupt data detected: invalid checksum.")
        return checksum_data[10:]

    def __setitem__(self, key, value):
        # Write the new data out at the end of the file.
        # Format is
        # <checksum><keysize>:<key><valsize>:<val>
        _len = len
        # Everything except for the actual checksum + value
        pre_value_blob = '%s:%s%s:' % (_len(key), key, _len(value) + 10)
        pre_value_blob_size = _len(pre_value_blob)
        checksum = crc32(value) & 0xffffffff
        blob = '%s%010d%s' % (pre_value_blob, checksum, value)
        os.write(self._data_fd, blob)
        # Update the in memory index.
        self._index[key] = (self._current_offset + pre_value_blob_size,
                            _len(value) + 10)
        self._current_offset += len(blob)

    def __contains__(self, key):
        return key in self._index

    def __delitem__(self, key):
        _len = len
        # When the data blog is _DELETED:
        # this indicates the key was deleted.
        blob = '%s:%s%s:Z' % (_len(key), key, _DELETED)
        os.write(self._data_fd, blob)
        del self._index[key]
        self._current_offset += len(blob)

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
        os.close(self._data_fd)

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
        os.fsync(self._data_fd)

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
        new_db = self.__class__(os.path.join(self._dbdir, 'compact'))
        for key in self._index:
            new_db[key] = self[key]
        new_db.close()
        os.close(self._data_fd)
        self._renamer(new_db._data_filename, self._data_filename)
        os.rmdir(new_db._dbdir)
        # The index is already compacted so we don't need to compact it.
        self._load_db()


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
        os.close(self._data_fd)


class _SemiDBMReadOnlyMMap(_SemiDBMReadOnly):
    def __init__(self, dbdir, **kwargs):
        self._data_map = None
        super(_SemiDBMReadOnlyMMap, self).__init__(dbdir, **kwargs)

    def _load_db(self):
        self._create_db_dir()
        self._index = self._load_index(self._data_filename)
        self._data_fd = os.open(self._data_filename, os.O_RDONLY|os.O_CREAT)
        if os.path.getsize(self._data_filename) > 0:
            self._data_map = mmap.mmap(self._data_fd, 0,
                                       access=mmap.ACCESS_READ)

    def __getitem__(self, key):
        offset, size = self._index[key]
        checksum_data = self._data_map[offset:offset+size]
        return self._verify_checksum_data(checksum_data)

    def close(self, compact=False):
        super(_SemiDBMReadOnlyMMap, self).close()
        if self._data_map is not None:
            self._data_map.close()


class _SemiDBMReadWrite(_SemiDBM):
    def _load_db(self):
        if not os.path.isfile(self._data_filename):
            raise DBMError("Not a file: %s" % self._data_filename)

        super(_SemiDBMReadWrite, self)._load_db()


class _SemiDBMNew(_SemiDBM):
    def _load_db(self):
        self._create_db_dir()
        self._remove_files_in_dbdir()
        super(_SemiDBMNew, self)._load_db()

    def _remove_files_in_dbdir(self):
        # We want to create a new DB so we need to remove
        # any of the existing files in the dbdir.
        if os.path.exists(self._data_filename):
            os.remove(self._data_filename)


# These renamer classes are needed because windows
# doesn't support atomic renames, and I won't want
# non-window clients to suffer for this.  If you're on
# windows, you don't get atomic renames.
class _Renamer(object):
    """An object that can rename files."""
    def __call__(self, from_file, to_file):
        os.rename(from_file, to_file)


# Note that this also works on posix platforms as well.
class _WindowsRenamer(object):
    def __call__(self, from_file, to_file):
        # os.rename(from_, to) will fail is the to file exists,
        # so in order to accommodate this, the to_file is renamed,
        # then from_file -> to_file, and then to_file is removed.
        os.rename(to_file, to_file + os.extsep + 'tmprename')
        os.rename(from_file, to_file)
        os.remove(to_file + os.extsep + 'tmprename')


# The "dbm" interface is:
#
#     open(filename, flag='r', mode=0666)
#
# All the other args after this should have default values
# so that this function remains compatible with the dbm interface.
def open(filename, flag='r', mode=0666, verify_checksums=False):
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
    if sys.platform.startswith('win'):
        renamer = _WindowsRenamer()
    else:
        renamer = _Renamer()
    kwargs = {'renamer': renamer, 'verify_checksums': verify_checksums}
    if flag == 'r':
        return _SemiDBMReadOnly(filename, **kwargs)
    elif flag == 'c':
        return _SemiDBM(filename, **kwargs)
    elif flag == 'w':
        return _SemiDBMReadWrite(filename, **kwargs)
    elif flag == 'n':
        return _SemiDBMNew(filename, **kwargs)
    else:
        raise ValueError("flag argument must be 'r', 'c', 'w', or 'n'")
