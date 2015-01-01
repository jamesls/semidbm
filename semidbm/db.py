"""An only semi-dumb DBM.

This module is an attempt to do slightly better than the
standard library's dumbdbm.  It keeps a similar design
to dumbdbm while improving and fixing some of dumbdbm's
problems.

"""
import os
import sys
from binascii import crc32
import struct

from semidbm.exceptions import DBMLoadError, DBMChecksumError, DBMError
from semidbm.loaders import _DELETED, FILE_FORMAT_VERSION, FILE_IDENTIFIER
from semidbm import compat


_open = compat.file_open


class _SemiDBM(object):
    """

    :param dbdir: The directory containing the dbm files.  If the directory
        does not exist it will be created.

    """
    def __init__(self, dbdir, renamer, data_loader=None,
                 verify_checksums=False):
        self._renamer = renamer
        self._data_loader = data_loader
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
        self._data_fd = os.open(self._data_filename, compat.DATA_OPEN_FLAGS)
        self._current_offset = os.lseek(self._data_fd, 0, os.SEEK_END)

    def _load_index(self, filename):
        # This method is only used upon instantiation to populate
        # the in memory index.
        if not os.path.exists(filename):
            self._write_headers(filename)
            return {}
        try:
            return self._load_index_from_fileobj(filename)
        except ValueError as e:
            raise DBMLoadError("Bad index file %s: %s" % (filename, e))

    def _write_headers(self, filename):
        with _open(filename, 'wb') as f:
            # Magic number identifier.
            f.write(FILE_IDENTIFIER)
            # File version format.
            f.write(struct.pack('!HH', *FILE_FORMAT_VERSION))

    def _load_index_from_fileobj(self, filename):
        index = {}
        for key_name, offset, size in self._data_loader.iter_keys(filename):
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

    def __getitem__(self, key, read=os.read, lseek=os.lseek,
                    seek_set=os.SEEK_SET, str_type=compat.str_type,
                    isinstance=isinstance):
        if isinstance(key, str_type):
            key = key.encode('utf-8')
        offset, size = self._index[key]
        lseek(self._data_fd, offset, seek_set)
        if not self._verify_checksums:
            return read(self._data_fd, size)
        else:
            # Checksum is at the end of the value.
            data = read(self._data_fd, size + 4)
            return self._verify_checksum_data(key, data)

    def _verify_checksum_data(self, key, data):
        # key is the bytes of the key,
        # data is the bytes of the value + 4 byte checksum at the end.
        value = data[:-4]
        expected = struct.unpack('!I', data[-4:])[0]
        actual = crc32(key)
        actual = crc32(value, actual)
        if actual & 0xffffffff != expected:
            raise DBMChecksumError(
                "Corrupt data detected: invalid checksum for key %s" % key)
        return value

    def __setitem__(self, key, value, len=len, crc32=crc32, write=os.write,
                    str_type=compat.str_type, pack=struct.pack,
                    isinstance=isinstance):
        if isinstance(key, str_type):
            key = key.encode('utf-8')
        if isinstance(value, str_type):
            value = value.encode('utf-8')
        # Write the new data out at the end of the file.
        # Format is
        # 4 bytes   4bytes              4bytes
        # <keysize><valsize><key><val><keyvalcksum>
        # Everything except for the actual checksum + value
        key_size = len(key)
        val_size = len(value)
        keyval_size = pack('!ii', key_size, val_size)
        keyval = key + value
        checksum = pack('!I', crc32(keyval) & 0xffffffff)
        blob = keyval_size + keyval + checksum

        write(self._data_fd, blob)
        # Update the in memory index.
        self._index[key] = (self._current_offset + 8 + key_size,
                            val_size)
        self._current_offset += len(blob)

    def __contains__(self, key):
        return key in self._index

    def __delitem__(self, key, len=len, write=os.write, deleted=_DELETED,
                    str_type=compat.str_type, isinstance=isinstance,
                    crc32=crc32, pack=struct.pack):
        if isinstance(key, str_type):
            key = key.encode('utf-8')
        key_size = pack('!ii', len(key), _DELETED)
        crc = pack('!I', crc32(key) & 0xffffffff)
        blob = key_size + key + crc

        write(self._data_fd, blob)
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

    def values(self):
        return [self[key] for key in self._index]

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

        """
        # Basically, compaction works by opening a new db, writing
        # all the keys from this db to the new db, renaming the
        # new db to the filenames associated with this db, and
        # reopening the files associated with this db.  This
        # implementation can certainly be more efficient, but compaction
        # is really slow anyways.
        new_db = self.__class__(os.path.join(self._dbdir, 'compact'),
                                data_loader=self._data_loader,
                                renamer=self._renamer)
        for key in self._index:
            new_db[key] = self[key]
        new_db.sync()
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
        # os.rename() does not work if the dst file exists
        # on windows so we have to use our own version that
        # supports atomic renames.
        import semidbm.win32
        semidbm.win32.rename(from_file, to_file)


def _create_default_params(**starting_kwargs):
    kwargs = starting_kwargs.copy()
    # Internal method that creates the parameters based
    # on the choices like platform/available features.
    if sys.platform.startswith('win'):
        renamer = _WindowsRenamer()
    else:
        renamer = _Renamer()
    try:
        from semidbm.loaders.mmapload import MMapLoader
        data_loader = MMapLoader()
    except ImportError:
        # If mmap is not available then fall back to the
        # simple non mmap based file loader.
        from semidbm.loaders.simpleload import SimpleFileLoader
        data_loader = SimpleFileLoader()
    kwargs.update({'renamer': renamer, 'data_loader': data_loader})
    return kwargs


# The "dbm" interface is:
#
#     open(filename, flag='r', mode=0o666)
#
# All the other args after this should have default values
# so that this function remains compatible with the dbm interface.
def open(filename, flag='r', mode=0o666, verify_checksums=False):
    """Open a semidbm database.

    :param filename: The name of the db.  Note that for semidbm,
        this is actually a directory name.  The argument is named
        `filename` to be compatible with the dbm interface.

    :param flag: Specifies how the db should be opened.
        `flag` can be any of these values:

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

    :param verify_checksums: Verify the checksums for each value
        are correct on every __getitem__ call (defaults to False).

    """
    kwargs = _create_default_params(verify_checksums=verify_checksums)
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
