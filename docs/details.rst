===============
SemiDBM Details
===============

This guide goes into the details of how semidbm works.

Writing a Value
===============

One of the key characteristics of semidbm is that it only writes to the end of
a file.  **Once data has been written to a file, it is never changed.**  This
makes it easy to guarantee that once the data is written to disk, you can be
certain that semidbm will not corrupt the data.  This also makes semidbm
simpler because we don't have to worry about how to modify data in a way that
prevents corruption in the event of a crash.

Even updates to existing values are written as new values at the end of
a file.  When the data file is loaded, these transactions are "replayed"
so that the last change will "win".  For example, given these operations::

    add key "foo" with value "bar"
    add key "foo2" with value "bar2"
    delete key "foo2"
    add key "foo" with value "new value"

this would represent a dictionary that looked like this::

    {"foo": "new value"}

.. note::

  This is just the conceptual view of the transactions.  The actual
  format is a binary format specified in :doc:`fileformat`.

You can imagine that a db with a large number of updates can cause
the file to grow to a much larger size than is needed.  To reclaim
fixed space, you can use the ``compact()`` method.  This will
rewrite the data file is the shortest amount of transactions
needed.  The above example can be compacted to::

    add key "foo" with value "new value"

When a compaction occurs, a new data file is written out (the original
data file is left untouched).  Once all the compacted data has been
written out to the new data file (and fsync'd!), the new data file
is renamed over the original data file, completing the compaction.
This way, if a crash occurs during compaction, the original data file
is not corrupted.


Reading Values
==============

The index that is stored in memory does not contain the actual
data associated with the key.  Instead, it contains the location
within the file where the value is located, conceptually::

    db = {'foo': DiskLocation(offset=40, size=10)}

When the value for a key is requested, the offset and size are looked
up.  A disk seek is performed and a read is performed for the
specified size associated with the value.  This translates to
2 syscalls::

    lseek(fd, offset, os.SEEKSET)
    data = read(fs, value_size)

Data Verification
=================

Every write to a semidbm db file also includes a crc32 checksum.
When a value is read from disk, semidbm can verify this crc32 checksum.
By default, this verification step is turned off, but can be enabled using the
``verify_checksums`` argument::

    >>> db = semidbm.open('dbname', 'c', verify_checksums=True)

If a checksum error is detected a ``DBMChecksumError`` is raised::

    >>> db[b'foo']
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "./semidbm/db.py", line 192, in __getitem__
        return self._verify_checksum_data(key, data)
      File "./semidbm/db.py", line 203, in _verify_checksum_data
        "Corrupt data detected: invalid checksum for key %s" % key)
    semidbm.db.DBMChecksumError: Corrupt data detected: invalid checksum for key b'foo'


Read Only Mode
==============

SemiDBM includes an optimized read only mode.  If you know you only
want to read values from the database without writing new values you
can take advantage of this optimized read only mode.  To open a db
file as read only, use the ``'r'`` option::

    db = semidbm.open('dbname', 'r')
