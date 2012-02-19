=========
Changelog
=========

0.3.0
=====

* The data file and the index file are kept in a separate directory.  To load
  the the db you specify the directory name instead of the data filename.
* Non-mmapped read only version is used when the db is opened with ``r``.
* Write performance improvements.


0.2.1
=====

* DB can be opened with ``r``, ``c``, ``w``, and ``n``.
* Add a memory mapped read only implementation for reading
  from the DB (if your entire data file can be mmapped this
  provides a huge performance boost for reads).
* Benchmark scripts rewritten to provide more useful information.


0.2.0
=====

* New ``sync()`` method to ensure data is written to disk.

  * ``sync()`` is called during compaction and on ``close()``.

* Add a ``DBMLoadError`` exception for catching semidbm loading errors.


