=========
Changelog
=========

0.5.0
=====

* Remove mmap read only dbm subclass. This functionality
  has not been available in a public interface since
  b265e60c5f4c0b1e8e9e4343f5f2300b5e017bf0 (1.5 years ago)
  so it's now removed.
* Added non mmap based dbm loader for platforms that do not
  support mmap.


0.4.0
=====

0.4.0 is a backwards incompatible release with 0.3.1.
Data files created with 0.3.1 will not work with 0.4.0.
The reasons for switching to 0.4.0 include:

* Data format switched from ASCII to binary file format,
  this resulted in a nice performance boost.
* Index and data file consolidated to a single file, resulting
  in improved write performance.
* Checksums are written for all entries.  Checksums can
  be verified for every __getitem__ call (off by default).
* Python 3 support (officially python 3.3.x).


0.3.1
=====

* Windows support.


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


