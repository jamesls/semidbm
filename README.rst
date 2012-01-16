========
Overview
========

SemiDBM is an attempt at improving the dumbdbm in the python standard library.
It's a slight improvement in both performance and in durability.  It can be
used anywhere dumbdbm would be appropriate to use, which is basically when you
have no other options available.  It uses a similar design to dumbdbm which
means that it does inherit some of the same problems as dumbdbm, but it also
attempts to fix problems in dumbdbm, which makes it only a semi-dumb dbm :)

A design goal of semidbm is to remain a pure python dbm.  This makes
installation easy and allows semidbm to be used on any platform that
supports python.


============
Improvements
============

Below are a list of some of the improvements semidbm makes over dumbdbm.

Index and Data File In Sync
===========================

Both the index file and the data file are updated on every write operation.
This ensures that the index and data file are always in sync with each other.
This was a problem with dumbdbm.  The index file was out of sync with the
data file as soon as updates were performed.  With semidbm, any create, update,
or delete command will be written out to the index file.

Index and Data File Compaction
==============================

Semidbm uses a similar (but not identical) append only file format.  This has
the potential to grow to large sizes as space is never reclaimed.  Semidbm
addresses this in two ways:

* Compact the index when instantiated.  Semidbm will compact the index if
  necessary when the index is initially loaded.
* Add a compact() method that compacts the index and the data file.  This
  allows a client to compact the db whenever they need.

Performance
===========

Semidbm is significantly faster than dumbdbm (keep in mind both are pure python
libraries) in just about every way.  There is a ``profile.py`` script
distributed with the source that will give you an idea of the performance
differences between the various dbms (it will benchmark any dbm from the stdlib
you have installed you can compare against dbm, gdbm, Berkeley DB, etc.)

Modes
=====

You can open semidbm in these modes:

* ``r`` - Open dbm for reading only (default).
* ``w`` - Open dbm for read/write (db file must exist).
* ``c`` - Open dbm for read/write, creating it if it doesn't exist.
* ``n`` - Open dbm for read/write, erasing exisiting dbm if it exists.


Limitations
===========

* Not thread safe; can't be accessed by multiple processes.
* The entire index must fit in memory.


Post feedback and issues on `github issues`_, or check out the
latest changes at the github `repo`_.


Changelog
---------

0.2.1

* DB can be opened with ``r``, ``c``, ``w``, and ``n``.
* Add a memory mapped read only implementation for reading
  from the DB (if your entire data file can be mmapped this
  provides a huge performance boost for reads).
* Benchmark scripts rewritten to provide more useful information.


~~~~~
0.2.0
~~~~~

* New ``sync()`` method to ensure data is written to disk.

  * ``sync()`` is called during compaction and on ``close()``.

* Add a ``DBMLoadError`` exception for catching semidbm loading errors.


.. _github issues: https://github.com/jamesls/semidbm/issues
.. _repo: https://github.com/jamesls/semidbm
