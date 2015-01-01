========
Overview
========

.. image:: https://secure.travis-ci.org/jamesls/semidbm.png?branch=master
   :target: http://travis-ci.org/jamesls/semidbm

.. image:: https://coveralls.io/repos/jamesls/semidbm/badge.png?branch=master
   :target: https://coveralls.io/r/jamesls/semidbm?branch=master

.. image:: https://pypip.in/version/semidbm/badge.svg
    :target: https://pypi.python.org/pypi/semidbm/
    :alt: Latest Version

.. image:: https://pypip.in/py_versions/semidbm/badge.svg
    :target: https://pypi.python.org/pypi/semidbm/
    :alt: Supported Python versions

.. image:: https://pypip.in/implementation/semidbm/badge.svg
    :target: https://pypi.python.org/pypi/semidbm/
    :alt: Supported Python implementations

.. image:: https://pypip.in/license/semidbm/badge.svg
    :target: https://pypi.python.org/pypi/semidbm/
    :alt: License

.. image:: https://pypip.in/wheel/semidbm/badge.svg
    :target: https://pypi.python.org/pypi/semidbm/
    :alt: Wheel Status


Semidbm is a fast, pure python implementation of a dbm, which is a
persistent key value store. It allows you to get and set keys through
a dict interface::

    import semidbm
    db = semidbm.open('testdb', 'c')
    db['foo'] = 'bar'
    print db['foo']
    db.close()

These values are persisted to disk, and you can later retrieve
these key/value pairs::

    # Then at a later time:
    db = semidbm.open('testdb', 'r')
    # prints "bar"
    print db['foo']


It was written with these things in mind:

* Pure python, supporting python 2.6, 2.7, 3.3, and 3.4.
* Cross platform, works on Windows, Linux, Mac OS X.
* Supports CPython, pypy, and jython (versions 2.7-b3 and higher).
* Simple and Fast (See `Benchmarking Semidbm <http://semidbm.readthedocs.org/en/latest/benchmarks.html>`__).


Supported Python Versions
=========================

Semidbm supports python 2.6, 2.7, 3.3, and 3.4.

=============
Official Docs
=============

Read the `semidbm docs <http://semidbm.readthedocs.org>`_ for more information
and how to use semidbm.


========
Features
========

Semidbm originally started off as an improvement over the
`dumbdbm <https://docs.python.org/2/library/dumbdbm.html>`__
library in the python standard library.  Below are a list of some of the
improvements over dumbdbm.


Single Data File
================

Instead of an index file and a data file, the index and data have been
consolidated into a single file.  This single data file is always appended to,
data written to the file is never modified.


Data File Compaction
====================

Semidbm uses an append only file format.  This has the potential to grow to
large sizes as space is never reclaimed.  Semidbm addresses this by adding a
``compact()`` method that will rewrite the data file to a minimal size.


Performance
===========

Semidbm is significantly faster than dumbdbm (keep in mind both are pure python
libraries) in just about every way.  The documentation shows the
`results <http://semidbm.readthedocs.org/en/latest/benchmarks.html>`_
of semidbm vs. other dbms, along with how to run the benchmarking
script yourself.


===========
Limitations
===========

* Not thread safe; can't be accessed by multiple processes.
* The entire index must fit in memory.  This essentially means that all of the
  keys must fit in memory.


Post feedback and issues on `github issues`_, or check out the
latest changes at the github `repo`_.


.. _github issues: https://github.com/jamesls/semidbm/issues
.. _repo: https://github.com/jamesls/semidbm
