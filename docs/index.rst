Semidbm
=======

semidbm is a pure python implementation of a dbm, which is essentially a
persistent key value store. It allows you to get and set keys::

    import semidbm
    db = semidbm.open('testdb', 'c')
    db['foo'] = 'bar'
    print db['foo']

    db.close()
    # Then at a later time:
    db = semidbm.open('testdb', 'r')
    # prints "bar"
    print db['foo']


It was written with these things in mind:

* Pure python.  Many of the "standard" dbms are written in C,
  which requires a C extension to give python code access to the dbm.
  This of course make installation more complicated, requires that
  the OS you're using has the shared library installed, requires
  that you have the necessary setup to build a C extension for python
  (unless you want to use a binary package).  Semidbm supports python
  2.6, python 2.7, and python 3.3.
* Cross platform.  Because semidbm is written in python, it runs
  on any platform that supports python.  The file format used for
  semidbm is also cross platform.
* Simplicity.  The original design is based off of python's dumbdbm module
  in the standard library, and one of the goals of semidbm is to try to keep
  the design comparably simple.



Post feedback and issues on
`github issues <https://github.com/jamesls/semidbm/issues>`_, or check out the
latest changes at the github `repo <https://github.com/jamesls/semidbm>`_.


Topics
------

.. toctree::
    :maxdepth: 2

    overview
    details
    benchmarks
    changelog


Developer Documentation
-----------------------

.. toctree::
    :maxdepth: 2

    api_semidbm
    fileformat


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

