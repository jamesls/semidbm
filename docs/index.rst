.. semidbm documentation master file, created by
   sphinx-quickstart on Sun Feb 12 21:08:54 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

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
  (unless you want to use a binary package).
* Cross platform.  Because semidbm is written in python, it runs
  on any platform that supports python.  The file format used for
  semidbm is also cross platform.
* Simplicity.  The original design is based off of python's dumbdbm module
  in the standard library, and one of the goals of semidbm is to try to keep
  the design comparably simple.


API Documentation
-----------------

.. toctree::
    :maxdepth: 2

    api_semidbm


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

