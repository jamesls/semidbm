======================
An Overview of Semidbm
======================

The easiest way to think of semidbm is as an improvement over python's
`dumbdbm <http://docs.python.org/library/dumbdbm.html>`_ module.

While the standard library has faster dbms based on well established C
libraries (GNU dbm, Berkeley DB, ndbm), dumbdbm is the only pure python
portable dbm in the standard library.

Semidbm offers a few improvements over dumbdbm including:

* Better overall performance (more on this later).
* Only a single file is used (no separate index and data file).
* Data file compaction.  Free space can be reclaimed (though this
  only happens whenever explicitly asked to do so
  using the `compact()` method).
* Get/set/delete are require O(1) IO.

Like dumbdbm, semidbm is cross platform.  It has been tested on:

* Linux (Ubuntu 11.10, debian)
* Mac OS X (Lion/Mountain Lion)
* Windows 7/8.

There are also a few caveats to consider when using semidbm:

* The entire index must fit in memory, this means all keys must
  fit in memory.
* Not thread safe; can only be accessed by a single process.
* While the performance is reasonable, it still will not beat one of the
  standard dbms (GNU dbm, Berkeley DB, etc).


Using Semidbm
=============

To create a new db, specify the name of the directory::

    import semidbm
    db = semidbm.open('mydb', 'c')

This will create a *mydb* directory.  This directory is where semidbm will
place all the files it needs to keep track of the keys and values stored in the
db.  If the directory does not exist, it will be created.


Once the db has been created, you can get and set values::

    db['key1'] = 'value1'
    print db['key1']

**Keys and values can be either str or bytes.**

``str`` types will be encoded to utf-8 before writing to disk.
You can avoid this encoding step by providing a byte string
directly::

    db[b'key1'] = b'value1'

Otherwise, semidbm will do the equivalent of::

    db['key1'.encode('utf-8')] = 'value1'.encode('utf-8')

It is recommended that you handle the encoding of your strings
in your application, and only use ``bytes`` when working with
semidbm.  The reason for this is that when a value
is retrieved, it is returned as a bytestring (semidbm can't
know the encoding of the bytes it retrieved).  For example (this
is with python 3.3)::

    >>> db['foo'] = 'value'
    >>> db['foo']
    b'value'
    >>> db['kēy'] = 'valueē'
    >>> db['kēy']
    b'value\xc4\x93'

To avoid this confusion, encode your strings before storing with
with semidbm.

The reason this automatic conversion is supported is that this is
what is done with the DBMs in the python standard library (including
``dumbdbm`` which this module was intended to be a drop in replacement
for).  In order to be able to be a drop in replacement, this
automatic encoding process needs to be supported (but not recommended).
