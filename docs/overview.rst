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
* Data and index file are always in sync after every operation
  (get/set/delete).
* Index and data file compaction.  Free space can be reclaimed (though this
  only happens when the db is opened or whenever explicitly asked to do so
  using the `compact()` method).


There are also a few caveats to consider when using semidbm:

* The entire index must fit in memory.
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

**Keys and values must be strings.**
