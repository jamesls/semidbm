Overview
========

SemiDBM is an attempt at improving the dumbdbm in the python standard library.
It's a slight improvement in both performance and in durability.  It can be
used anywhere dumbdbm would be appropriate to use, which is basically when you
have no other options available.  It uses a similar design to dumbdbm which
means that it does inherit some of the same problems as dumbdbm, but it also
attempts to fix problems in dumbdbm, which makes it only a semi-dumb dbm :)
