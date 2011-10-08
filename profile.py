#!/usr/bin/env python
"""Very simple script for profiling various dbms.

"""
import os
import time
import tempfile
import random

random.seed(100)


_potential_dbms = ['hashdb', 'dbm', 'gdbm', 'dumbdbm', 'semidbm']
DBMS = []
TEMPDIR = tempfile.mkdtemp(prefix='dbmprofile')


for potential in _potential_dbms:
    try:
        d = __import__(potential)
        DBMS.append(d)
    except ImportError:
        continue


def print_time(f):
    def _time_function(*args, **kwargs):
        start = time.time()
        f(*args, **kwargs)
        end = time.time()
        print "time: %.8f" % (end - start),
    return _time_function


def write_test():
    for dbm in DBMS:
        num_keys = 10000
        _clean_out_dir(TEMPDIR)
        fresh_populate(dbm, num_keys, 1024)
        sequential_reads(dbm, num_keys)
        keys = range(num_keys)
        random.shuffle(keys)
        random_reads(dbm, keys)
        print
    _clean_out_dir(TEMPDIR)


def _clean_out_dir(dirname):
    for path in os.listdir(dirname):
        os.remove(os.path.join(dirname, path))


@print_time
def fresh_populate(dbm, num_keys, value_size):
    print "%-10s %-15s %-15s" % (dbm.__name__, num_keys,
                                 value_size),
    db = dbm.open(os.path.join(TEMPDIR, 'db'), 'c')
    for i in xrange(num_keys):
        db[str(i)] = os.urandom(value_size)
    db.close()


@print_time
def sequential_reads(dbm, num_keys):
    print "seq_read",
    db = dbm.open(os.path.join(TEMPDIR, 'db'), 'r')
    for i in xrange(num_keys):
        db[str(i)]


@print_time
def random_reads(dbm, key_order):
    print "  random",
    db = dbm.open(os.path.join(TEMPDIR, 'db'), 'r')
    for key in key_order:
        db[str(key)]


def main():
    write_test()


if __name__ == '__main__':
    main()
