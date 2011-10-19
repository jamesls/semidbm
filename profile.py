#!/usr/bin/env python
"""Very simple script for profiling various dbms.

"""
import os
import optparse
import time
import tempfile
import random

random.seed(100)


_potential_dbms = ['dbhash', 'dbm', 'gdbm', 'dumbdbm', 'semidbm']
DBMS = []
TEMPDIR = tempfile.mkdtemp(prefix='dbmprofile')


def set_dbms(dbms):
    DBMS[:] = []
    for potential in dbms:
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
        print "%.6f" % (end - start),
    return _time_function


def basic_test(num_keys, value_size):
    title = "\n\nbasic keys=%s value_size=%s" % (num_keys, value_size)
    print title
    print '=' * len(title)
    print
    for dbm in DBMS:
        _clean_out_dir(TEMPDIR)
        fresh_populate(dbm, num_keys, value_size)
        sequential_reads(dbm, num_keys)
        keys = range(num_keys)
        random.shuffle(keys)
        random_reads(dbm, keys)
        fixed_reads(dbm, keys[:100])
        new_keys = [str(k) + "_new" for k in keys[:100]]
        fixed_writes(dbm, new_keys, value_size)
        if dbm.__name__ == 'dumbdbm' and num_keys * value_size > 500000:
            # dumbdbm writes out the entire index on
            # a delete, so these were taking on the
            # order of minutes for the 10MB tests.
            print "delall: TOOOSLOW",
        else:
            delete_all(dbm, num_keys)
        load_twice(dbm)
        print
    _clean_out_dir(TEMPDIR)


def _clean_out_dir(dirname):
    for path in os.listdir(dirname):
        os.remove(os.path.join(dirname, path))


@print_time
def fresh_populate(dbm, num_keys, value_size):
    print "%-8s %-7s %-6s" % (dbm.__name__, num_keys,
                              value_size),
    print "load:",
    db = dbm.open(os.path.join(TEMPDIR, 'db'), 'c')
    for i in xrange(num_keys):
        db[str(i)] = os.urandom(value_size)
    db.close()


@print_time
def sequential_reads(dbm, num_keys):
    print "seq_read:",
    db = dbm.open(os.path.join(TEMPDIR, 'db'), 'r')
    for i in xrange(num_keys):
        db[str(i)]
    db.close()


@print_time
def random_reads(dbm, key_order):
    print "random:",
    db = dbm.open(os.path.join(TEMPDIR, 'db'), 'r')
    for key in key_order:
        db[str(key)]
    db.close()


@print_time
def fixed_reads(dbm, key_order):
    # This is to test how long it takes to read a fixed amount of keys.
    print "read%s:" % len(key_order),
    db = dbm.open(os.path.join(TEMPDIR, 'db'), 'r')
    for key in key_order:
        db[str(key)]
    db.close()


@print_time
def fixed_writes(dbm, key_order, value_size):
    print "write%s:" % len(key_order),
    db = dbm.open(os.path.join(TEMPDIR, 'db'), 'c')
    for key in key_order:
        db[key] = os.urandom(value_size)
    db.close()


@print_time
def delete_all(dbm, num_keys):
    print "delall:",
    db = dbm.open(os.path.join(TEMPDIR, 'db'), 'c')
    for i in xrange(num_keys):
        del db[str(i)]
    db.close()


@print_time
def load_twice(dbm):
    print "load2",
    db = dbm.open(os.path.join(TEMPDIR, 'db'), 'c')
    db.close()
    db2 = dbm.open(os.path.join(TEMPDIR, 'db'), 'c')
    db2.close()


def main():
    parser = optparse.OptionParser()
    parser.add_option('--dbm', dest='dbms', action='append')
    opts, args = parser.parse_args()
    if opts.dbms:
        set_dbms(opts.dbms)
    else:
        set_dbms(_potential_dbms)
    # These generate the same amount of value data,
    # but one is 401 keys with 512 byte sizes and the
    # other is 512 keys with 401 byte sizes.
    basic_test(num_keys=401, value_size=512)
    basic_test(num_keys=512, value_size=401)

    # Large (~10MB db size).
    basic_test(num_keys=10000, value_size=1024)
    basic_test(num_keys=10000, value_size=495)


if __name__ == '__main__':
    main()
