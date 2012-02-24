#!/usr/bin/env python

import os
import sys
import shutil
import unittest
import tempfile
import StringIO

import semidbm


class SemiDBMTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix='semidbm_ut')
        self.dbdir = os.path.join(self.tempdir, 'myfile.db')

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def open_db_file(self, **kwargs):
        return semidbm.open(self.dbdir, 'c', **kwargs)

    def open_data_file(self, dbdir=None, mode='r'):
        if dbdir is None:
            dbdir = self.dbdir
        if not os.path.exists(dbdir):
            os.makedirs(dbdir)
        data_filename = os.path.join(dbdir, 'data')
        return open(data_filename, mode=mode)

    def open_index_file(self, dbdir=None, mode='r'):
        """Given a dbdir, return a fileobj of the index file.

        The dbdir will be created if needed.
        """
        if dbdir is None:
            dbdir = self.dbdir
        if not os.path.exists(dbdir):
            os.makedirs(dbdir)
        index_filename = os.path.join(dbdir, 'data.idx')
        return open(index_filename, mode=mode)


class TestSemiDBM(SemiDBMTest):
    def test_insert_then_retrieve(self):
        db = self.open_db_file()
        db['foo'] = 'bar'
        self.assertEqual(db['foo'], 'bar')
        db.close()

    def test_insert_close_retrieve(self):
        # This will verify loading the index.
        db = self.open_db_file()
        db['foo'] = 'bar'
        db.close()

        db2 = self.open_db_file()
        self.assertEqual(db2['foo'], 'bar')
        db2.close()

    def test_insert_multiple(self):
        db = self.open_db_file()
        db['one'] = '1'
        db['two'] = '2'
        db['three'] = '3'
        self.assertEqual(db['one'], '1')
        self.assertEqual(db['two'], '2')
        self.assertEqual(db['three'], '3')
        db.close()

    def test_intermixed_inserts_and_retrievals(self):
        db = self.open_db_file()
        db['one'] = '1'
        db['two'] = '2'
        self.assertEqual(db['one'], '1')
        db['three'] = '3'
        self.assertEqual(db['two'], '2')
        self.assertEqual(db['three'], '3')
        db.close()

    def test_keyerror_raised_when_key_does_not_exist(self):
        db = self.open_db_file()
        self.assertRaises(KeyError, db.__getitem__, 'one')
        db.close()

    def test_updates(self):
        db = self.open_db_file()
        db['one'] = 'foo'
        db['one'] = 'bar'
        self.assertEqual(db['one'], 'bar')
        db['one'] = 'baz'
        self.assertEqual(db['one'], 'baz')
        db.close()

    def test_updates_persist(self):
        db = self.open_db_file()
        db['one'] = 'foo'
        db['one'] = 'bar'
        db['one'] = 'baz'
        db.close()

        db2 = self.open_db_file()
        self.assertEqual(db2['one'], 'baz')
        db2.close()

    def test_contains(self):
        db = self.open_db_file()
        db['one'] = 'foo'
        self.assertTrue('one' in db)
        db.close()

    def test_deletes(self):
        db = self.open_db_file()
        db['foo'] = 'bar'
        del db['foo']
        self.assertTrue('foo' not in db)
        db.close()

    def test_delete_key_not_there_when_reopened(self):
        db = self.open_db_file()
        db['foo'] = 'foo'
        db['bar'] = 'bar'
        del db['foo']
        db.close()

        db2 = self.open_db_file()
        self.assertTrue('foo' not in db2)
        self.assertEqual(db2['bar'], 'bar')
        db2.close()

    def test_compaction_of_index_file_on_open_deletes(self):
        db = self.open_db_file()
        for i in xrange(10):
            db[str(i)] = str(i)
        for i in xrange(10):
            del db[str(i)]
        db.close()
        db2 = self.open_db_file()
        self.assertEqual(os.stat(db2._index_filename).st_size, 0)
        db2.close()

    def test_compaction_does_not_leave_behind_files(self):
        db = self.open_db_file()
        before = len(os.listdir(self.dbdir))
        for i in xrange(10):
            db[str(i)] = str(i)
        for i in xrange(10):
            del db[str(i)]
        db.close()
        db2 = self.open_db_file()
        db2.compact()
        db2.close()
        after = len(os.listdir(self.dbdir))
        self.assertEqual(before, after, os.listdir(self.dbdir))

    def test_compaction_of_index_file_on_open_updates(self):
        # This is definitely implementation specific, but
        # I can't think of a better way to validate
        # update compaction in the index file.
        db = self.open_db_file()
        for i in xrange(10):
            db[str(i)] = str(i)
            db[str(i)] = str(i + 1)
            db[str(i)] = str(i + 2)
        # With 3 updates per key, the index file is 30 lines long.
        # On compaction, the index file should only be 10 minutes long.
        db.close()
        db2 = self.open_db_file()
        self.assertEqual(len(open(db2._index_filename).readlines()), 10)
        db2.close()

    def test_keys_method(self):
        db = self.open_db_file()
        db['one'] = 'bar'
        db['two'] = 'bar'
        db['three'] = 'bar'
        self.assertEqual(set(db.keys()), set(['one', 'two', 'three']))
        db.close()

    def test_iterate(self):
        db = self.open_db_file()
        db['one'] = 'foo'
        db['two'] = 'bar'
        db['three'] = 'baz'
        self.assertEqual(set(db), set(['one', 'two', 'three']))
        db.close()

    def test_compaction_of_data_file_on_open_deletes(self):
        db = self.open_db_file()
        db['key'] = 'original'
        db['key'] = 'updated'
        del db['key']
        db.compact()
        db.close()
        self.assertEqual(len(open(db._data_filename).read()), 0)

    def test_compact_and_retrieve_data(self):
        db = self.open_db_file()
        db['one'] = 'foo'
        db['key'] = 'original'
        db['two'] = 'bar'
        db['key'] = 'updated'
        del db['key']
        db['three'] = 'baz'
        db.compact()
        self.assertEqual(db['one'], 'foo')
        self.assertEqual(db['two'], 'bar')
        self.assertEqual(db['three'], 'baz')
        db.close()

    def test_compact_on_close(self):
        db = self.open_db_file()
        db['key'] = 'original'
        del db['key']
        db.close(compact=True)
        self.assertEqual(len(open(db._data_filename).read()), 0)

    def test_compact_then_write_data(self):
        db = self.open_db_file()
        db['before'] = 'before'
        del db['before']
        db.compact()
        db['after'] = 'after'
        db.close()

        db2 = self.open_db_file()
        self.assertEqual(db2['after'], 'after')
        db2.close()

    def test_loading_error_bad_format(self):
        dbdir = os.path.join(self.tempdir, 'bad.db')
        with self.open_index_file(dbdir=dbdir, mode='w') as f:
            f.write("bad index file")
        self.assertRaises(semidbm.DBMLoadError, semidbm.open, dbdir, 'c')

    def test_loading_error_bad_line(self):
        dbdir = os.path.join(self.tempdir, 'bad.db')
        with self.open_index_file(dbdir=dbdir, mode='w') as f:
            # The first number should be 3 not 4, so
            # a DBMLoadError is expected.
            f.write("4:foo3:1242:12\n")
        self.assertRaises(semidbm.DBMLoadError, semidbm.open, dbdir, 'c')

    def test_loading_error_missing_fields(self):
        dbdir = os.path.join(self.tempdir, 'bad.db')
        with self.open_index_file(dbdir=dbdir, mode='w') as f:
            # Missing the size attribute (the third value of the line).
            f.write("4:foo3:124\n4:bar3:189\n")
        self.assertRaises(semidbm.DBMLoadError, semidbm.open, dbdir, 'c')

    def test_sync_contents(self):
        # So there's not really a good way to test this, so
        # I'm just making sure you can call it, and you can see the data.
        db = self.open_db_file()
        db['foo'] = 'bar'
        db.sync()
        db.close()
        db2 = self.open_db_file()
        self.assertEqual(db2['foo'], 'bar')
        db2.close()


class TestReadOnlyMode(SemiDBMTest):
    def open_db_file(self, **kwargs):
        return semidbm.open(self.dbdir, 'r', **kwargs)

    def test_cant_setitem(self):
        db = self.open_db_file()
        self.assertRaises(semidbm.DBMError, db.__setitem__, 'foo', 'bar')
        db.close()

    def test_cant_sync(self):
        db = self.open_db_file()
        self.assertRaises(semidbm.DBMError, db.sync)
        db.close()

    def test_cant_compact(self):
        db = self.open_db_file()
        self.assertRaises(semidbm.DBMError, db.compact)
        db.close()

    def test_cant_delitem(self):
        db = self.open_db_file()
        self.assertRaises(semidbm.DBMError, db.__delitem__, 'foo')
        db.close()

    def test_close_never_compacts_index(self):
        db = self.open_db_file()
        db.calls = []
        db.compact = lambda: db.calls.append('compact')
        db.sync = lambda: db.calls.append('sync')

        db.close(compact=True)

        self.assertEqual(db.calls, [])

    def test_open_read_multiple_times(self):
        db = semidbm.open(self.dbdir, 'c')
        db['foo'] = 'bar'
        db.close()
        # Open then close db immediately.
        db2 = self.open_db_file()
        db2.close()
        read_only = self.open_db_file()
        self.assertEqual(read_only['foo'], 'bar')
        read_only.close()

    def test_can_read_items(self):
        db = semidbm.open(self.dbdir, 'c')
        db['foo'] = 'bar'
        db['bar'] = 'baz'
        db['baz'] = 'foo'
        db.close()

        read_only = self.open_db_file()
        self.assertEqual(read_only['foo'], 'bar')
        self.assertEqual(read_only['bar'], 'baz')
        self.assertEqual(read_only['baz'], 'foo')
        read_only.close()

    def test_key_does_not_exist(self):
        db = semidbm.open(self.dbdir, 'c')
        db['foo'] = 'bar'
        db.close()

        read_only = self.open_db_file()
        self.assertRaises(KeyError, read_only.__getitem__, 'bar')
        read_only.close()

    def test_checksum_failure(self):
        db = semidbm.open(self.dbdir, 'c')
        db['foo'] = 'bar'
        db.close()
        # Change the first digit of the checksum data.
        data_file = self.open_data_file(mode='r')
        new_digit = int(data_file.read(1)) + 1
        data_file.close()
        data_file = self.open_data_file(mode='w')
        data_file.write(str(new_digit))
        data_file.close()
        db = self.open_db_file(verify_checksums=True)
        with self.assertRaises(semidbm.DBMChecksumError):
            db['foo']


class TestReadOnlyModeMMapped(TestReadOnlyMode):
    def open_db_file(self, **kwargs):
        return semidbm._SemiDBMReadOnlyMMap(self.dbdir, **kwargs)

    def test_load_empty_db(self):
        db = semidbm.open(self.dbdir, 'c')
        db.close()
        empty_db = self.open_db_file()
        keys = empty_db.keys()
        empty_db.close()
        self.assertEqual(keys, [])


class TestWriteMode(SemiDBMTest):
    def test_when_index_file_does_not_exist(self):
        self.open_index_file(mode='w')
        self.assertRaises(semidbm.DBMError, semidbm.open, self.dbdir, 'w')

    def test_when_data_file_does_not_exist(self):
        self.assertRaises(semidbm.DBMError, semidbm.open, self.dbdir, 'w')

    def test_when_files_exist(self):
        db = self.open_db_file()
        db['foo'] = 'bar'
        db.close()

        db_write_mode = semidbm.open(self.dbdir, 'w')
        self.assertEqual(db_write_mode['foo'], 'bar')
        db_write_mode.close()


class TestNewMode(SemiDBMTest):
    def test_when_file_does_not_exist(self):
        path = os.path.join(self.tempdir, 'foo.db')
        db = semidbm.open(path, 'n')
        db['foo'] = 'bar'
        self.assertEqual(db['foo'], 'bar')
        db.close()

        # Opening the file again should basically blank out
        # any existing database.
        db = semidbm.open(path, 'n')
        self.assertEqual(db.keys(), [])
        db.close()


class TestInvalidModeArgument(unittest.TestCase):
    def test_invalid_open_arg_raises_exception(self):
        self.assertRaises(ValueError, semidbm.open, 'foo.db', 'z')


class TestWindowsSemidbm(TestSemiDBM):
    def setUp(self):
        super(TestWindowsSemidbm, self).setUp()
        self.original_platform = sys.platform
        # The win32 specific code is compatible with posix platforms,
        # so these tests can run on mac/linux/etc. just find.
        sys.platform = 'win32'

    def tearDown(self):
        super(TestWindowsSemidbm, self).tearDown()
        sys.platform = self.original_platform 


if __name__ == '__main__':
    unittest.main()
