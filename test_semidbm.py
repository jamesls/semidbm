#!/usr/bin/env python

import os
import sys
import mmap
import shutil
import struct
import tempfile
try:
    import unittest2 as unittest
except ImportError:
    import unittest

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


class TestSemiDBM(SemiDBMTest):
    def test_insert_then_retrieve(self):
        db = self.open_db_file()
        db['foo'] = 'bar'
        self.assertEqual(db['foo'], b'bar')
        db.close()

    def test_insert_close_retrieve(self):
        # This will verify loading the index.
        db = self.open_db_file()
        db['foo'] = 'bar'
        db.close()

        db2 = self.open_db_file()
        self.assertEqual(db2['foo'], b'bar')
        db2.close()

    def test_insert_multiple(self):
        db = self.open_db_file()
        db['one'] = '1'
        db['two'] = '2'
        db['three'] = '3'
        self.assertEqual(db['one'], b'1')
        self.assertEqual(db['two'], b'2')
        self.assertEqual(db['three'], b'3')
        db.close()

    def test_intermixed_inserts_and_retrievals(self):
        db = self.open_db_file()
        db['one'] = '1'
        db['two'] = '2'
        self.assertEqual(db['one'], b'1')
        db['three'] = '3'
        self.assertEqual(db['two'], b'2')
        self.assertEqual(db['three'], b'3')
        db.close()

    def test_keyerror_raised_when_key_does_not_exist(self):
        db = self.open_db_file()
        self.assertRaises(KeyError, db.__getitem__, 'one')
        db.close()

    def test_updates(self):
        db = self.open_db_file()
        db['one'] = 'foo'
        db['one'] = 'bar'
        self.assertEqual(db['one'], b'bar')
        db['one'] = 'baz'
        self.assertEqual(db['one'], b'baz')
        db.close()

    def test_updates_persist(self):
        db = self.open_db_file()
        db['one'] = 'foo'
        db['one'] = 'bar'
        db['one'] = 'baz'
        db.close()

        db2 = self.open_db_file()
        self.assertEqual(db2['one'], b'baz')
        db2.close()

    def test_contains(self):
        db = self.open_db_file()
        db[b'one'] = 'foo'
        self.assertTrue(b'one' in db)
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
        self.assertEqual(db2['bar'], b'bar')
        db2.close()

    def test_multiple_deletes(self):
        db = self.open_db_file()
        db['foo'] = 'foo'
        del db['foo']
        db['foo'] = 'foo'
        del db['foo']
        db['foo'] = 'foo'
        del db['foo']
        db['bar'] = 'bar'
        db.close()
        db2 = self.open_db_file()
        self.assertTrue('foo' not in db2)
        self.assertEqual(db2['bar'], b'bar')

    def test_keys_method(self):
        db = self.open_db_file()
        db['one'] = 'bar'
        db['two'] = 'bar'
        db['three'] = 'bar'
        self.assertEqual(set(db.keys()), set([b'one', b'two', b'three']))
        db.close()

    def test_values_method(self):
        db = self.open_db_file()
        db['one'] = 'one_value'
        db['two'] = 'two_value'
        db['three'] = 'three_value'
        self.assertEqual(set(db.values()), set([b'one_value', b'two_value',
                                                b'three_value']))

    def test_iterate(self):
        db = self.open_db_file()
        db['one'] = 'foo'
        db['two'] = 'bar'
        db['three'] = 'baz'
        self.assertEqual(set(db), set([b'one', b'two', b'three']))
        db.close()

    def test_sync_contents(self):
        # So there's not really a good way to test this, so
        # I'm just making sure you can call it, and you can see the data.
        db = self.open_db_file()
        db['foo'] = 'bar'
        db.sync()
        db.close()
        db2 = self.open_db_file()
        self.assertEqual(db2['foo'], b'bar')
        db2.close()

    def test_compaction_does_not_leave_behind_files(self):
        db = self.open_db_file()
        before = len(os.listdir(self.dbdir))
        for i in range(10):
            db[str(i)] = str(i)
        for i in range(10):
            del db[str(i)]
        db.close()
        db2 = self.open_db_file()
        db2.compact()
        db2.close()
        after = len(os.listdir(self.dbdir))
        self.assertEqual(before, after, os.listdir(self.dbdir))

    def test_inserts_after_deletes(self):
        db = self.open_db_file()
        db['one'] = b'one'
        del db['one']
        db['two'] = b'two'

        self.assertEqual(db['two'], b'two')

    def test_mixed_updates_and_deletes(self):
        db = self.open_db_file()
        db['one'] = 'one'
        db['CHECK'] = 'original'
        db['two'] = 'two'
        db['CHECK'] = 'updated'
        del db['CHECK']
        db['three'] = 'three'

        self.assertEqual(db['one'], b'one')
        self.assertEqual(db['two'], b'two')
        self.assertEqual(db['three'], b'three')

    def test_compact_and_retrieve_data(self):
        db = self.open_db_file()
        db['one'] = 'foo'
        db['key'] = 'original'
        db['two'] = 'bar'
        db['key'] = 'updated'
        del db['key']
        db['three'] = 'baz'
        db.compact()
        self.assertEqual(db['one'], b'foo')
        self.assertEqual(db['two'], b'bar')
        self.assertEqual(db['three'], b'baz')
        db.close()

    def test_compact_on_close(self):
        db = self.open_db_file()
        db['key'] = 'original'
        del db['key']
        db.close(compact=True)
        # Header is 8 bytes.
        self.assertEqual(len(open(db._data_filename).read()), 8)

    def test_compact_then_write_data(self):
        db = self.open_db_file()
        db['before'] = 'before'
        del db['before']
        db.compact()
        db['after'] = 'after'
        db.close()

        db2 = self.open_db_file()
        self.assertEqual(db2['after'], b'after')
        db2.close()


class TestSignatureMismatch(SemiDBMTest):
    def test_bad_magic_number(self):
        db = self.open_db_file()
        db['foo'] = 'bar'
        db.close()
        with self.open_data_file(mode='rb+') as f:
            f.seek(0)
            f.write(b'Z')
        # Opening the db file should now fail.
        self.assertRaises(semidbm.DBMLoadError, self.open_db_file)

    def test_incompatible_version_number(self):
        db = self.open_db_file()
        db['foo'] = 'bar'
        db.close()
        with self.open_data_file(mode='rb+') as f:
            f.seek(4)
            f.write(struct.pack('!H', 2))
        # Opening the db file should now fail.
        self.assertRaises(semidbm.DBMLoadError, self.open_db_file)


class TestRemapping(SemiDBMTest):
    def setUp(self):
        super(TestRemapping, self).setUp()
        self.original = semidbm.db._MAPPED_LOAD_PAGES
        # Change the number of mapped pages to 1 so that we don't have to write
        # as much data.  The logic in the code uses this constant, so changing
        # the value of the constant won't affect the code logic, it'll just
        # make the test run faster.
        semidbm.db._MAPPED_LOAD_PAGES = 1

    def tearDown(self):
        super(TestRemapping, self).tearDown()
        semidbm.db._MAPPED_LOAD_PAGES = self.original

    def test_remap_required(self):
        # Verify the loading buffer logic works.  This is
        # really slow.
        size = semidbm.db._MAPPED_LOAD_PAGES * mmap.ALLOCATIONGRANULARITY * 4
        db = self.open_db_file()
        # 100 byte values.
        values = b'abcd' * 25
        for i in range(int(size / 100)):
            db[str(i)] = values
        db.close()

        db2 = self.open_db_file()
        for k in db2:
            self.assertEqual(db2[k], values)


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
        self.assertEqual(read_only['foo'], b'bar')
        read_only.close()

    def test_can_read_items(self):
        db = semidbm.open(self.dbdir, 'c')
        db['foo'] = 'bar'
        db['bar'] = 'baz'
        db['baz'] = 'foo'
        db.close()

        read_only = self.open_db_file()
        self.assertEqual(read_only[b'foo'], b'bar')
        self.assertEqual(read_only[b'bar'], b'baz')
        self.assertEqual(read_only[b'baz'], b'foo')
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
        db[b'key'] = b'value'
        db.close()
        data_file = self.open_data_file(mode='rb')
        contents = data_file.read()
        data_file.close()
        # Changing 'value' to 'Value' should cause a checksum failure.
        contents = contents.replace(b'value', b'Value')
        data_file = self.open_data_file(mode='wb')
        data_file.write(contents)
        data_file.close()
        db = self.open_db_file(verify_checksums=True)
        with self.assertRaises(semidbm.DBMChecksumError):
            db['key']
        # If checksums are not enabled, an exception is not raised.
        db = self.open_db_file(verify_checksums=False)
        try:
            db['key']
        except semidbm.DBMChecksumError:
            self.fail("Checksums were suppose to be disabled.")

    def test_unicode_chars(self):
        db = semidbm.open(self.dbdir, 'c')
        # cafe with the e-accute.
        db[b'caf\xc3\xa9'] = b'caf\xc3\xa9'
        self.assertEqual(db[b'caf\xc3\xa9'], b'caf\xc3\xa9')


class TestReadOnlyModeMMapped(TestReadOnlyMode):
    def open_db_file(self, **kwargs):
        return semidbm.db._SemiDBMReadOnlyMMap(self.dbdir, **kwargs)

    def test_load_empty_db(self):
        db = semidbm.open(self.dbdir, 'c')
        db.close()
        empty_db = self.open_db_file()
        keys = empty_db.keys()
        empty_db.close()
        self.assertEqual(list(keys), [])


class TestWriteMode(SemiDBMTest):
    def test_when_index_file_does_not_exist(self):
        self.assertRaises(semidbm.DBMError, semidbm.open, self.dbdir, 'w')

    def test_when_data_file_does_not_exist(self):
        self.assertRaises(semidbm.DBMError, semidbm.open, self.dbdir, 'w')

    def test_when_files_exist(self):
        db = self.open_db_file()
        db['foo'] = 'bar'
        db.close()

        db_write_mode = semidbm.open(self.dbdir, 'w')
        self.assertEqual(db_write_mode['foo'], b'bar')
        db_write_mode.close()


class TestNewMode(SemiDBMTest):
    def test_when_file_does_not_exist(self):
        path = os.path.join(self.tempdir, 'foo.db')
        db = semidbm.open(path, 'n')
        db['foo'] = 'bar'
        self.assertEqual(db['foo'], b'bar')
        db.close()

        # Opening the file again should basically blank out
        # any existing database.
        db = semidbm.open(path, 'n')
        self.assertEqual(list(db.keys()), [])
        db.close()


class TestInvalidModeArgument(unittest.TestCase):
    def test_invalid_open_arg_raises_exception(self):
        self.assertRaises(ValueError, semidbm.open, 'foo.db', 'z')


class TestWindowsSemidbm(TestSemiDBM):
    def setUp(self):
        super(TestWindowsSemidbm, self).setUp()
        self.original_platform = sys.platform
        # The win32 specific code is compatible with posix platforms,
        # so these tests can run on mac/linux/etc. just fine.
        sys.platform = 'win32'

    def tearDown(self):
        super(TestWindowsSemidbm, self).tearDown()
        sys.platform = self.original_platform


class TestWithChecksumsOn(TestSemiDBM):
    def open_db_file(self, **kwargs):
        # If they do not explicitly set verify_checksums
        # to something, default to it being on.
        if 'verify_checksums' not in kwargs:
            kwargs['verify_checksums'] = True
        return semidbm.open(self.dbdir, 'c', **kwargs)


if __name__ == '__main__':
    unittest.main()
