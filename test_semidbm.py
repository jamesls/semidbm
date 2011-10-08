#!/usr/bin/env python

import os
import unittest
import tempfile
import StringIO

import semidbm


class TestSemiDBM(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix='semidbm_ut')

    def tearDown(self):
        for f in os.listdir(self.tempdir):
            os.remove(os.path.join(self.tempdir, f))
        os.rmdir(self.tempdir)

    def open_db_file(self):
        return semidbm.open(os.path.join(self.tempdir,
                                         'myfile.db'), 'c')

    def testInsertThenRetrieve(self):
        db = self.open_db_file()
        db['foo'] = 'bar'
        self.assertEqual(db['foo'], 'bar')

    def testInsertCloseRetrieve(self):
        # This will verify loading the index.
        db = self.open_db_file()
        db['foo'] = 'bar'
        db.close()

        db2 = self.open_db_file()
        self.assertEqual(db2['foo'], 'bar')


if __name__ == '__main__':
    unittest.main()
