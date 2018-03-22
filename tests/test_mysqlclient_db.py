#!/usr/bin/env python
"""Tests database methods"""
import logging
import unittest
import sys


from serviceclients.database.mysqlclient_db import DBClient


DB_CONN_PARAMS = {
    'read_host': 'localhost',
    'read_port': 3306,
    'read_username': 'root',
    'read_password': 'root',
    'db_name': 'test',
    'write_host': 'localhost',
    'write_port': 3306,
    'write_username': 'root',
    'write_password': 'root'
}

# Python2-3 compatibility
if sys.version_info > (3,):
    long = int  # There is no long in Py3, just int


class TestDatabase(unittest.TestCase):
    def setUp(self):
        """Sets up before each test"""
        logging.debug('setting up TestDatabase')

    def tearDown(self):
        """Tears down after each test"""
        logging.debug('tearing down TestDatabase')

    def shortDescription(self):
        return None

    @classmethod
    def setup_class(cls):
        """setup_class() before any methods in this class, init class"""
        cls.db_config = DB_CONN_PARAMS

        cls.db = DBClient(config=cls.db_config)

    # Test start below

    def test_read_connection1(self):
        with self.assertRaises((TypeError, Exception)):
            DBClient().read_connection()

    def test_read_connection2(self):
        with self.assertRaises((TypeError, Exception)):
            DBClient(config=None).read_connection()

    def test_read_connection3(self):
        """
        Test opening a dirty reads connection
        """
        read_conn = DBClient(self.db_config, dirty_reads=True).read_connection()
        assert read_conn

    def test_read_connection4(self):
        try:
            config = {
                'read_host': 'localhost',
                'read_port': 6379,
                'db_name': ''
            }
            read_conn = DBClient(config=config).read_connection()
        except (TypeError, KeyError):
            read_conn = None
        assert not read_conn

    def test_write_connection1(self):
        with self.assertRaises((TypeError, Exception)):
            DBClient().write_connection()

    def test_write_connection2(self):
        with self.assertRaises((TypeError, Exception)):
            DBClient(config=None).write_connection()

    def test_write_connection3(self):
        try:
            config = {
                'host': 'localhost',
                'port': 6379
            }
            write_conn = DBClient(config=config).write_connection()
        except (TypeError, KeyError, AttributeError):
            write_conn = None
        assert not write_conn

    def test_escape_string1(self):
        """
        Only applies to Python2, unicode does not get
        quoted if escape_string does not cast to str()

        If no cast made:
        test_escape_string1 (test_database.TestDatabase) ... <type 'unicode'>
        hello there <type 'str'>
        ok
        test_escape_string2 (test_database.TestDatabase) ... <type 'str'>
        'hello there' <type 'str'>
        """
        dirty_string = u'hello there'
        self.db.read_connection()  # Connect for escape_string
        quoted_string = self.db.escape_string(dirty_string)
        # Will be unicode in Py2, which is just str in Py3
        # assert isinstance(quoted_string, str), "Got {}".format(type(quoted_string))
        assert quoted_string == "'hello there'"

    def test_escape_string2(self):
        """
        Regular string escaping
        """
        dirty_string = 'hello there; AUTOCOMMIT=False;'
        self.db.read_connection()  # Connect for escape_string
        quoted_string = self.db.escape_string(dirty_string)
        # Will be unicode in Py2, which is just str in Py3
        # assert isinstance(quoted_string, str), "Got {}".format(type(quoted_string))
        assert quoted_string == "'hello there; AUTOCOMMIT=False;'"

if __name__ == '__main__':
    unittest.main()  # pragma: no cover
