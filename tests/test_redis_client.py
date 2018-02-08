#!/usr/bin/env python
"""Tests cache methods"""
import logging
import unittest
import sys


from serviceclients.cache.redis_cache import RedisCache


CACHE_CONN_PARAMS = {
    'read_host': 'localhost',
    'read_port': 6379,
    'write_host': 'localhost',
    'write_port': 6379
}


# Python2-3 compatibility
if sys.version_info > (3,):
    long = int


class TestCache(unittest.TestCase):

    def setUp(self):
        """Sets up before each test"""
        logging.debug('setting up TestCache')

    def tearDown(self):
        """Tears down after each test"""
        logging.debug('tearing down TestCache')

    def shortDescription(self):
        return None

    @classmethod
    def setup_class(cls):
        """setup_class() before any methods in this class, init class"""
        cls.kache = RedisCache(CACHE_CONN_PARAMS)
        cls.kache.write_connection()

    # Test start below

    def test_read_connection(self):
        read_conn = RedisCache(CACHE_CONN_PARAMS).read_connection()
        assert read_conn

    def test_write_connection(self):
        write_conn = RedisCache(CACHE_CONN_PARAMS).write_connection()
        assert write_conn

    def test_read_connection2(self):
        with self.assertRaises(TypeError):
            RedisCache(config=None).read_connection()

    def test_read_connection3(self):
        config = {
            'host': 'localhost',
            'port': 6379
        }
        with self.assertRaises(KeyError):
            RedisCache(config=config).read_connection()

    def test_write_connection2(self):
        with self.assertRaises(TypeError):
            RedisCache(config=None).write_connection()

    def test_write_connection3(self):
        config = {
            'host': 'localhost',
            'port': 6379
        }
        with self.assertRaises(KeyError):
            RedisCache(config=config).write_connection()

    def test_clear_cache_ns(self):
        count = self.kache.clear_cache_ns('test:events')
        assert isinstance(count, (int, long))

    def test_flush_all_cache(self):
        assert self.kache.flush_cache()

    def test_operations1(self):
        """
        Test set and get
        """
        key = 'test:123:123'
        self.kache.write_connection().set(key, 'test data')
        assert self.kache.write_connection().get(key)

    def test_operations2(self):
        """
        Test hset and hget
        """
        key = 'test:tickets:123:123'
        self.kache.write_connection().hset(key, 'ticket_data', '{"hello": "test", "_skip": 1}')
        assert self.kache.write_connection().hget(key, 'ticket_data')

if __name__ == '__main__':
    unittest.main()  # pragma: no cover
