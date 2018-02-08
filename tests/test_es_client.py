#!/usr/bin/env python
"""Tests search connection methods"""
import logging
import unittest

from serviceclients.search.es import ESClient

ES_CONN_PARAMS = [{'host': 'localhost', 'port': 9200, 'timeout': 1}]

TEST_ES_INDEX = 'test_es_index'


class TestESClient(unittest.TestCase):

    def setUp(self):
        """Sets up before each test"""
        logging.debug('setting up TestES')

    def tearDown(self):
        """Tears down after each test"""
        logging.debug('tearing down TestES')

    def shortDescription(self):
        return None

    @classmethod
    def setup_class(cls):
        """setup_class() before any methods in this class, init class"""
        cls.es_client = ESClient(ES_CONN_PARAMS)

    # Test start below

    def test_connection(self):
        es_client = ESClient()  # Should gt default config
        es_conn = es_client.connection
        assert es_conn

    def test_connection2(self):
        es_client = ESClient(config=None)
        es_conn = es_client.connection
        assert es_conn

    def test_connection3(self):
        # Bad config file, not a list
        config = {
            'host': 'localhost',
            'port': 9200
        }
        with self.assertRaises(TypeError):
            ESClient(config=config)

    def test_connection4(self):
        config = [{
            'host': 'localhost',
            'port': 9200
        }]
        es_client = ESClient(config=config)
        es_conn = es_client.connection
        assert es_conn

    def test_connection5(self):
        es_client = ESClient(config=ES_CONN_PARAMS)
        es_conn = es_client.connection
        assert es_conn

    def test_search1(self):
        """
        Single query search
        """
        self.es_client.create_index(TEST_ES_INDEX)
        q = """
        {
            "min_score": 2.0,
            "track_scores": true,
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "venue_name": {
                                    "query": "dodger stadium",
                                    "operator": "and"
                                }
                            }
                        },
                        {
                            "bool": {
                                "should": [
                                    {
                                        "match": {
                                            "name": {
                                                "query": "ironman",
                                                "minimum_should_match": "33%",
                                                "fuzziness": "AUTO"
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        }
        """
        assert isinstance(self.es_client.search(q, index_name=TEST_ES_INDEX), list)

    def test_msearch1(self):
        """
        Test multi-search
        """
        self.es_client.create_index(TEST_ES_INDEX)

        # Multiple queries
        queries = []
        queries.append(
            {"min_score": 2.0, "query": {"bool": {"should": [{"match": {"name": {"query": "batman"}}}]}}}
        )
        queries.append(
            {"min_score": 1.0, "query": {"bool": {"should": [{"match": {"name": {"query": "ironman"}}}]}}}
        )
        queries.append(
            {"track_scores": True, "min_score": 9.0, "query":
                {"bool": {"should": [{"match": {"name": {"query": "not-findable"}}}]}}}
        )
        q_results = self.es_client.msearch(queries, index_name=TEST_ES_INDEX, doc_type='event')

        assert len(q_results) == 3

    def test_upsert1(self):
        """
        Test inserting doc, then updating it
        """
        doc_id = 1
        event = {
            'event_id': doc_id,
            'event_name': 'Ryder Cup Golf',
            'event_alt_names': '',
            'event_date': '2017-12-12 22:00:00',
            'event_time': '10:00 pm',
            'venue_name': 'Hazeltine National Golf Club',
        }
        self.es_client.create_index(TEST_ES_INDEX)

        # Insert
        update = self.es_client.upsert_doc(doc_id, event, index_name=TEST_ES_INDEX)
        assert isinstance(update, dict)
        assert int(update['_id']) == doc_id, "Got {}".format(doc_id)

        # Update
        event['event_name'] = 'Ryder Cup Golf Test'
        update = self.es_client.upsert_doc(doc_id, event, index_name=TEST_ES_INDEX)
        assert isinstance(update, dict)
        assert int(update['_id']) == doc_id, "Got {}".format(doc_id)

    def test_remove1(self):
        doc_id = 1
        event = {
            'event_id': doc_id,
            'event_name': 'Ryder Cup Golf',
            'event_date': '2017-12-12 22:00:00',
            'event_time': '10:00 pm',
            'venue_name': 'Hazeltine National Golf Club',
        }
        test_index_name = TEST_ES_INDEX
        self.es_client.create_index(test_index_name)
        self.es_client.upsert_doc(doc_id, event, index_name=test_index_name)

        remove = self.es_client.remove_doc(test_index_name, doc_id=doc_id)
        assert isinstance(remove, dict)
        assert int(remove['_id']) == doc_id, "Got {}".format(doc_id)

    def test_add_remove_alias1(self):
        """
        Add as a list, multi-indexes
        """
        self.es_client.create_index(TEST_ES_INDEX)
        alias = 'test_alias1'
        self.es_client.delete_alias(index_name=TEST_ES_INDEX, alias_name=alias)

        result = self.es_client.add_alias(indexes=[TEST_ES_INDEX, ], alias_name=alias)
        assert result['acknowledged']

        result = self.es_client.delete_alias(index_name=TEST_ES_INDEX, alias_name=alias)
        assert result['acknowledged']

    def test_add_remove_alias2(self):
        """
        Add as an alias for single index
        """
        alias = 'test_alias1'
        self.es_client.delete_alias(index_name=TEST_ES_INDEX, alias_name=alias)

        result = self.es_client.add_alias(indexes=TEST_ES_INDEX, alias_name=alias)
        assert result['acknowledged']

        result = self.es_client.delete_alias(index_name=TEST_ES_INDEX, alias_name=alias)
        assert result['acknowledged']

    def test_create_delete_index1(self):
        assert isinstance(self.es_client.create_index(TEST_ES_INDEX), bool)  # Could be created, or already exists
        result = self.es_client.delete_index(TEST_ES_INDEX)
        assert result['acknowledged']

    def test_bulk_update_event_index1(self):
        # Add superbowl event to indexer
        super_bowl_event_id = 145565427
        super_bowl_event = {
            'event_id': super_bowl_event_id,
            'event_name': 'super bowl lii ::02/04/2018:5:30pm:None',
            'event_date': '2018-02-04 17:30:00',
            'event_time': '5:30PM',
            'venue_name': 'us bank stadium',
        }
        assert self.es_client.bulk_update_event_index(data=[super_bowl_event, ],
                                                      index_name='test_alias')

    def test_setup_index1(self):
        index = 'test_es_setup_index'
        self.es_client.delete_index(index)
        self.es_client.create_index(index)
        settings = {
            'settings': {
                'index': {
                    'index_concurrency': 8,

                }
            }
        }
        mappings = {
            "event": {
                "properties": {
                    "field1": {"type": "string"}
                }
            }
        }
        assert self.es_client.setup_index(index, settings, mappings) == {u'acknowledged': True}

    def test_create_index1(self):
        index_name = 'test_es_setup_index'
        success = self.es_client.create_index(index_name)
        assert isinstance(success, bool), "Got {}".format(type(success))

    def test_create_index2(self):
        index_name = 'test_es_setup_index'
        assert self.es_client.create_index(index_name, replace=True)

    def test_get_alias1(self):
        index_name = 'test_es_setup_index'
        assert not self.es_client.get_alias(index_name=index_name)[index_name]['aliases']

    def test_get_alias2(self):
        alias_name = 'test_es_setup_non_existent'
        assert not self.es_client.get_alias(alias_name=alias_name)


if __name__ == '__main__':
    unittest.main()  # pragma: no cover
