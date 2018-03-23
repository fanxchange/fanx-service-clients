from __future__ import unicode_literals
"""
ElasticSearch connection wrapper around ElasticSearch
For mapping tickets by map worker and pipeline indexer
to create the index for mapping tickets to events
Wraps query connection errors and retries
"""

import sys
import logging
import time

from elasticsearch import Elasticsearch
from elasticsearch import serializer
from elasticsearch import exceptions as es_exceptions
from elasticsearch import helpers


# Try to get ujson if available
try:
    import ujson as json
except ImportError:
    import json

# Python2-3 compatibility. This should be here for a while, until no more Py2.
if sys.version_info > (3,):  # pragma: no cover
    basestring = str  # There is no long in Py3, just int


class UJSONSerializer(serializer.JSONSerializer):
    """
    Override ElasticSearch library serializer with ujson
    See original at:
    https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/serializer.py#L42
    """
    def dumps(self, data):
        # Don't serialize strings
        if isinstance(data, basestring):
            return data
        # Use ujson for performance
        try:
            return json.dumps(data)
        except (ValueError, TypeError) as e:  # pragma: no cover
            raise es_exceptions.SerializationError(data, e)


class ESClient:
    """
    ES class encapsulates ElasticSearch
    connections and queries
    """
    RECONNECT_SLEEP_SECS = 1  # In seconds. Timeout between re-connect
    # Number of query retries before throwing error
    RETRY_ATTEMPTS = 60  # In seconds. The last major TransportError lasted 40s
    # Default request timeout is 10s if not set.
    REQUEST_TIMEOUT = 15  # In seconds.

    def __init__(self, config=None, reconnect_sleep_secs=None, retry_attempts=None, timeout=None):
        """
        Load config from passed params or override with defaults
        :param config: list of dicts
        """
        self.config = config
        self.hosts = None
        self.connection = None

        # Overwrite default settings
        self.RECONNECT_SLEEP_SECS = reconnect_sleep_secs or self.RECONNECT_SLEEP_SECS
        self.RETRY_ATTEMPTS = retry_attempts or self.RETRY_ATTEMPTS
        self.REQUEST_TIMEOUT = timeout or self.REQUEST_TIMEOUT

        if self.config:
            self.hosts = [{'host': h['host'], 'port': h['port']} for h in self.config]
        else:
            self.hosts = [{'host': 'localhost'}]
        self.connect()

    def connect(self):
        """
        Establish ES connection
        """
        try:
            # Sniffing: The client can be configured to inspect the cluster state to get a list of nodes upon startup,
            # periodically and/or on failure. Getting TransportError(N/A, 'Unable to sniff hosts.')
            # Sniff on startup/fail inspect the cluster & load balance across nodes. sniffer_timeout sets interval
            # ConnectionPool dead_timeout is 60 seconds by default
            self.connection = Elasticsearch(self.hosts, serializer=UJSONSerializer(), sniff_on_connection_fail=True,
                                            retry_on_timeout=True, sniff_on_start=True, timeout=self.REQUEST_TIMEOUT)
        except Exception as e:  # pragma: no cover
            logging.error("ESClient.connect failed with params {}, error {}".format(self.config, e))

    def search(self, query, index_name, retries=0):
        """
        ES search query
        :param query: dict, es query
        :param index_name: str, index to query against
        :param retries: int, current retry attempt
        :return: dict, found doc status
        """
        resp = ''
        try:
            resp = self.connection.search(body=query, index=index_name)
            found = resp['hits']['hits']
        except KeyError:  # No hits key in response
            logging.critical("ESClient.search invalid response {}".format(resp))
            if retries > self.RETRY_ATTEMPTS:  # pragma: no cover
                logging.error("ESClient.search max attempts exceeded (key error)")
                raise
            found = self.search(query=query, index_name=index_name, retries=retries + 1)
        except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError,
                es_exceptions.TransportError):  # pragma: no cover
            logging.warning("ESClient.search connection failed, retrying...")  # Retry on timeout
            if retries > self.RETRY_ATTEMPTS:  # pragma: no cover
                logging.error("ESClient.search max attempts exceeded")
                raise
            time.sleep(self.RECONNECT_SLEEP_SECS)
            self.connect()  # Not sure if this is helpful
            found = self.search(query=query, index_name=index_name, retries=retries + 1)
        except Exception as e:  # pragma: no cover
            logging.critical("ESClient.search error {} on query {}".format(e, query))
            raise

        return found

    def msearch(self, queries, index_name, doc_type='event', retries=0, chunk_size=100):
        """
        Es multi-search query
        :param queries: list of dict, es queries
        :param index_name: str, index to query against
        :param doc_type: str, defined event type i.e. event
        :param retries: int, current retry attempt
        :param chunk_size: int, how many queries to send to es at a time
            Increase the search queue size before sending too many requests
            I.e. threadpool.search.queue_size: 50000  in es config
        :return: dict, found doc status
        """
        search_header = json.dumps({'index': index_name, 'type': doc_type})

        def chunk_queries():
            for i in range(0, len(queries), chunk_size):
                yield queries[i:i + chunk_size]

        chunked_queries = chunk_queries()

        found = []
        for query_chunk in chunked_queries:
            request = ''
            for q in query_chunk:
                # request head, body pairs
                request += '{}\n{}\n'.format(search_header, json.dumps(q))

            resp = {}
            try:
                resp = self.connection.msearch(body=request, index=index_name)
                found.extend([r['hits']['hits'] for r in resp['responses']])
            except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError,
                    es_exceptions.TransportError, KeyError) as e:  # pragma: no cover
                if retries > self.RETRY_ATTEMPTS:  # pragma: no cover
                    logging.error("ESClient.msearch max attempts exceeded, error {}".format(e))
                    raise

                logging.warning("ESClient.msearch connection failed, retrying...")  # Retry on timeout

                # No hits key in response, don't retry if es_rejected_execution_exception
                if e.__class__ == KeyError:
                    # 'hits' missing, could be es_rejected_execution_exception, queue capacity reached
                    logging.critical("ESClient.msearch invalid response {}".format(resp.get('responses')))
                    # if 'search_phase_execution_exception' not in str(resp):  # reason 'all shards failed'
                    if 'es_rejected_execution_exception':
                        # raise if underlying error is ConnectionRefusedError in urllib3 caused by NewConnectionError
                        logging.error("ESClient.msearch query rejected, error {}".format(e))
                        raise

                time.sleep(self.RECONNECT_SLEEP_SECS)
                self.connect()  # Not sure if useful
                found = self.msearch(queries=queries, index_name=index_name, retries=retries + 1)
            except Exception as e:  # pragma: no cover
                logging.critical("ESClient.msearch error {} on query {}".format(e, queries))
                raise

        return found

    def delete_index(self, index_name):
        """
        Delete an index by name
        :param index_name: str, index name
        :return: dict, removed status
        """
        result = None
        for attempt in range(1, self.RETRY_ATTEMPTS + 1):
            try:
                result = self.connection.indices.delete(index=index_name)
                break
            except es_exceptions.NotFoundError:  # pragma: no cover
                result = False
                break
            except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError):  # pragma: no cover
                logging.warning("ESClient.delete_index connection timeout")  # Retry on timeout
                self.connect()  # Not sure if this is helpful, or connection is lazy?
                continue

        if not result:  # pragma: no cover
            logging.warning("ESClient.delete_index failed for {}".format(index_name))
        return result

    def create_index(self, index_name, body=None, replace=False):
        """
        Creates an index by name, populate with body
        :param index_name: str, name of index
        :param body: dict, optional document to create
        :param replace: bool, force replace existing index
        :return: dict, created status info
        """
        result = None
        for attempt in range(1, self.RETRY_ATTEMPTS + 1):
            try:
                result = self.connection.indices.create(index=index_name, ignore=400, body=body)
                result = bool('acknowledged' in result)
                break
            except es_exceptions.AuthorizationException:  # pragma: no cover
                result = False
                break
            except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError):  # pragma: no cover
                logging.warning("ESClient.create_index connection timeout")  # Retry on timeout
                self.connect()  # Not sure if this is helpful, or connection is lazy?
                continue

        if replace and not result:
            logging.warning("ESClient.create_index replacing existing index {}".format(index_name))
            self.delete_index(index_name)
            result = self.connection.indices.create(index=index_name, ignore=400, body=body)

        if result:
            self.connection.indices.refresh(index_name)
        return result

    def add_alias(self, indexes, alias_name, retries=0):
        """
        Set the alias current for new index
        Note: It is possible to have one alias for multiple
        indexes but bulk populate will fail for that alias
        :param indexes: list (or single str) of index names
        :param alias_name: str, alias to use for the index
        :param retries: int, number of retries of the function
        :return: dict, added info
        """
        try:
            added = self.connection.indices.put_alias(index=indexes, name=alias_name)
        except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError):  # pragma: no cover
            logging.warning("ESClient.add_alias connection failed, retrying...")  # Retry on timeout
            if retries > self.RETRY_ATTEMPTS:  # pragma: no cover
                raise
            time.sleep(self.RECONNECT_SLEEP_SECS)
            added = self.get_alias(indexes, alias_name, retries=retries + 1)
        return added

    def get_alias(self, alias_name=None, index_name=None, retries=0):
        """
        Return alias information i.e indexes either by
        alias name or index to get aliases for an index
        :param alias_name: str, alias to use for the index
        :param index_name: str, name of index
        :param retries: int, number of retries of the function
        :return:
        """
        try:
            alias = self.connection.indices.get_alias(name=alias_name, index=index_name)
        except es_exceptions.NotFoundError:  # pragma: no cover
            alias = None
        except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError):  # pragma: no cover
            logging.warning("ESClient.get_alias connection failed, retrying...")  # Retry on timeout
            if retries > self.RETRY_ATTEMPTS:  # pragma: no cover
                raise
            time.sleep(self.RECONNECT_SLEEP_SECS)
            alias = self.get_alias(alias_name, index_name, retries=retries + 1)
        return alias

    def delete_alias(self, index_name, alias_name, retries=0):
        """
        Removes alias
        :param index_name: str, index name
        :param alias_name: str, alias to use for the index
        :param retries: int, number of retries of the function
        :return: dict, removed status
        """
        try:
            removed = self.connection.indices.delete_alias(name=alias_name, index=index_name)
        except es_exceptions.NotFoundError:  # pragma: no cover
            return False
        except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError):  # pragma: no cover
            logging.warning("ESClient.delete_alias connection failed, retrying...")  # Retry on timeout
            if retries > self.RETRY_ATTEMPTS:  # pragma: no cover
                raise
            time.sleep(self.RECONNECT_SLEEP_SECS)
            removed = self.delete_alias(index_name, alias_name, retries=retries + 1)
        return removed

    def upsert_doc(self, doc_id, body, index_name, doc_type='event', retries=0):
        """
        Upsert a document into an es index, specifically
        made for upserting an event to pipeline indexer
        Will be used to add/update events directly in index
        :param index_name: str, index name
        :param doc_id: int, event id
        :param body: dict, event doc
        :param doc_type: str, event time i.e. event
        :param retries: int, number of retries of the function
        :return: dict, result
        """
        try:
            result = self.connection.update(index=index_name,
                                            doc_type=doc_type,
                                            id=doc_id,
                                            body={"doc": body, 'doc_as_upsert': True})
        except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError):  # pragma: no cover
            logging.warning("ESClient.upsert connection failed, retrying...")  # Retry on timeout
            if retries > self.RETRY_ATTEMPTS:  # pragma: no cover
                raise
            time.sleep(self.RECONNECT_SLEEP_SECS)
            result = self.upsert_doc(doc_id, body, index_name, doc_type, retries=retries + 1)

        return result

    def remove_doc(self, index_name, doc_id, doc_type='event', retries=0):  # pragma: no cover
        """
        Remove a document from es index
        Will be used to remove events directly from index
        :param index_name: str, index name
        :param doc_id: int, event id
        :param doc_type: str, defined event type i.e. event
        :param retries: int, number of retries of the function
        :return: dict, result
        """
        try:
            result = self.connection.delete(index=index_name, doc_type=doc_type, id=doc_id)
        except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError):  # pragma: no cover
            logging.warning("ESClient.remove connection failed, retrying...")  # Retry on timeout
            if retries > self.RETRY_ATTEMPTS:  # pragma: no cover
                raise
            time.sleep(self.RECONNECT_SLEEP_SECS)
            result = self.remove_doc(index_name, doc_id, doc_type, retries=retries + 1)

        return result

    def setup_index(self, index_name, index_settings, doc_mapping):
        """
        Setup Index
        :param index_name: str, index name
        :param index_settings: str or dict, index settings document
        :param doc_mapping: str or dict, index doc mapping schema
        :return: bool, setup settings and index success
        """
        settings = mapped = None
        for attempt in range(1, self.RETRY_ATTEMPTS + 1):
            try:
                # close index to modify settings
                self.connection.indices.close(index=index_name)
                # Creates es analyzer, filter settings
                settings = self.connection.indices.put_settings(index=index_name, body=index_settings)
                self.connection.indices.open(index=index_name)

                # Sets up document structure / mapping
                mapped = self.connection.indices.put_mapping(index=index_name, doc_type='event', body=doc_mapping)
                break
            except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError):  # pragma: no cover
                logging.warning("ESClient.setup_index connection timeout")  # Retry on timeout
                self.connect()  # Not sure if this is helpful, or connection is lazy?
                continue

        return settings and mapped

    def bulk_update_event_index(self, data, index_name, event_layouts=None):
        """
        Bulk populates the es index with event data
        Can also be used to add a single doc to index
        :param index_name: str, index name
        :param data: list, of dicts events
        :param event_layouts: dict, event layouts from canon
        :return: bool, success
        """
        bulk_data = []
        event_layouts = event_layouts or {}

        def combine_names(name, name_list):
            all_names = [name, ]
            if name_list:
                all_names = all_names + name_list.split(',')
            return filter(None, all_names)

        for d in data:
            venue_names = combine_names(d['venue_name'], d['venue_alt_names'])
            event_names = combine_names(d['event_name'], d['event_alt_names'])

            # Fill in event name as performers for those events missing the value
            if not d['performers']:
                    d['performers'] = d['event_name']

            # Clean performers
            performers = ''
            if d['performers']:
                performers = ','.join(x.strip() for x in d['performers'].split(','))

            doc = {
                'local_date': str(d.get('event_date', '')),  # TODO: Deprecated
                'iso_event_date': d['iso_event_date'],
                'local_time': d['event_time'],
                'venue_id': d['venue_id'],
                'layout_id': event_layouts.get(d['event_id']),
                'event_status': d['event_status'],
                'venue_name': venue_names,
                'name': event_names,
                'event_id': d['event_id'],
                'performers': performers,
                'taxonomy': d['taxonomy'],
            }

            jsoned_event = json.dumps(doc)

            action = {
                '_index': index_name,
                '_type': 'event',
                '_source': jsoned_event,
                '_id': d['event_id']
            }

            bulk_data.append(action)

        success = False
        for attempt in range(1, self.RETRY_ATTEMPTS + 1):
            try:
                helpers.bulk(self.connection, bulk_data)
                self.connection.indices.refresh(index=index_name)
                success = True
                break
            except (es_exceptions.ConnectionTimeout, es_exceptions.ConnectionError):  # pragma: no cover
                logging.warning("ESClient.bulk_update_event_index connection timeout")  # Retry on timeout
                self.connect()  # Not sure if this is helpful, or connection is lazy?
                continue

        return success
