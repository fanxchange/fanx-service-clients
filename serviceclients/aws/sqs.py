from __future__ import unicode_literals
"""
SQS feed queue core class
"""
import logging
import os
import time

from boto.sqs import connect_to_region
from boto.sqs.message import RawMessage

# Try to get ujson if available
try:
    import ujson as json
except ImportError:
    import json


class SQSClient:
    """
    SQS class encapsulates queue operations,
    This is not covered in unit test test coverage,
    but in integration tests since its an external process
    """

    # Request timeout to poll for msg, must be 0 to 20
    SQS_LONG_POLL_SECONDS = 20
    # Make message invisible to other consumers. Defaults via SQS to 30
    SQS_MSG_INVISIBLE_SECONDS = 14

    RECONNECT_SLEEP_SECS = 0.5
    CONN_RETRIES = 20

    def __init__(self, config, sqs_long_poll_seconds=SQS_LONG_POLL_SECONDS,
                 sqs_msg_invisible_seconds=SQS_MSG_INVISIBLE_SECONDS, reconnect_sleep_secs=RECONNECT_SLEEP_SECS,
                 conn_retries=CONN_RETRIES):
        """
        Load config from passed params or override with defaults
        :param config: dict with access_key_id, secret_access_key, bucket name
        :return: None
        """
        # Load from passed params or override with defaults
        try:
            self.config = config
            self.access_key_id = self.config['access_key_id']
            self.secret_access_key = self.config['secret_access_key']
            self.queue_region = self.config['queue_region']
            self.queue_name = self.config['queue_name']

            self.SQS_LONG_POLL_SECONDS = sqs_long_poll_seconds
            self.SQS_MSG_INVISIBLE_SECONDS = sqs_msg_invisible_seconds
            self.RECONNECT_SLEEP_SECS = reconnect_sleep_secs
            self.CONN_RETRIES = conn_retries
        except Exception as e:
            logging.exception("SQSClient.__init__ configuration error {}".format(e))
            self.access_key_id = None
            self.secret_access_key = None
            self.queue_region = None
            self.queue_name = None
            self.config = None

        self.connection_attempt = 0
        self.connection = None
        self.queue = None
        self.connect()

    def connect(self):
        """
        Establish SQS connection
        """
        try:
            self.connection = connect_to_region(region_name=self.queue_region,
                                                aws_access_key_id=self.access_key_id,
                                                aws_secret_access_key=self.secret_access_key)
            self._get_queue()
            self.connection_attempt = 0  # Got queue connection, reset retries
        except Exception as e:
            logging.exception("SQSClient.connect failed with params {}, error {}".format(self.config, e))
            self.connection_attempt += 1
            if self.connection_attempt >= self.CONN_RETRIES:
                raise

    def _get_queue(self):  # pragma: no cover
        """
        Get SQS queue connection
        SQS message contains broker info and file path in S3.
        """
        try:
            self.queue = self.connection.get_queue(self.queue_name)
            # Set getting of message body in raw format
            self.queue.set_message_class(RawMessage)
        except Exception as e:
            # Can throw SQSError
            logging.exception("SQSClient._get_queue unable to get queue '{}', region '{}', error {}"
                              "".format(self.queue_name, self.queue_region, e))
            raise

    def parse_message(self, message):
        """
        Parse a single SQS message to get the
        file path, broker and body from it
        :param message: sqs message object, single message
        :return: tuple(str s3 file key, str broker_id, int file_timestamp)
        """
        try:
            body = json.loads(message.get_body())
            # Example key: home/fanx.ftp/1566/1566_mytickets.txt-1490066686
            s3_file_path = body[u'Records'][0][u's3'][u'object'][u'key']
            broker_id = os.path.split(s3_file_path)[1].split('_')[0]
        except Exception as e:
            logging.error("SQSClient.parse_message error {}, message '{}', deleting".format(e, message.get_body()))
            self.delete_message(message)
            raise
        return s3_file_path, broker_id

    def get_messages(self, num_messages=1, visibility_timeout=SQS_MSG_INVISIBLE_SECONDS,
                     wait_time_seconds=SQS_LONG_POLL_SECONDS):
        """
        Get messages from sqs feed queue
        :return: list, of sqs messages object
        """
        try:
            # Long polling for a message from SQS (list of 1 message)
            sqs_messages = self.queue.get_messages(num_messages=num_messages,
                                                   visibility_timeout=visibility_timeout,
                                                   wait_time_seconds=wait_time_seconds)
        except Exception as e:
            # I.e. gaierror: [Errno -2] Name or service not known
            logging.exception("SQSClient.get_messages error, retrying. {}".format(e))
            time.sleep(self.RECONNECT_SLEEP_SECS)
            self.connect()
            if self.connection_attempt >= self.CONN_RETRIES:
                raise
            sqs_messages = self.get_messages()

        return sqs_messages

    def delete_message(self, sqs_message):
        """
        Delete an sqs msg
        :param sqs_message: sqs message object
        :return:
        """
        try:
            self.queue.delete_message(sqs_message)
        except AttributeError:
            # Message was already deleted
            pass
        except Exception as e:
            logging.exception("SQSClient.delete_message error, retrying. {}".format(e))
            time.sleep(self.RECONNECT_SLEEP_SECS)
            self.connect()
            if self.connection_attempt >= self.CONN_RETRIES:
                raise
            self.delete_message(sqs_message)

        return True

    def send_message(self, body):
        """
        For testing only, send a message
        :param body: str, message_content
        :return: bool, success
        """
        return self.connection.send_message(self.queue, body)
