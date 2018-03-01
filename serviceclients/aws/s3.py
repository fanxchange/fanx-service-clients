from __future__ import unicode_literals
"""
S3 bucket CRUD operations core module
Based on https://github.com/krux/python-krux-boto-s3/blob/master/krux_s3/s3.py
"""
import logging
import time
from difflib import SequenceMatcher

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.lifecycle import Lifecycle, Transition, Rule


class S3Client:  # pragma: no cover
    """
    S3 class encapsulates uploading,
    downloading & other s3 file ops and handling errors
    This is not covered in unit test test coverage,
    but in integration tests since its an external process
    """
    S3_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.000Z'

    RECONNECT_SLEEP_SECS = 0.5
    CONN_RETRIES = 10

    def __init__(self, config, reconnect_sleep_secs=RECONNECT_SLEEP_SECS, conn_retries=CONN_RETRIES):
        """
        Load config from passed params or override with defaults
        :param config: dict, config with access_key_id, secret_access_key, bucket name
        :return: None
        """
        self.config = config
        self.bucket_name = self.config['bucket_name']
        self.access_key_id = self.config['access_key_id']
        self.secret_access_key = self.config['secret_access_key']

        self.RECONNECT_SLEEP_SECS = reconnect_sleep_secs
        self.CONN_RETRIES = conn_retries

        self.connection_attempt = 0
        self.connection = None
        self.bucket = None
        self.connect()

    def connect(self):
        """
        Creates object connection to the designated region (self.boto.cli_region).
        The connection is established on the first call for this instance (lazy) and cached.
        :return: None
        """
        try:
            self.connection_attempt += 1
            self.connection = S3Connection(self.access_key_id, self.secret_access_key)
            self._get_bucket()
        except Exception as e:
            logging.exception("S3Client.connect failed with params {}, error {}".format(self.config, e))
            if self.connection_attempt >= self.CONN_RETRIES:
                raise

    def _get_bucket(self):
        """
        Uses S3 Connection and return connection to queue
        S3 used for getting the listing file in the SQS message
        :return: None
        """
        try:
            self.bucket = self.connection.get_bucket(self.bucket_name)
        except Exception as e:
            # I.e. gaierror: [Errno -2] Name or service not known
            logging.exception("S3Client.get_bucket unable to get bucket {}, error {}".format(self.bucket_name, e))
            raise

    def read(self, key):
        """
        Get bucket key value, return contents
        Get contents of a file from S3
        :param key: str, bucket key filename
        :return: str, contents of key
        """
        try:
            k = Key(self.bucket)
            k.key = key
            contents = k.get_contents_as_string()
        except Exception as e:  # Retry in-case we have a connection error
            logging.exception("S3Client.read failed for key {}, error {}".format(key, e))
            time.sleep(self.RECONNECT_SLEEP_SECS)
            self.connect()
            contents = self.read(key)

        return contents

    def write(self, content, key):
        """
        Create bucket key from string
        Write content to a file in S3
        :param content: str, contents to save to a file
        :param key: str, bucket key filename
        :return: dict, output
        """
        output = None
        try:
            k = Key(self.bucket)
            k.key = key
            output = {
                'file_name': key,
                'is_new': not k.exists(),
            }
            response = k.set_contents_from_string(content)
            if not response:
                logging.error("S3Client.write {} to S3 failed".format(key))
        except Exception as e:
            logging.exception("S3Client.write failed for key {}, error {}".format(key, e))

        return output

    def upload(self, key, origin):
        """
        Create bucket key from filename
        Upload a file to S3 from origin file
        :param origin: str, path to origin filename
        :param key: str, bucket key filename
        :return: bool, success
        """
        try:
            k = Key(self.bucket)
            k.key = key
            if not k.set_contents_from_filename(origin):
                logging.error("S3Client.upload {} to {} failed".format(origin, key))
        except Exception as e:
            logging.exception("S3Client.upload failed for key {}, error {} ".format(key, e))

        return True

    def download(self, key, destination):
        """
        Get key
        Download a file from S3 to destination
        :param destination: str, path to local file name
        :param key: str, bucket key filename
        :return: bool, success
        """
        key_obj = None
        result = True
        try:
            # Get key, download contents. k = Key(self.bucket); k.key = key
            # Can get gaierror: [Errno -2] Name or service not known
            key_obj = self.bucket.get_key(key)
            # Can get error: [Errno 104] Connection reset by peer (most common)
            key_obj.get_contents_to_filename(destination)
        except Exception as e:
            logging.warning("S3Client.download failed for key {} to {}, error {}, retrying".format(key, destination, e))
            if not key_obj:  # Most likely can't find the key in the bucket
                logging.critical("S3Client.download bucket missing key file {}".format(key))
                raise
            time.sleep(self.RECONNECT_SLEEP_SECS)
            self.connect()
            result = self.download(key, destination)

        del key_obj
        return result

    def set_transition_to_glacier(self, days, prefix=''):
        """
        Set rules when the files should be moved
        to Amazon Glacier for archiving
        This method must be called before write/upload methods
        Not used at the time, but could be for archiving s3 broker files
        :param prefix: str, prefix
        :param days: int, num of days
        :return: None
        """
        try:
            to_glacier = Transition(days=days, storage_class='GLACIER')
            rule = Rule(id='ruleid', prefix=prefix, status='Enabled', transition=to_glacier)
            lifecycle = Lifecycle()
            lifecycle.append(rule)
            self.bucket.configure_lifecycle(lifecycle)
        except Exception as e:
            logging.exception("S3Client.set_transition_to_glacier failed for bucket {}, error {}"
                              "".format(self.bucket_name, e))

    def remove(self, keys):
        """
        Deletes the given keys from the given bucket.
        :param keys: list, list of key names
        :return: bool, success
        """
        logging.warning("S3Client.remove deleting keys {}".format(keys))
        self.bucket.delete_keys(keys)
        return True

    @staticmethod
    def get_timestamp_prefix(past_hours=0.5, before_timestamp=None):
        """
        Gets a partial timestamp prefix used as a
        wildcard to help find
        partial timestamp string i.e. 11112222 and 11113333 have 1111xxxx in common
        :param past_hours: float, range to search forward from
        :param before_timestamp: int, minimum or start timestamp (default now)
        :return: str, partial wildcard timestamp
        """
        now = int(before_timestamp or time.time())
        then = str(now - 3600 * past_hours)
        now = str(now)
        match = SequenceMatcher(None, then, now).find_longest_match(0, len(then), 0, len(now))
        return now[match.b: match.b + match.size]
