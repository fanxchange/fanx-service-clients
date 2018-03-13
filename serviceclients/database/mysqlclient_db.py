from __future__ import unicode_literals
"""
MySQL database service functions using mysqlclient
Best performing client for CPython
Works on pypy but slower than PyMySQL
"""
import logging
import time

import MySQLdb.cursors
import MySQLdb


class DBClient(object):
    """
    DB Class used to connect to mysql database
    """

    WRITE_RETRY_WAIT_SECS = 0.5
    WRITE_RETRY_ATTEMPTS = 100
    DIRTY_READS = False

    def __init__(self, config=None, dirty_reads=DIRTY_READS, write_retry_attempts=WRITE_RETRY_ATTEMPTS):
        """
        Load defaults by passing config keyword
        :param config: dict, config
        :param dirty_reads: bool, enable dirty read
        :param write_retry_attempts: int, num of write query retry attempts before raise
        :return: None
        """
        self.config = config

        self.WRITE_RETRY_ATTEMPTS = write_retry_attempts
        self.DIRTY_READS = dirty_reads

        self.read_db = self.write_db = None

    def read_connection(self):
        """
        Returns read connection
        Sets one up if not already configured
        :return: read conn
        """
        if not self.is_connection_open(self.read_db):
            try:
                self.read_db = self._connect(self.config['read_username'], self.config['read_password'],
                                             self.config['read_host'], self.config['read_port'],
                                             self.config['db_name'])
                if self.DIRTY_READS:  # Enable dirty reads on current connection
                    with self.read_db.cursor() as cursor:
                        cursor.execute('SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')
            except Exception as e:
                logging.exception("DBClient.read_connection unhandled exception {}".format(e))
                raise

        return self.read_db

    def write_connection(self):
        """
        Returns write connection
        Sets one up if not already configured
        :return: read conn
        """
        if not self.is_connection_open(self.write_db):
            try:
                self.write_db = self._connect(self.config['write_username'], self.config['write_password'],
                                              self.config['write_host'], self.config['write_port'],
                                              self.config['db_name'])
                # SET autocommit for current session, although Autocommit is already on by default in _connect
                # write_cursor.execute("""SET autocommit = 1""")
                # if autocommit ...
            except Exception as e:
                logging.exception("DBClient.write_connection unhandled exception {}".format(e))
                raise

        return self.write_db

    @staticmethod
    def _connect(username, password, host, port, db_name, cursor_type=None):
        """
        Connect function used by read write conn
        :param username: str, username
        :param password: str, pass
        :param host: str, host
        :param port: int, port
        :param db_name: str, db name
        :param cursor_type: obj, optional cursor type
        :return: boj, connection
        """
        conn = None
        cursor_type = cursor_type or MySQLdb.cursors.DictCursor

        try:
            # autocommit - Can set autocommit=True, but without it will use server default. Seems we need it.
            # charset - If supplied, the conn character set will be changed to it. Implies use_unicode=True
            conn = MySQLdb.connect(host=host,
                                   port=port,
                                   user=username,
                                   password=password,
                                   db=db_name,
                                   charset='utf8mb4',
                                   cursorclass=cursor_type,
                                   autocommit=True)

        except MySQLdb.OperationalError as e:  # pragma: no cover
            logging.critical("DBClient._connect unable to create DB conn!")
            logging.exception("DBClient._connect exception {}".format(e))
        except Exception as e:  # pragma: no cover
            logging.exception("DBClient._connect unhandled DB conn error {}".format(e))

        return conn

    @staticmethod
    def is_connection_open(conn):
        """
        Check if connection is initialized and alive/open.
        Connection auto-closes at end of function execution, i.e. nosetest test functions
        Opening a cursor will give you db connection error if closed.
        :param conn: read or write connection to check if open
        :return: bool, conn is_open
        """
        is_open = False
        if conn:
            try:
                with conn.cursor():
                    # OperationalError can occur when you fetchall or fetchone so have to catch this later too
                    # TODO have to execute to see if open
                    # cur.execute('SELECT 1')
                    is_open = True
            except (MySQLdb.ProgrammingError, MySQLdb.OperationalError):  # pragma: no cover
                try:
                    conn.close()
                except MySQLdb.OperationalError:
                    pass
        return is_open

    def db_lock_action(self):  # pragma: no cover
        """
        Special actions for db locks i.e.
        stats. Can be left blank and overwritten later
        :return:
        """
        pass

    def execute_write_query(self, query, retries=0):
        """
        Run a db write query
        Used for remove query
        :param query: str, query
        :param retries: int, number of retries
        :return: bool or int, success or no of rows affected
        """
        if retries > self.WRITE_RETRY_ATTEMPTS:  # pragma: no cover
            logging.debug("DBClient.execute_write_query retries exceeded, failed query {}".format(query))
            raise Exception("db write execute retires exceeded")

        result = True  # success or row count

        try:
            with self.write_connection().cursor() as cursor:
                cursor.execute(query)

                # Update or delete will return rowcount. There are cases where the rowcount being 0 is ok
                if cursor.rowcount > 0:
                    logging.debug("DBClient.execute_write_query affected rows: {}".format(cursor.rowcount))
                    result = cursor.rowcount
        except (MySQLdb.ProgrammingError, MySQLdb.InterfaceError) as e:  # pragma: no cover
            # Lock on table or db op error. This can also be Table .. doesn't exist error
            query_type = query[:query.find(' ')]
            logging.warning("DBClient.execute_write_query db error for {} query {}".format(query_type, e))
            str_e = str(e)
            # Deadlock found when trying to get lock
            # 1205 Lock wait timeout exceeded; try restarting transaction
            # 2003 Can't connect to MySQL server
            if 'trying to get lock' in str_e or 'wait timeout exceeded' in str_e or 'connect to MySQL server' in str_e:
                logging.warning("DBClient.execute_write_query lock {}".format(e))
                self.db_lock_action()
                time.sleep(self.WRITE_RETRY_WAIT_SECS)
                result = self.execute_write_query(query, retries=retries + 1)
            else:
                logging.critical("DBClient.execute_write_query exception {}, sql {}.".format(query, e))
                logging.exception("DBClient.execute_write_query unhandled error {}".format(e))
                result = False
        except MySQLdb.OperationalError as e:  # pragma: no cover
            logging.warning("DBClient.execute_write_query db operational error {}".format(e))
            self.write_db = None  # Reset db connection, server closed stale db conn but client not aware
            # MySQL server has gone away: could also be The query length of x bytes is larger than
            # max_allowed_packet size (y).
            result = self.execute_write_query(query, retries=retries + 1)
        except Exception as e:  # pragma: no cover
            logging.critical("DBClient.execute_write_query exception {}, sql {}.".format(query, e))
            logging.exception("DBClient.execute_write_query exception {}".format(e))
            result = False

        del query

        return result

    def execute_read_query(self, query, retries=0):
        """
        Run a db read query
        Used by getting stale and getting tix by broker ref
        :param query: str, query
        :param retries: int, number of retries
        :return: list, result
        """
        if retries > self.WRITE_RETRY_ATTEMPTS:  # pragma: no cover
            logging.debug("DBClient.execute_read_query retries exceeded, failed query {}".format(query))
            raise Exception("db read execute retires exceeded")

        result = None

        try:
            with self.read_connection().cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
        except MySQLdb.OperationalError as e:  # pragma: no cover
            logging.warning("DBClient.execute_write_query db operational error {}".format(e))
            self.read_db = None  # Reset db connection, server closed stale db conn but client not aware
            result = self.execute_read_query(query, retries=retries + 1)
        except Exception as e:  # pragma: no cover
            logging.critical("DBClient.execute_read_query exception {}, sql {}.".format(query, e))
            logging.exception("DBClient.execute_read_query exception {}".format(e))

        del query
        return result

    def escape_string(self, string):
        """
        Escape a string
        Without cast to string, unicode types does not get
        quotes around it, but string always does. Looks like
        a python2 thing.
        :param string: str, string to clean
        :return: str, clean string
        """
        # return str(self.read_db.escape(string))  # Ok for Python2 only
        return self.read_db.escape(str(string).encode("utf-8")).decode("utf-8")
