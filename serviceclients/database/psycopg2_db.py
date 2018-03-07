"""
Postgres database service functions

Create postgres schema -

CREATE TABLE products (
  products_id SERIAL PRIMARY KEY,
  customers_id NUMERIC(20) DEFAULT NULL,
  reference_id varchar(255) DEFAULT NULL,
  event_id NUMERIC(20) DEFAULT NULL,
  job_instance_id NUMERIC(20) DEFAULT NULL,
  products_quantity NUMERIC(20) NOT NULL DEFAULT '0',
  products_model varchar(12) DEFAULT NULL,
  products_price decimal(15,2) NOT NULL DEFAULT '0.00',
  upload_price decimal(15,2) NOT NULL DEFAULT '0.00',
  products_date_available timestamp DEFAULT NULL,
  products_status numeric(1) DEFAULT NULL,
  specific_seat numeric(4) DEFAULT NULL,
  section_id numeric(10) DEFAULT NULL,
  seat_section varchar(255) DEFAULT NULL,
  seat_row varchar(255) DEFAULT NULL,
  seat_from varchar(20) DEFAULT NULL,
  seat_to varchar(20) DEFAULT NULL,
  seat_numbers varchar(2048) DEFAULT NULL,
  eticket numeric(4) NOT NULL DEFAULT '0',
  is_auto_confirmable numeric(4) NOT NULL DEFAULT '0',
  notes text,
  stock_type varchar(20) DEFAULT NULL,
  metadata varchar(1024) DEFAULT NULL,
  products_last_modified timestamp DEFAULT NULL,
  products_date_added timestamp NOT NULL DEFAULT CURRENT_DATE,
  hard_hash text,
  soft_hash text,
  UNIQUE (customers_id,reference_id,products_status)
)

# Need to create an index for -
  KEY idx_products_date_added (products_date_added),
  KEY idx_products_count (customers_id,products_quantity,products_status),
  KEY event_id_idx (event_id),
  KEY products_stock_type_idx (stock_type),
  KEY products_status_idx (products_status),
  KEY pricing_idx (products_status,event_id,stock_type,products_price)


CREATE TABLE brokers (
  broker_id SERIAL PRIMARY KEY,
  name varchar(255) DEFAULT NULL,
  has_autoconfirm numeric(1) NOT NULL DEFAULT '0',
  api_username varchar(255) DEFAULT NULL,
  api_encrypted_password varchar(255) DEFAULT NULL,
  groupon_trusted numeric(1) NOT NULL DEFAULT '1',
  notification_url varchar(255) DEFAULT NULL,
  notification_ok_string varchar(255) DEFAULT NULL,
  dataprocessor_id numeric(11) DEFAULT NULL,
  dataprocessor_broker_reference varchar(255) DEFAULT NULL,
  disable_ingestion numeric(1) DEFAULT NULL,
  disable_distribution numeric(1) DEFAULT NULL,
  portal_password_hash varchar(1024) DEFAULT NULL,
  email varchar(255) DEFAULT NULL,
  api_version decimal(4,2) DEFAULT NULL,
  listprice_markup_rate decimal(7,2) DEFAULT NULL,
  seller_fee_rate decimal(7,2) DEFAULT NULL,
  wholesale_opt_out numeric(1) NOT NULL DEFAULT '0',
  is_natb numeric(1) NOT NULL DEFAULT '0',
  terms_accepted_on TIMESTAMP DEFAULT NULL,
  emergency_contact_name varchar(255) DEFAULT '',
  emergency_contact_phone varchar(255) DEFAULT '',
  active numeric(1) DEFAULT '1',
  created_at timestamp NOT NULL DEFAULT CURRENT_DATE,
  updated_at timestamp DEFAULT NULL
)
"""
import logging
import time

import psycopg2
import psycopg2.extras


# For test only at the moment, setup:
# sudo yum install postgresql postgresql-devel postgresql-server
# sudo su postgres
# createuser --superuser
# $ psql template1
# psql (9.2.4)
# Type "help" for help.
# template1=# ALTER USER user-name with encrypted password '<Password>';
# ALTER USER postgres with encrypted password 'postgres'
# CREATE DATABASE db-name OWNER postgres ENCODING 'UTF8';
# Encoding error: new encoding (UTF8) is incompatible with the encoding of the template database (SQL_ASCII)
# See https://gist.github.com/amolkhanorkar/8706915
# sudo vim /var/lib/pgsql/data/pg_hba.conf  (change from peer & ident to md5) & restart
# sudo service postgresql restart

# template1=# CREATE USER tempuser WITH PASSWORD 'tempuser';
# ERROR:  role "tempuser" already exists
# template1=# CREATE USER tempuser1 WITH PASSWORD 'tempuser1';
# CREATE ROLE
# template1=# CREATE DATABASE tickets OWNER tempuser1 ENCODING 'UTF8';
# CREATE DATABASE
# template1=# GRANT ALL PRIVILEGES ON DATABASE tickets to tempuser1;


# NOTE: insert into on duplicate will also not work in pg:
# stackoverflow.com/questions/1009584/how-to-emulate-insert-ignore-and-on-duplicate-key-update-sql-merge-with-po
# Diff syntax: https://stackoverflow.com/questions/1109061/insert-on-duplicate-update-in-postgresql

# DEFAULT_CONFIG = MNG.POSTGRES_DB_CONN_PARAMS
POSTGRES_DB_CONN_PARAMS = {
    'read_host': 'localhost',
    'read_port': 5432,
    'read_username': 'postgres',
    'read_password': 'postgres',
    'db_name': 'tickets',
    'write_host': 'localhost',
    'write_port': 5432,
    'write_username': 'postgres',
    'write_password': 'postgres'
}


class DBConnExceeded(Exception):
    pass


class DBClient(object):
    """
    PostgresDB
    DB Class used to connect to postgres database
    """

    WRITE_RETRY_WAIT_SECS = 0.5
    WRITE_RETRY_ATTEMPTS = 100
    DIRTY_READS = False

    # CURSOR = psycopg2.extras.DictCursor
    CURSOR = psycopg2.extras.RealDictCursor

    def __init__(self, config=None, dirty_reads=DIRTY_READS, write_retry_attempts=WRITE_RETRY_ATTEMPTS):
        """
        Load defaults by passing config keyword
        :param config: dict, config
        :param dirty_reads: bool, enable dirty read
        :param write_retry_attempts: int, num of write query retry attempts before raise
        :return: None
        """
        self.config = config
        # self.config = POSTGRES_DB_CONN_PARAMS  # config  # TODO: hack for now to force local config while testing

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
                                             self.config['read_host'], self.config['read_port'], self.config['db_name'])
                # Dirty reads seem to decrease write locks in uat, but increase them in prod
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
                                              self.config['write_host'], self.config['write_port'], self.config['db_name'])
                # SET autocommit for current session, although Autocommit is already on by default in _connect
                # write_cursor.execute("""SET autocommit = 1""")
                # if autocommit ...
            except Exception as e:
                logging.exception("DBClient.write_connection unhandled exception {}".format(e))
                raise

        return self.write_db

    @staticmethod
    def _connect(username, password, host, port, db_name, **kwargs):
        conn = None
        try:
            conn = psycopg2.connect(host=host, port=port, database=db_name, user=username, password=password)
            conn.autocommit = True
        except Exception as e:  # pragma: no cover
            logging.exception("DBClient._connect unhandled DB conn error {}".format(e))

        return conn

    @staticmethod
    def is_connection_open(conn):
        """
        Check if connection is initialized and alive/open.
        :param conn: read or write connection to check if open
        :return: boolean is_open
        """
        is_open = False
        if conn:
            try:
                with conn.cursor():
                    # OperationalError can occur when you fetchall or fetchone so have to catch this later too
                    # TODO have to execute to see if open
                    # cur.execute('SELECT 1')
                    is_open = True
            except (psycopg2.ProgrammingError, psycopg2.OperationalError):  # pragma: no cover
                try:
                    conn.close()
                except psycopg2.OperationalError:
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
            raise DBConnExceeded("db write execute retires exceeded")

        result = True  # success or row count

        try:
            with self.write_connection().cursor() as cursor:
                cursor.execute(query)

                # Update or delete will return rowcount. There are cases where the rowcount being 0 is ok
                if cursor.rowcount > 0:
                    logging.debug("DBClient.execute_write_query affected rows: {}".format(cursor.rowcount))
                    result = cursor.rowcount
        except (psycopg2.ProgrammingError, psycopg2.InterfaceError) as e:  # pragma: no cover
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
        except psycopg2.OperationalError as e:  # pragma: no cover
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
            raise DBConnExceeded("db read execute retires exceeded")

        result = None

        try:
            with self.read_connection().cursor(cursor_factory=self.CURSOR) as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
        except psycopg2.OperationalError as e:  # pragma: no cover
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
        psycopg2 does not have a built in for this
        :param string: str, string to clean
        :return: str, clean string
        """
        return "'{}'".format(str(string))
