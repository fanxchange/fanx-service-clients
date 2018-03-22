# fanx-service-clients

Base service clients for services like ElasticSearch, Redis, RabbitMQ, MySQL, Postgres. Python2 and 3 compatible.


## Pip install

* pip command:
`pip install git+https://github.com/fanxchange/fanx-service-clients.git@v1.1.1`

* In pip requirements.txt:
`-e git+https://github.com/fanxchange/fanx-service-clients.git@v1.1.1#egg=fanx-service-clients`

NOTE: You must have mysql client installed, on osx do `brew install mysql`


## Usage

ES:

    from serviceclients.search.es import ESClient
    config = [{'host': 'localhost', 'port': 9200, 'timeout': 1}]
    es_client = ESClient(config)
    es_client.create_index('some_index')

DB:

    from serviceclients.database.mysqlclient_db import DBClient
    config = {
    'read_host': 'localhost',
    'read_port': 3306,
    'read_username': 'root',
    'read_password': 'root',
    'db_name': 'test',
    'write_host': 'localhost',
    'write_port': 3306,
    'write_username': 'root',
    'write_password': 'root'}

    db = DBClient(config=config)
    sql = """SELECT * FROM table"""
    result = self.db.execute_read_query(sql)

For more see `/tests`.


## Testing

TODO: Add CI testing of each build. For now tested within the project that uses it.
