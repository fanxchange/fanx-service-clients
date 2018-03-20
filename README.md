# fanx-service-clients

Base service clients for services like ElasticSearch, Redis, RabbitMQ, MySQL, Postgres


## Pip install

* pip command:
`git+https://github.com/fanxchange/fanx-service-clients.git@v1.1.0`

* In pip requirements.txt:
`-e git+https://github.com/fanxchange/fanx-service-clients.git@v1.1.0#egg=fanx-service-clients`

NOTE: You must have mysql client installed, on osx do `brew install mysql`

## Usage

ES:

    from serviceclients.search.es import ESClient
    config = [{'host': 'localhost', 'port': 9200, 'timeout': 1}]
    es_client = ESClient(config)
    es_client.create_index('some_index')

For more see `/tests`.


## Testing

TODO: Add CI testing of each build. For now tested within the project that uses it.
