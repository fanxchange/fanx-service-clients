# fanx-service-clients

Base service clients for services like ElasticSearch, Redis, RabbitMQ, MySQL, Postgres


## Pip install

* pip command:
`git+https://github.com/fanxchange/fanx-service-clients.git@v1.0.0`

* In pip requirements.txt:
`-e git+https://github.com/fanxchange/fanx-service-clients.git@v1.0.0#egg=fanx-service-clients`


## Usage

ES:

    from serviceclients.search.es import ESClient
    config = [{'host': 'localhost', 'port': 9200, 'timeout': 1}]
    es_client = ESClient(config)
    es_client.create_index('some_index')

For more see `/tests`.
