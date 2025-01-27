Wind Turbine Analytics
======================

## Running the jbo

### Set up

1. Start the Docker containers.

    `docker compose up -d`

2. Set up the Airflow `admin` account so that the UI can be accessed:

    `docker compose run airflow-webserver airflow users create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin`

3. Set up the connections:

    `docker compose exec -it postgres bash -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB -a -f /tmp/setup_connections.sql'`

## Tear down

`docker compose down`

## Testing

```sh
. ./bin/activate
pip install '.[dev]'
pytest --cache-clear --capture=no -m "integration" ./src
```