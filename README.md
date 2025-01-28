Wind Turbine Analytics
======================

## Summary

The analytics suite calculates summary statistics and finds wind turbine power output anomalies, using wind turbine telemetry data.

## System requirements

Tested on macOS Sequoia 15.2

Software requirements:
- [Docker](https://www.docker.com)
- [Python 3.10](https://www.python.org)

## Technical overview

### Key considerations

The solution uses [Apache Spark](https://spark.apache.org) for data transformations, to ensure:
- a standard stack: Spark is used by many organisations as well as being a foundation technology to the commercial [Databricks Intelligence Platform](https://www.databricks.com); it has a wide range of packages and tooling available
- scalability: Spark is a distributed data processing framework, supporting data partitioning and distributing the processing of these partitions over a cluster of machines
- adaptability: 


The solution uses [Apache Airflow](https://airflow.apache.org) for job orchestration, to ensure:
- 

### Assumptions

- The wind turbine telemetry data is provided as comma-delimeted CSV files, having the first row as headers.
- Timestamp information is only accepted in the Spark timestamp format, and only timestamps compatiblw with the [to_timestamp](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp.html) function are accepted.
- The solution is running Spark in local mode, not in distributed mode - though the master configuration (`local[*]`) will use all available CPU cores (as defined as an Airflow connection in [./database/setup_connections.sql](./database/setup_connections.sql))

### Data and schemas




## Running the analytics suite

### Set up

1. Start the Docker containers.

    `docker compose up -d`

2. Set up the Airflow `admin` account so that the UI can be accessed:

    `docker compose run airflow-webserver airflow users create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin`

3. Set up the connections:

    `docker compose exec -it postgres bash -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB -a -f /tmp/setup_connections.sql'`

### Triggering the job

1. Log in at [http://localhost:8080](http://localhost:8080).
2. Use the
    - username: `airflow`
    - password: `airflow`
3. You should see a DAG called `wind_turbine_analytics`
    ![DAGs](./docs/airflow-dags.png)
4. Trigger the DAG
    ![DAG trigger parameters](./docs/airflow-dag-trigger.png)

### Monitoring and validation

#### In Airflow

The DAG should successfully complete:
![DAG completion](./docs/airflow-dag-success.png)

The DAG should create Datasets in Airflow to each table produced:
![Datasets](./docs/airflow-datasets.png)

#### In PostgreSQL

1. Open [http://localhost:8081](http://localhost:8081) in your browser
2. Log in by using
    - System: `PostgreSQL`
    - Server: `postgres`
    - Username: `wind_turbine_analytics`
    - Password: `wind_turbine_analytics`
    - Database: `wind_turbine_analytics`

Bronze table content:
![Bronze](./docs/db-bronze.png)

Silver table content:
![Silver](./docs/db-silver.png)

Quarantine table content:
![Quarantine](./docs/db-quarantine.png)

Gold Summary table content:
![Gold Summary](./docs/db-gold-summary.png)

Quarantine Anomalies table content:
![Quarantine Anomalies](./docs/db-gold-anomalies.png)

### Tear down

`docker compose down`

## Testing

*It's recommended to use a [Virtual Environment (venv)](https://docs.python.org/3/library/venv.html) to install packages*

```sh
pip install '.[dev]'
pytest --cache-clear --capture=no -m "integration" ./src
```