from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.datasets import Dataset
from airflow.hooks.base_hook import BaseHook

# spark_master = "spark://spark:7077"
spark_master = "local[*]"
spark_app_name = "Spark Hello World"
file_path = "/usr/local/spark/data"

JDBC_DRIVER_PATH = "/usr/local/spark/driver/postgresql-42.7.5.jar" #"os.environ['JDBC_DRIVER_PATH']

args = {
    'owner': 'Airflow',
}

jdbc_connection = BaseHook.get_connection("postgres_local")

WIND_TURBINE_ANALYTICS_QUARANTINE_DATASET = Dataset("postgres://postgres:5432/wind_turbine_analytics?schema=quarantine&table=wind_turbine_telemetry")
WIND_TURBINE_ANALYTICS_BRONZE_DATASET = Dataset("postgres://postgres:5432/wind_turbine_analytics?schema=bronze&table=wind_turbine_telemetry")
WIND_TURBINE_ANALYTICS_SILVER_DATASET = Dataset("postgres://postgres:5432/wind_turbine_analytics?schema=silver&table=wind_turbine_telemetry")
WIND_TURBINE_ANALYTICS_GOLD_SUMMARY_DATASET = Dataset("postgres://postgres:5432/wind_turbine_analytics?schema=gold&table=summary_statistics")
WIND_TURBINE_ANALYTICS_GOLD_ANOMALY_DATASET = Dataset("postgres://postgres:5432/wind_turbine_analytics?schema=gold&table=turbine_power_output_anomaly")

with DAG(
    dag_id='wind_turbine_analytics',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['test'],
    params = {
        "publish_start_datetime": "2022-03-01 00:00:00",
        "publish_end_datetime": "2022-03-01 20:00:00"
    }
) as dag:

    start = DummyOperator(task_id="start", dag=dag)

    ingest = SparkSubmitOperator(
        task_id="ingest",
        application="/usr/local/spark/app/driver.py",
        name=spark_app_name,
        # conn_id="spark_default",
        conn_id='spark_local',
        verbose=1,
        conf={
            # "spark.master": spark_master,
            "spark.driver.extraClassPath": JDBC_DRIVER_PATH,
            "spark.executor.extraClassPath": JDBC_DRIVER_PATH
        },
        application_args=["ingest", "{{ run_id }}", jdbc_connection.host, str(jdbc_connection.port), jdbc_connection.login, jdbc_connection.password, file_path],
        outlets=[WIND_TURBINE_ANALYTICS_BRONZE_DATASET],
        dag=dag
    )

    transform = SparkSubmitOperator(
        task_id="transform",
        application="/usr/local/spark/app/driver.py",
        name=spark_app_name,
        # conn_id="spark_default",
        conn_id='spark_local',
        verbose=1,
        conf={
            "spark.master": spark_master,
            "spark.driver.extraClassPath": JDBC_DRIVER_PATH,
            "spark.executor.extraClassPath": JDBC_DRIVER_PATH
        },
        application_args=["transform", "{{ run_id }}", jdbc_connection.host, str(jdbc_connection.port), jdbc_connection.login, jdbc_connection.password],
        outlets=[WIND_TURBINE_ANALYTICS_QUARANTINE_DATASET, WIND_TURBINE_ANALYTICS_SILVER_DATASET],
        dag=dag
    )

    publish = SparkSubmitOperator(
        task_id="publish",
        application="/usr/local/spark/app/driver.py",
        name=spark_app_name,
        # conn_id="spark_default",
        conn_id='spark_local',
        verbose=1,
        conf={
            "spark.master": spark_master,
            "spark.driver.extraClassPath": JDBC_DRIVER_PATH,
            "spark.executor.extraClassPath": JDBC_DRIVER_PATH
        },
        application_args=["publish", "{{ run_id }}", jdbc_connection.host, str(jdbc_connection.port), jdbc_connection.login, jdbc_connection.password, dag.params['publish_start_datetime'], dag.params['publish_end_datetime']],
        outlets=[WIND_TURBINE_ANALYTICS_GOLD_SUMMARY_DATASET, WIND_TURBINE_ANALYTICS_GOLD_ANOMALY_DATASET],
        dag=dag
    )


    end = DummyOperator(task_id="end", dag=dag)

    start >> ingest >> transform >> publish >> end
    