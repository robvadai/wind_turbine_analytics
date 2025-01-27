from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.datasets import Dataset
from airflow.providers.jdbc.hooks.jdbc import JdbcHook

# spark_master = "spark://spark:7077"
spark_master = "local[*]"
spark_app_name = "Spark Hello World"
file_path = "/usr/local/spark/data"

JDBC_DRIVER_PATH = "/usr/local/spark/driver/postgresql-42.7.5.jar" #"os.environ['JDBC_DRIVER_PATH']

args = {
    'owner': 'Airflow',
}

# postgres_jdbc_local_hook = JdbcHook(jdbc_conn_id="postgres_jdbc_local")

# print(postgres_jdbc_local_hook.get_conn())

WIND_TURBINE_ANALYTICS_QUARANTINE_DATASET = Dataset("wind_turbine_analytics_quarantine")
WIND_TURBINE_ANALYTICS_BRONZE_DATASET = Dataset("wind_turbine_analytics_bronze")
WIND_TURBINE_ANALYTICS_SILVER_DATASET = Dataset("wind_turbine_analytics_silver")
WIND_TURBINE_ANALYTICS_GOLD_DATASET = Dataset("wind_turbine_analytics_gold")

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
            "spark.master": spark_master,
            "spark.driver.extraClassPath": JDBC_DRIVER_PATH,
            "spark.executor.extraClassPath": JDBC_DRIVER_PATH
        },
        application_args=["ingest", "{{ run_id }}", file_path],
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
        application_args=["transform", "{{ run_id }}"],
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
        application_args=["publish", "{{ run_id }}", dag.params['publish_start_datetime'], dag.params['publish_end_datetime']],
        outlets=[WIND_TURBINE_ANALYTICS_GOLD_DATASET],
        dag=dag
    )


    end = DummyOperator(task_id="end", dag=dag)

    start >> ingest >> transform >> publish >> end
    