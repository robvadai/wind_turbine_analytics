from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from .schema import WIND_TURBINE_TELEMETRY_SCHEMA


def ingest(spark_session: SparkSession, source_path: str, event_id: str, db_connection: dict) -> None:
    df = spark_session.read.csv(source_path, header=True, inferSchema=False, enforceSchema=True, schema=WIND_TURBINE_TELEMETRY_SCHEMA)
    df.withColumn("event_id", F.lit(event_id)).write.jdbc(
        url=db_connection["jdbc_connection_string"],
        table='bronze.wind_turbine_telemetry',
        properties=db_connection["jdbc_connection_properties"],
        mode="append",
    )