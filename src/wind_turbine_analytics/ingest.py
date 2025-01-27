from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from .schema import WIND_TURBINE_TELEMETRY_SCHEMA


def ingest(spark_session: SparkSession, source_path: str, event_id: str) -> None:
    df = spark_session.read.csv(source_path, header=True, inferSchema=False, enforceSchema=True, schema=WIND_TURBINE_TELEMETRY_SCHEMA)
    df.withColumn("event_id", F.lit(event_id)).write.jdbc(
        url='jdbc:postgresql://postgres:5432/wind_turbine_analytics',
        table='bronze.wind_turbine_telemetry',
        properties={"user": "wind_turbine_analytics", "password": "wind_turbine_analytics", "driver": "org.postgresql.Driver"},
        mode="append",
    )