from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

from .schema import WIND_TURBINE_TELEMETRY_SCHEMA

def ingest(spark_session: SparkSession, source_path: str, event_id: str) -> None:
    df = spark_session.read.csv(source_path, header=True, inferSchema=False, enforceSchema=True, schema=WIND_TURBINE_TELEMETRY_SCHEMA)
    df.withColumn("event_id", F.lit(event_id)).write.jdbc(
        url='jdbc:postgresql://postgres:5432/wind_turbine_analytics',
        table='bronze.wind_turbine_telemetry',
        properties={"user": "wind_turbine_analytics", "password": "wind_turbine_analytics", "driver": "org.postgresql.Driver"},
        mode="append",
    )


def transform(spark_session: SparkSession, event_id: str) -> None:

    df = spark_session.read.jdbc(
        url='jdbc:postgresql://postgres:5432/wind_turbine_analytics',
        table='bronze.wind_turbine_telemetry',
        properties={"user": "wind_turbine_analytics", "password": "wind_turbine_analytics",
                    "driver": "org.postgresql.Driver"},
    ).filter(
        F.col("event_id").eqNullSafe(event_id)
    )

    data_types_adjusted = df.select(
        F.to_timestamp(F.col("timestamp")).alias("telemetry_time"),
        F.col("turbine_id").cast(T.IntegerType()).alias("turbine_id"),
        F.col("wind_speed").cast(T.DoubleType()).alias("wind_speed"),
        F.col("wind_direction").cast(T.IntegerType()).alias("wind_direction"),
        F.col("power_output").cast(T.DoubleType()).alias("power_output"),
        F.col("event_id")
    ).withColumn(
        "is_error",
        F.col("telemetry_time").isNull() |
        F.col("turbine_id").isNull() |
        F.col("wind_speed").isNull() |
        F.col("wind_direction").isNull() |
        ~F.col("wind_direction").between(0, 359) |
        F.col("power_output").isNull()
    ).cache()

    data_types_adjusted.filter(
        ~F.col("is_error")
    ).drop(
        "is_error"
    ).write.jdbc(
        url='jdbc:postgresql://postgres:5432/wind_turbine_analytics',
        table='silver.wind_turbine_telemetry',
        properties={"user": "wind_turbine_analytics", "password": "wind_turbine_analytics", "driver": "org.postgresql.Driver"},
        mode="append",
    )

    data_types_adjusted.filter(
        F.col("is_error")
    ).drop(
        "is_error"
    ).write.jdbc(
        url='jdbc:postgresql://postgres:5432/wind_turbine_analytics',
        table='quarantine.wind_turbine_telemetry',
        properties={"user": "wind_turbine_analytics", "password": "wind_turbine_analytics", "driver": "org.postgresql.Driver"},
        mode="append",
    )


def publish(spark_session: SparkSession, event_id: str, start_datetime: str, end_datetime: str) -> None:
    df = spark_session.read.jdbc(
        url='jdbc:postgresql://postgres:5432/wind_turbine_analytics',
        table='silver.wind_turbine_telemetry',
        properties={"user": "wind_turbine_analytics", "password": "wind_turbine_analytics",
                    "driver": "org.postgresql.Driver"},
    ).filter(
        F.col("event_id").eqNullSafe(event_id) &
        F.col("telemetry_time").between(start_datetime, end_datetime)
    ).cache()

    gold_summary_statistics = df.groupby(
        F.col("turbine_id")
    ).agg(
        F.min(F.col("power_output")).alias("min_power_output"),
        F.max(F.col("power_output")).alias("max_power_output"),
        F.mean(F.col("power_output")).alias("mean_power_output"),
        F.stddev(F.col("power_output")).alias("stddev_power_output"),
    ).cache()

    turbine_power_output_anomaly_thresholds = gold_summary_statistics.withColumn(
        "power_output_anomaly_threshold_min",
        F.col("mean_power_output") - (F.col("stddev_power_output") * 2)
    ).withColumn(
        "power_output_anomaly_threshold_max",
        F.col("mean_power_output") + (F.col("stddev_power_output") * 2)
    )

    turbine_power_output_anomaly = df.alias("t").join(
        turbine_power_output_anomaly_thresholds.alias("a"),
        F.col("t.turbine_id").eqNullSafe(F.col("a.turbine_id"))
    ).filter(
        (F.col("t.power_output") < F.col("a.power_output_anomaly_threshold_min")) |
        (F.col("t.power_output") > F.col("a.power_output_anomaly_threshold_max"))
    )

    gold_summary_statistics.select(
        F.col("turbine_id"),
        F.col("min_power_output"),
        F.col("max_power_output"),
        F.col("mean_power_output"),
        F.lit(event_id).alias("event_id")
    ).write.jdbc(
        url='jdbc:postgresql://postgres:5432/wind_turbine_analytics',
        table='gold.summary_statistics',
        properties={"user": "wind_turbine_analytics", "password": "wind_turbine_analytics", "driver": "org.postgresql.Driver"},
        mode="append",
    )

    turbine_power_output_anomaly.select(
        F.col("t.turbine_id"),
        F.col("t.power_output"),
        F.lit(event_id).alias("event_id")
    ).write.jdbc(
        url='jdbc:postgresql://postgres:5432/wind_turbine_analytics',
        table='gold.turbine_power_output_anomaly',
        properties={"user": "wind_turbine_analytics", "password": "wind_turbine_analytics", "driver": "org.postgresql.Driver"},
        mode="append",
    )