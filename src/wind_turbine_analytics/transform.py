from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


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