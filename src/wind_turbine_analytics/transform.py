from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T


def transform_with_quality(wind_turbine_telemetry: DataFrame, event_id: str) -> DataFrame:
    return wind_turbine_telemetry.filter(
        F.col("event_id").eqNullSafe(event_id)
    ).select(
        F.col("timestamp"),
        F.col("turbine_id"),
        F.col("wind_speed"),
        F.col("wind_direction"),
        F.col("power_output"),

        F.to_timestamp(F.col("timestamp")).alias("converted_timestamp"),
        F.col("turbine_id").cast(T.IntegerType()).alias("converted_turbine_id"),
        F.col("wind_speed").cast(T.DoubleType()).alias("converted_wind_speed"),
        F.col("wind_direction").cast(T.IntegerType()).alias("converted_wind_direction"),
        F.col("power_output").cast(T.DoubleType()).alias("converted_power_output"),

        F.col("event_id")
    ).withColumn(
        "is_error",
        F.col("converted_timestamp").isNull() |
        F.col("converted_turbine_id").isNull() |
        F.col("converted_wind_speed").isNull() |
        F.col("converted_wind_direction").isNull() |
        ~F.col("converted_wind_direction").between(0, 359) |
        F.col("converted_power_output").isNull()
    )


def to_quarantine(wind_turbine_telemetry: DataFrame) -> DataFrame:
    return wind_turbine_telemetry.filter(
        F.col("is_error")
    ).select(
        F.col("timestamp"),
        F.col("turbine_id"),
        F.col("wind_speed"),
        F.col("wind_direction"),
        F.col("power_output"),
        F.col("event_id")
    )


def to_silver(wind_turbine_telemetry: DataFrame) -> DataFrame:
    return wind_turbine_telemetry.filter(
        ~F.col("is_error")
    ).select(
        F.col("converted_timestamp").alias("telemetry_time"),
        F.col("converted_turbine_id").alias("turbine_id"),
        F.col("converted_wind_speed").alias("wind_speed"),
        F.col("converted_wind_direction").alias("wind_direction"),
        F.col("converted_power_output").alias("power_output"),
        F.col("event_id")
    )


def transform(spark_session: SparkSession, event_id: str, db_connection: dict) -> None:

    df = spark_session.read.jdbc(
        url=db_connection["jdbc_connection_string"],
        table='bronze.wind_turbine_telemetry',
        properties=db_connection["jdbc_connection_properties"]
    )

    data_types_adjusted = transform_with_quality(df, event_id).cache()

    to_silver(data_types_adjusted).write.jdbc(
        url=db_connection["jdbc_connection_string"],
        table='silver.wind_turbine_telemetry',
        properties=db_connection["jdbc_connection_properties"],
        mode="append",
    )

    to_quarantine(data_types_adjusted).write.jdbc(
        url=db_connection["jdbc_connection_string"],
        table='quarantine.wind_turbine_telemetry',
        properties=db_connection["jdbc_connection_properties"],
        mode="append",
    )