from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F


def publish_transformations(wind_turbine_telemetry: DataFrame, event_id: str, start_datetime: str, end_datetime: str) -> (DataFrame, DataFrame):
    df = wind_turbine_telemetry.filter(
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

    summary = gold_summary_statistics.select(
        F.col("turbine_id"),
        F.col("min_power_output"),
        F.col("max_power_output"),
        F.col("mean_power_output"),
        F.lit(event_id).alias("event_id")
    )

    anomalies = turbine_power_output_anomaly.select(
        F.col("t.telemetry_time"),
        F.col("t.turbine_id"),
        F.col("t.power_output"),
        F.lit(event_id).alias("event_id")
    )

    return summary, anomalies


def publish(spark_session: SparkSession, event_id: str, db_connection: dict, start_datetime: str, end_datetime: str) -> None:
    df = spark_session.read.jdbc(
        url=db_connection["jdbc_connection_string"],
        table='silver.wind_turbine_telemetry',
        properties=db_connection["jdbc_connection_properties"]
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
        url=db_connection["jdbc_connection_string"],
        table='gold.summary_statistics',
        properties=db_connection["jdbc_connection_properties"],
        mode="append",
    )

    turbine_power_output_anomaly.select(
        F.col("t.turbine_id"),
        F.col("t.power_output"),
        F.lit(event_id).alias("event_id")
    ).write.jdbc(
        url=db_connection["jdbc_connection_string"],
        table='gold.turbine_power_output_anomaly',
        properties=db_connection["jdbc_connection_properties"],
        mode="append",
    )