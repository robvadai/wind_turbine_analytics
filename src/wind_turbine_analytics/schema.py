from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType

WIND_TURBINE_TELEMETRY_SCHEMA = StructType([
    StructField("timestamp", StringType(), True),
    StructField("turbine_id", StringType(), True),
    StructField("wind_speed", StringType(), True),
    StructField("wind_direction", StringType(), True),
    StructField("power_output", StringType(), True),
    StructField("event_id", StringType(), True)
])

WIND_TURBINE_TELEMETRY_BRONZE = StructType([
    StructField("timestamp", StringType(), True),
    StructField("turbine_id", StringType(), True),
    StructField("wind_speed", StringType(), True),
    StructField("wind_direction", StringType(), True),
    StructField("power_output", StringType(), True),
    StructField("event_id", StringType(), True)
])

WIND_TURBINE_SCHEMA_SILVER = StructType([
    StructField("telemetry_time", TimestampType(), False),
    StructField("turbine_id", IntegerType(), False),
    StructField("wind_speed", DoubleType(), False),
    StructField("wind_direction", IntegerType(), False),
    StructField("power_output", DoubleType(), False),
    StructField("event_id", StringType(), False)
])
