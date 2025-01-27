import pytest


from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, IntegerType, DoubleType
from pyspark_test import assert_pyspark_df_equal

from ..wind_turbine_analytics.transform import transform_with_quality, to_quarantine
from ..wind_turbine_analytics.schema import WIND_TURBINE_TELEMETRY_BRONZE
from .stubs import to_spark_timestamp


@pytest.mark.integration
def test_transform_with_quality(spark_session):

    event_id = "abc123"

    expected_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("turbine_id", StringType(), True),
        StructField("wind_speed", StringType(), True),
        StructField("wind_direction", StringType(), True),
        StructField("power_output", StringType(), True),

        StructField("converted_timestamp", TimestampType(), True),
        StructField("converted_turbine_id", IntegerType(), True),
        StructField("converted_wind_speed", DoubleType(), True),
        StructField("converted_wind_direction", IntegerType(), True),
        StructField("converted_power_output", DoubleType(), True),

        StructField("event_id", StringType(), False),
        StructField("is_error", BooleanType(), False)
    ])

    expectation = spark_session.createDataFrame(
        data=[
            ("2022-03-01 00:00:00", "1", "2.0", "500", "2.3", to_spark_timestamp("2022-03-01 00:00:00"), 1, 2.0, 500, 2.3, event_id, True),
            ("2022-03-01 05:00:00", "1", "2.0", "0", "2.3", to_spark_timestamp("2022-03-01 05:00:00"), 1, 2.0, 0, 2.3, event_id, False),
            ("2022-03-01 10:00:00", "1", "2.0", "1", "2.3", to_spark_timestamp("2022-03-01 10:00:00"), 1, 2.0, 1, 2.3, event_id, False),
            ("2022-03-02 00:00:00", "1", "2.0", "2", "2.3", to_spark_timestamp("2022-03-02 00:00:00"), 1, 2.0, 2, 2.3, event_id, False),
            ("2022-03-03 00:00:00", "1", "2.0", "50", "2.3", to_spark_timestamp("2022-03-03 00:00:00"), 1, 2.0, 50, 2.3, event_id, False),
            ("aaaaaa", "1", "2.0", "50", "2.3", None, 1, 2.0, 50, 2.3, event_id, True),
            (None, "1", "2.0", "500.0", "2.3", None, 1, 2.0, 500, 2.3, event_id, True),
        ],
        schema=expected_schema
    )

    base_df = spark_session.createDataFrame(
        data=[
            ("2022-03-01 00:00:00", "1", "2.0", "500", "2.3", event_id),
            ("2022-03-01 05:00:00", "1", "2.0", "0", "2.3", event_id),
            ("2022-03-01 10:00:00", "1", "2.0", "1", "2.3", event_id),
            ("2022-03-02 00:00:00", "1", "2.0", "2", "2.3", event_id),
            ("2022-03-03 00:00:00", "1", "2.0", "50", "2.3", event_id),
            ("aaaaaa", "1", "2.0", "50", "2.3", event_id),
            (None, "1", "2.0", "500.0", "2.3", event_id),
        ],
        schema=WIND_TURBINE_TELEMETRY_BRONZE
    )

    result = transform_with_quality(base_df, event_id)

    assert_pyspark_df_equal(result, expectation)


@pytest.mark.integration
def test_to_quarantine(spark_session):

    event_id = "abc123"

    validated_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("turbine_id", StringType(), True),
        StructField("wind_speed", StringType(), True),
        StructField("wind_direction", StringType(), True),
        StructField("power_output", StringType(), True),

        StructField("converted_timestamp", TimestampType(), True),
        StructField("converted_turbine_id", IntegerType(), True),
        StructField("converted_wind_speed", DoubleType(), True),
        StructField("converted_wind_direction", IntegerType(), True),
        StructField("converted_power_output", DoubleType(), True),

        StructField("event_id", StringType(), False),
        StructField("is_error", BooleanType(), False)
    ])

    base_df = spark_session.createDataFrame(
        data=[
            ("2022-03-01 00:00:00", "1", "2.0", "500", "2.3", to_spark_timestamp("2022-03-01 00:00:00"), 1, 2.0, 500, 2.3, event_id, True),
            ("2022-03-01 05:00:00", "1", "2.0", "0", "2.3", to_spark_timestamp("2022-03-01 05:00:00"), 1, 2.0, 0, 2.3, event_id, False),
            ("2022-03-01 10:00:00", "1", "2.0", "1", "2.3", to_spark_timestamp("2022-03-01 10:00:00"), 1, 2.0, 1, 2.3, event_id, False),
            ("2022-03-02 00:00:00", "1", "2.0", "2", "2.3", to_spark_timestamp("2022-03-02 00:00:00"), 1, 2.0, 2, 2.3, event_id, False),
            ("2022-03-03 00:00:00", "1", "2.0", "50", "2.3", to_spark_timestamp("2022-03-03 00:00:00"), 1, 2.0, 50, 2.3, event_id, False),
            ("aaaaaa", "1", "2.0", "50", "2.3", None, 1, 2.0, 50, 2.3, event_id, True),
            (None, "1", "2.0", "500.0", "2.3", None, 1, 2.0, 500, 2.3, event_id, True),
        ],
        schema=validated_schema
    )

    expectation = spark_session.createDataFrame(
        data=[
            ("2022-03-01 00:00:00", "1", "2.0", "500", "2.3", event_id),
            ("aaaaaa", "1", "2.0", "50", "2.3", event_id),
            (None, "1", "2.0", "500.0", "2.3", event_id),
        ],
        schema=WIND_TURBINE_TELEMETRY_BRONZE
    )

    result = to_quarantine(base_df)

    assert_pyspark_df_equal(result, expectation)
