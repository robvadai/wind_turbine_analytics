import pytest


from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, BooleanType, IntegerType, DoubleType
from pyspark_test import assert_pyspark_df_equal

from ..wind_turbine_analytics.transform import transform_with_quality
from ..wind_turbine_analytics.schema import WIND_TURBINE_TELEMETRY_BRONZE, WIND_TURBINE_SCHEMA_SILVER
from .stubs import to_spark_timestamp


@pytest.mark.integration
def test_transform_with_quality(spark_session):

    event_id = "abc123"

    expected_schema = StructType([
        StructField("telemetry_time", TimestampType(), True),
        StructField("turbine_id", IntegerType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_direction", IntegerType(), True),
        StructField("power_output", DoubleType(), True),
        StructField("event_id", StringType(), False),
        StructField("is_error", StringType(), False)
    ])

    expectation = spark_session.createDataFrame(
        data=[
            (to_spark_timestamp("2022-03-01 00:00:00"), 1, 2.0, 500, 2.3, event_id, True),
            (to_spark_timestamp("2022-03-01 05:00:00"), 1, 2.0, 0, 2.3, event_id, False),
            (to_spark_timestamp("2022-03-01 10:00:00"), 1, 2.0, 1, 2.3, event_id, False),
            (to_spark_timestamp("2022-03-02 00:00:00"), 1, 2.0, 2, 2.3, event_id, False),
            (to_spark_timestamp("2022-03-03 00:00:00"), 1, 2.0, 50, 2.3, event_id, False),
            (None, 1, 2.0, 500, 2.3, event_id, True),
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

    result.show()

    # assert_pyspark_df_equal(summary_expectation, summary_result)
    # assert_pyspark_df_equal(anomalies_expectation, anomalies_result)

