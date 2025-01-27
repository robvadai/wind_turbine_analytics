import pytest

from datetime import datetime
from pyspark_test import assert_pyspark_df_equal

from ..wind_turbine_analytics.publish import publish_transformations
from ..wind_turbine_analytics.schema import WIND_TURBINE_SCHEMA_SILVER, WIND_TURBINE_SCHEMA_GOLD_SUMMARY, WIND_TURBINE_SCHEMA_GOLD_ANOMALIES


def to_spark_timestamp(date_time: str) -> datetime:
    return datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')


@pytest.mark.integration
def test_publish_transformations(spark_session):

    event_id = "abc123"
    start_date = "2022-03-01 00:00:00"
    end_date = "2022-03-02 00:00:00"

    summary_expectation = spark_session.createDataFrame(
        data=[
            (1, 1.6, 4.5, 2.685714285714286, event_id),
            (2, 5.0, 10.0, 7.5, event_id),
        ],
        schema=WIND_TURBINE_SCHEMA_GOLD_SUMMARY
    )

    anomalies_expectation = spark_session.createDataFrame(
        data=[
            (to_spark_timestamp("2022-03-01 16:00:00"), 1, 4.5, event_id)
        ],
        schema=WIND_TURBINE_SCHEMA_GOLD_ANOMALIES
    )

    base_df = spark_session.createDataFrame(
        data=[
            (to_spark_timestamp("2022-03-01 00:00:00"), 1, 2.0, 1, 2.3, event_id),
            (to_spark_timestamp("2022-03-01 01:00:00"), 1, 2.0, 1, 1.9, event_id),
            (to_spark_timestamp("2022-03-01 02:00:00"), 1, 2.0, 1, 4.0, event_id),
            (to_spark_timestamp("2022-03-01 02:00:00"), 1, 2.0, 1, 2.2, event_id),
            (to_spark_timestamp("2022-03-01 03:00:00"), 1, 2.0, 1, 2.6, event_id),
            (to_spark_timestamp("2022-03-01 04:00:00"), 1, 2.0, 1, 3.3, event_id),
            (to_spark_timestamp("2022-03-01 05:00:00"), 1, 2.0, 1, 2.3, event_id),
            (to_spark_timestamp("2022-03-01 05:00:00"), 1, 2.0, 1, 3.8, event_id),
            (to_spark_timestamp("2022-03-01 05:00:00"), 1, 2.0, 1, 1.6, event_id),
            (to_spark_timestamp("2022-03-01 13:00:00"), 1, 2.0, 1, 3.7, event_id),
            (to_spark_timestamp("2022-03-01 14:00:00"), 1, 2.0, 1, 2.3, event_id),
            (to_spark_timestamp("2022-03-01 15:00:00"), 1, 2.0, 1, 1.7, event_id),
            (to_spark_timestamp("2022-03-01 16:00:00"), 1, 2.0, 1, 4.5, event_id),
            (to_spark_timestamp("2022-03-01 17:00:00"), 1, 2.0, 1, 1.8, event_id),
            (to_spark_timestamp("2022-03-01 18:00:00"), 1, 2.0, 1, 3.3, event_id),
            (to_spark_timestamp("2022-03-01 19:00:00"), 1, 2.0, 1, 2.2, event_id),
            (to_spark_timestamp("2022-03-01 20:00:00"), 1, 2.0, 1, 1.6, event_id),
            (to_spark_timestamp("2022-03-01 21:00:00"), 1, 2.0, 1, 3.1, event_id),
            (to_spark_timestamp("2022-03-01 22:00:00"), 1, 2.0, 1, 2.7, event_id),
            (to_spark_timestamp("2022-03-01 23:59:00"), 1, 2.0, 1, 1.9, event_id),
            (to_spark_timestamp("2022-03-02 00:00:00"), 1, 2.0, 1, 3.6, event_id),
            (to_spark_timestamp("2022-03-02 10:00:00"), 1, 2.0, 1, 3.6, event_id),

            (to_spark_timestamp("2022-03-01 00:00:00"), 2, 2.0, 1, 5.0, event_id),
            (to_spark_timestamp("2022-03-01 07:00:00"), 2, 2.0, 1, 10.0, event_id),
        ],
        schema=WIND_TURBINE_SCHEMA_SILVER
    )

    summary_result, anomalies_result = publish_transformations(base_df, event_id, start_date, end_date)

    assert_pyspark_df_equal(summary_expectation, summary_result)
    assert_pyspark_df_equal(anomalies_expectation, anomalies_result)

