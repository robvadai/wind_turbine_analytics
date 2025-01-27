import sys
from pyspark.sql import SparkSession

from wind_turbine_analytics.ingest import ingest
from wind_turbine_analytics.transform import transform
from wind_turbine_analytics.publish import publish


def main():

    if len(sys.argv) < 7:
        raise Exception("Argument list is required, none found")

    mode = sys.argv[1]
    event_id = sys.argv[2]
    jdbc_connection_host = sys.argv[3]
    jdbc_connection_port = sys.argv[4]
    jdbc_connection_user = sys.argv[5]
    jdbc_connection_password = sys.argv[6]

    if mode not in ["ingest", "transform", "publish"]:
        raise Exception(f"Unknown mode: {mode}")

    spark_session = SparkSession \
                    .builder \
                    .appName("Hello World") \
                    .getOrCreate()

    data_connection = {
        "jdbc_connection_string": f"jdbc:postgresql://{jdbc_connection_host}:{jdbc_connection_port}/wind_turbine_analytics",
        "jdbc_connection_properties": {"user": jdbc_connection_user, "password": jdbc_connection_password,
                                       "driver": "org.postgresql.Driver"}
    }

    if mode == "ingest":
        if len(sys.argv) < 8:
            raise Exception("Source path argument is required, none found")
        ingest(spark_session, sys.argv[7], event_id, data_connection)
    elif mode == "transform":
        transform(spark_session, event_id, data_connection)
    else:
        if len(sys.argv) < 9:
            raise Exception("Start and end date time arguments are required")
        publish(spark_session, event_id, data_connection, sys.argv[7], sys.argv[8])


if __name__ == "__main__":
    main()