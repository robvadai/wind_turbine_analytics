import sys
from pyspark.sql import SparkSession

from wind_turbine_analytics.tasks import ingest, transform, publish


def main():

    if len(sys.argv) < 3:
        raise Exception("Argument list is required, none found")

    mode = sys.argv[1]
    event_id = sys.argv[2]

    print("mode:", mode)
    print("event_id:", event_id)

    if mode not in ["ingest", "transform", "publish"]:
        raise Exception(f"Unknown mode: {mode}")

    spark_session = SparkSession \
                    .builder \
                    .appName("Hello World") \
                    .getOrCreate()

    if mode == "ingest":
        if len(sys.argv) < 4:
            raise Exception("Source path argument is required, none found")
        ingest(spark_session, sys.argv[3], event_id)
    elif mode == "transform":
        transform(spark_session, event_id)
    else:
        if len(sys.argv) < 5:
            raise Exception("Start and end date time arguments are required")
        publish(spark_session, event_id, sys.argv[3], sys.argv[4])


if __name__ == "__main__":
    main()