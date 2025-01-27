from datetime import datetime


def to_spark_timestamp(date_time: str) -> datetime:
    return datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')
