from pyspark.sql.functions import hour, dayofweek, month, col
from pyspark.sql.types import DoubleType, LongType

def prepare_features_with_fare(data):
    return data.select(
        col("tpep_pickup_datetime"),
        col("trip_distance").cast(DoubleType()),
        col("passenger_count").cast(DoubleType()),
        col("fare_amount").cast(DoubleType()),
        col("PULocationID").cast(LongType()),
        col("DOLocationID").cast(LongType()))

def prepare_features(data):
    return data.select(
        col("tpep_pickup_datetime"),
        col("trip_distance").cast(DoubleType()),
        col("passenger_count").cast(DoubleType()),
        col("PULocationID").cast(LongType()),
        col("DOLocationID").cast(LongType()))

def add_time_features(data):
    return data.withColumn("hour", hour(col("tpep_pickup_datetime"))) \
        .withColumn("day_of_week", dayofweek(col("tpep_pickup_datetime"))) \
        .withColumn("month", month(col("tpep_pickup_datetime")))

def filter_data_with_fare(data):
    return data.filter(
        (col("fare_amount") > 0) &
        (col("fare_amount") < 150) &
        (col("trip_distance") > 0) &
        (col("passenger_count") > 0) &
        (col("passenger_count") <= 6)
    ).dropna()

def filter_data(data):
    return data.filter(
        (col("trip_distance") > 0) &
        (col("passenger_count") > 0) &
        (col("passenger_count") <= 6)
    ).dropna()

def select_features_with_fare(data):
    data = prepare_features_with_fare(data)
    data = add_time_features(data)
    data = filter_data_with_fare(data)
    
    final_data = data.select(
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "hour",
        "day_of_week",
        "month",
        "fare_amount"
    )

    return final_data

def select_features(data):
    data = prepare_features(data)
    data = add_time_features(data)
    data = filter_data(data)

    final_data = data.select(
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "hour",
        "day_of_week",
        "month",
    )

    return final_data