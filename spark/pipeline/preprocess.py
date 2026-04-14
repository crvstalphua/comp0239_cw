from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofweek, month, col
from pyspark.sql.types import DoubleType, LongType
import os

# start spark session
spark = SparkSession.builder.appName("NYC Taxi Data Processing") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

DATA_DIR = "/data/nyc-taxi/raw"
OUTPUT_DIR = "/data/nyc-taxi/cleaned"
YEARS = list(range(2019, 2024))
MONTHS = list(range(1, 13))

def process_file(year, month_num):
    
    path = f"{DATA_DIR}/yellow_tripdata_{year}-{month_num:02d}.parquet"
    if not os.path.exists(path):
        print(f"Skipping {path} - not found")
        return None
    
    print(f"Processing {year}-{month_num:02d}...")
    df = spark.read.option("enableVectorizedReader", "false").parquet(path)

    # select features
    df.select(
        col("tpep_pickup_datetime"),
        col("trip_distance").cast(DoubleType()),
        col("passenger_count").cast(DoubleType()),
        col("fare_amount").cast(DoubleType()),
        col("PULocationID").cast(LongType()),
        col("DOLocationID").cast(LongType()))

    # extract hour, day, month
    df = df.withColumn("hour", hour(col("tpep_pickup_datetime"))) \
        .withColumn("day_of_week", dayofweek(col("tpep_pickup_datetime"))) \
        .withColumn("month", month(col("tpep_pickup_datetime")))

    # filter bad data
    df = df.filter(
        (col("fare_amount") > 0) &
        (col("fare_amount") < 150) &
        (col("trip_distance") > 0) &
        (col("passenger_count") > 0) &
        (col("passenger_count") <= 6)
    ).dropna()

    final_df = df.select(
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "hour",
        "day_of_week",
        "month",
        "fare_amount"
    )

    return final_df

dfs = []
for y in YEARS:
    for m in MONTHS:
        result = process_file(y, m)
        if result is not None: 
            dfs.append(result)

if not dfs:
    print("No files processed")
    spark.stop()
    exit(1)

# union all cleaned data
print(f"Combining {len(dfs)} dataframes...")
df = dfs[0]
for d in dfs[1:]:
    df = df.union(d)

print(f"Writing cleaned data to {OUTPUT_DIR}...")
df.write.mode("overwrite").parquet(OUTPUT_DIR)

print(f"Preprocessing complete.")

spark.stop()