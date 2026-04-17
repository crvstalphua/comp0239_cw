from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofweek, month, col
from pyspark.sql.types import DoubleType, LongType
from features import select_features_with_fare
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

    final_df = select_features_with_fare(df)

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