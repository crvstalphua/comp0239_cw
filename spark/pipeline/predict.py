from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofweek, month, col
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

# start spark session
spark = SparkSession.builder.appName("NYC Taxi Fare Prediction").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

DATA_PATH = "/data/nyc-taxi/cleaned"
MODEL_PATH = "/data/nyc-taxi/model/gbt_taxi_model"
OUTPUT_PATH = "/data/nyc-taxi/outputs/predictions"

# load model
print("Loading model...")
model = PipelineModel.load(MODEL_PATH)

# load data 
print("Loading data...")
df = spark.read.parquet(DATA_PATH)

# run predictions
print("Running predictions...")
predictions = model.transform(df)

# write results
print(f"Writing results to {OUTPUT_PATH}")
predictions.select(
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "prediction"
).write.mode("overwrite").parquet(OUTPUT_PATH)

count = predictions.count()
print(f"Processed {count} records.")

spark.stop()