from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofweek, month, col
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import sys

# start spark session
spark = SparkSession.builder.appName("NYC Taxi Model Training").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

DATA_PATH = "/data/nyc-taxi/raw/yellow_tripdata_2022-*.parquet"
MODEL_PATH = "/data/nyc-taxi/model/gbt_taxi_model"

# load data
print("Loading training data...")
df = spark.read.parquet(DATA_PATH)

# basic feature engineering
df = df.withColumn("hour", hour(col("tpep_pickup_datetime"))) \
       .withColumn("day_of_week", dayofweek(col("tpep_pickup_datetime"))) \
       .withColumn("month", month(col("tpep_pickup_datetime")))

df = df.withColumn("trip_distance", col("trip_distance").cast(DoubleType())) \
       .withColumn("passenger_count", col("passenger_count").cast(DoubleType())) \
       .withColumn("fare_amount", col("fare_amount").cast(DoubleType()))

# data clearning
df = df.filter(
    (col("fare_amount") > 0) & 
    (col("fare_amount") < 150) &
    (col("trip_distance") > 0) &
    (col("passenger_count") > 0) &
    (col("passenger_count") <= 6))

df = df.select(
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "hour",
    "day_of_week",
    "month",
    "fare_amount"
).dropna()

# assemble all features into a vector
assembler = VectorAssembler(inputCols=[
                                "trip_distance",
                                "PULocationID",
                                "DOLocationID",
                                "passenger_count",
                                "hour",
                                "day_of_week",
                                "month"],
                            outputCol="features")

# gbt regressor
gbt = GBTRegressor(
    featuresCol="features",
    labelCol="fare_amount",
    maxIter=50,
    maxDepth=5,
    seed=42
)

pipeline = Pipeline(stages=[assembler, gbt])
train_set, test_set = df.randomSplit([0.8, 0.2], seed=42)

# training and evaluation
print("Training model...")
model = pipeline.fit(train_set)
predictions = model.transform(test_set)

evaluator = RegressionEvaluator(
    labelCol="fare_amount",
    predictionCol="prediction",
    metricName="rmse"
)

rmse = evaluator.evaluate(predictions)
print(f"RMSE: {rmse:.2f}") # average prediction error 

evaluator.setMetricName("r2")
r2 = evaluator.evaluate(predictions)
print(f"R2: {r2:.4f}") # proportion of fare variance explained by model

model.write().overwrite().save(MODEL_PATH)
print("Model saved successfully")
