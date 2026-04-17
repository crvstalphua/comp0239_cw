import sys
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from features import select_features
import os

# extract arguments from sys.argv to avoid argparse conflicts with spark-submit
input_path = None
output_path = None

for i, arg in enumerate(sys.argv[1:]):
    if arg == "--input":
        input_path = sys.argv[i + 2]
    if arg == "--output":
        output_path = sys.argv[i + 2]

if not input_path or not output_path:
    print("Error: --input and --output are required")
    sys.exit(1)

def predict():
    spark = SparkSession.builder.appName("NYC Taxi Fare User Query").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    MODEL_PATH = "/data/nyc-taxi/model/gbt_taxi_model"

    # load model
    print("Loading model...")
    model = PipelineModel.load(MODEL_PATH)

    # load data
    print(f"Loading data from {input_path}...")
    if input_path.endswith(".csv"):
        df = spark.read.option("header", "true").csv(input_path)
    else:
        df = spark.read.parquet(input_path)

    # force evaluation before deleting file
    df = df.cache()
    df.count()

    if os.path.exists(input_path):
        os.remove(input_path)

    df = select_features(df)

    if df.count() == 0:
        print("Error: no valid rows after data processing")
        spark.stop()
        sys.exit(1)

    print("Running predictions...")
    predictions = model.transform(df)

    print(f"Writing results to {output_path}...")
    predictions.select(
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "prediction"
    ).coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

    count = predictions.count()
    print(f"Done. Predicted {count} trips.")

    spark.stop()

if __name__ == "__main__":
    predict()