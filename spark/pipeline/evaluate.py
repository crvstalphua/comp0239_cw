from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import abs, col
from pyspark.sql import SparkSession
import json
from datetime import datetime

spark = SparkSession.builder.appName("NYC Taxi Model Evaluator").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

MODEL_PATH = "/data/nyc-taxi/model/gbt_taxi_model"
CLEANED_DATA_PATH = "/data/nyc-taxi/cleaned/"
OUTPUT_PATH = "/home/almalinux/comp0239_cw/results/evaluation.json"

print("Loading model and data...")
model = PipelineModel.load(MODEL_PATH)
data = spark.read.parquet(CLEANED_DATA_PATH)

_, test_data = data.randomSplit([0.8, 0.2], seed=42)
predictions = model.transform(test_data)

evaluator = RegressionEvaluator(labelCol="fare_amount", predictionCol="prediction")
rmse = evaluator.setMetricName("rmse").evaluate(predictions)
r2 = evaluator.setMetricName("r2").evaluate(predictions)
mae = evaluator.setMetricName("mae").evaluate(predictions)

total = predictions.count()
within1 = predictions.filter(abs(col("prediction") - col("fare_amount")) <= 1).count()
within2 = predictions.filter(abs(col("prediction") - col("fare_amount")) <= 2).count()
within5 = predictions.filter(abs(col("prediction") - col("fare_amount")) <= 5).count()

results = {
    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "test_records": total,
    "rmse": round(rmse, 4),
    "r2": round(r2, 4),
    "mae": round(mae, 4),
    "percentage_within_1_dollar": round(within1/total*100, 2),
    "percentage_within_2_dollar": round(within2/total*100, 2),
    "percentage_within_5_dollar": round(within5/total*100, 2)
}

print("\n--- Model Evaluation Results ---")
for k, v in results.items():
    print(f"{k}: {v}")

import os
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
with open(OUTPUT_PATH, "w") as file:
    json.dump(results, file, indent=2)

print(f"\nResults saved to {OUTPUT_PATH}")
spark.stop()