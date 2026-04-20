# input file type: csv, parquet
# input columns, trip_distance, pulocationid, dolocationid, passengercount, tpep_pickup_datetime

import argparse
import csv
import os
import sys
import uuid
import shutil
import subprocess
import time
from datetime import datetime

def parse_args():
    parser = argparse.ArgumentParser(description="Submit a NYC Taxi Fare prediction query")
    parser.add_argument("--input", required=True, help="Path to input file")
    parser.add_argument("--executor-memory", default="2g")
    parser.add_argument("--executor-cores", default="2")
    parser.add_argument("--parallelism", default="8")
    parser.add_argument("--config-label", default="default")
    return parser.parse_args()

def run_prediction():
    args = parse_args()

    # check input file exists
    if not os.path.exists(args.input):
        print(f"Error: input file not found: {args.input}")
        sys.exit(1)
    
    # check file type
    if not args.input.endswith((".csv", ".parquet")):
        print(f"Error: input file must be .csv or .parquet")
        sys.exit(1)

    # generate query id
    query_id = str(uuid.uuid4())[:8]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    query_name = f"query_{timestamp}_{query_id}"
    output_dir = f"/data/nyc-taxi/outputs/{query_name}"
    ext = os.path.splitext(args.input)[1]
    nfs_input = f"{output_dir}/input{ext}"

    os.makedirs(output_dir, exist_ok=True)
    os.chmod(output_dir, 0o777)
    shutil.copy2(args.input, nfs_input)

    print(f"Query ID:   {query_name}")
    print(f"Input:      {args.input}")
    print(f"Output:     {output_dir}")
    print(f"Started:    {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Running predictions...")

    spark_submit = "/opt/spark/bin/spark-submit"
    master = "spark://10.134.12.124:7077" # change to ip of master
    predict_script = "/home/almalinux/comp0239_cw/spark/pipeline/predict_user.py"
    features_script = "/home/almalinux/comp0239_cw/spark/pipeline/features.py"

    cmd = [
        "sudo", "-u", "spark",
        spark_submit, 
        "--master", master,
        "--name", query_name,
        "--executor-memory", args.executor_memory,
        "--executor-cores", args.executor_cores,
        "--conf", f"spark.default.parallelism={args.parallelism}",
        "--py-files", features_script,
        predict_script,
        "--input", nfs_input,
        "--output", output_dir
    ]

    start_time = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True)
    duration = time.time() - start_time

    if result.returncode == 0:
        log_query(query_name, args.input, duration, True, args.config_label)
        print(f"Query completed successfully. Results saved to: {output_dir}")
    else:
        log_query(query_name, args.input, duration, False, args.config_label)
        print(f"Job failed.")
        print(result.stderr)
        sys.exit(1)

def log_query(query_name, input_path, duration_seconds, success, config_label="default"):
    log_file = "/data/nyc-taxi/outputs/query_log.csv"
    file_exists = os.path.exists(log_file)

    with open(log_file, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "query_id", "input", "duration_seconds", "success", "config"])
        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            query_name,
            os.path.basename(input_path),
            round(duration_seconds, 2),
            success,
            config_label
        ])

if __name__ == "__main__":
    run_prediction()