# input file type: csv, parquet
# input columns, trip_distance, pulocationid, dolocationid, passengercount, tpep_pickup_datetime

import argparse
import os
import sys
import uuid
import shutil
import subprocess
from datetime import datetime

def parse_args():
    parser = argparse.ArgumentParser(description="Submit a NYC Taxi Fare prediction query")
    parser.add_argument("--input", required=True, help="Path to input file")
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
        "--py-files", features_script,
        predict_script,
        "--input", nfs_input,
        "--output", output_dir
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"Query completed successfully. Results saved to: {output_dir}")
    else:
        print(f"Job failed.")
        print(result.stderr)
        sys.exit(1)

if __name__ == "__main__":
    run_prediction()