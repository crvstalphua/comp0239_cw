# comp0239_cw

## NYC Taxi Fare Prediction 

A distributed machine learning inference service built on Apache Spark, deployed across 5 VMs using Ansible and Terraform. The service predicts the NYC taxi fares using a pre-trained Gradient Booster Tree (GBT) model trained on historical trip records.

## Architecture Overview
```
Laptop → condenser (UCL jumphost) → Host VM (10.134.12.124)
                                         │
                         ┌───────────────┼───────────────┐
                         │               │               │
                    Spark Master    NFS Server      Prometheus
                    Grafana         /data/nyc-taxi  Node Exporter
                    Job Submission
                         │
        ┌────────────────┼─────────────────┬─────────────────┐
        │                │                 │                 │
     Worker 1         Worker 2          Worker 3          Worker 4
  (10.134.12.123)   (10.134.12.93)    (10.134.12.85)    (10.134.12.101)
   Spark Executor   Spark Executor    Spark Executor     Spark Executor
   Node Exporter     Node Exporter     Node Exporter      Node Exporter
    NFS Client        NFS Client        NFS Client         NFS Client
```
**Resource allocation:**
| Node | vCPU | RAM | Disk | Role |
|------|------|-----|------|------|
| Host | 4 | 8 GB | 50 GB | Spark Master, NFS Server, Monitoring |
| Worker ×4 | 4 | 32 GB | 110 GB | Spark Executors |
| **Total** | **20** | **136 GB** | **490 GB** | |

---

## Dataset
 
**Source:** [NYC Taxi and Limousine Commission Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

**Scope:** Yellow taxi trip records, January 2015 – December 2023
 
**Size:** ~108 Parquet files, ~694 million records after cleaning

**Features used for prediction:**
 
| Feature | Description |
|---------|-------------|
| `trip_distance` | Distance in miles |
| `PULocationID` | Pickup taxi zone (1–263) |
| `DOLocationID` | Dropoff taxi zone (1–263) |
| `passenger_count` | Number of passengers |
| `hour` | Hour of pickup (0–23) |
| `day_of_week` | Day of week (1–7) |
| `month` | Month of year (1–12) |

**Target:** `fare_amount` (USD)

**Note:** Pre-2015 data uses GPS coordinates instead of zone IDs and is incompatible with this pipeline. Data from 2009–2014 is available on NFS but excluded from the training and testing at this moment.

**Note:** The NYC TLC CDN (CloudFront) may block download requests from cloud VM IPs. If re-running from scratch and ansible download fails, download the raw Parquet files locally and transfer via `scp`.

---

## Model

**Algorithm:** Gradient Boosted Tree Regressor (`GBTRegressor`) via Apache Spark MLlib
**Why GBT:**
- Handles non-linear relationships between location zones and fares
- Robust to outliers in fare amounts
- Naturally distributed via Spark's MLlib pipeline

**Training configuration:**
- `maxIter=50` — 50 boosting rounds
- `maxDepth=5` — maximum tree depth
- Train/test split: 80/20

**Performance:**
- Root Mean Squared Error (RMSE): 3.75
- R²: 0.8907
- Mean Absolute Error (MAE): $1.90
- Within $1: 46.3%
- Within $2: 72.0%
- Within $5: 93.4%

**Model storage:** `/data/nyc-taxi/model/gbt_taxi_model` (Spark ML PipelineModel format)

## Setup

### Requirements
- Terraform v1.14.3 
- Ansible 2.17.14
- Python 3 (Will be installed by ansible)

### SSH Key Setup Required
Before running any code, you must configure a SSH key to be used when provisioning the Virtual Machines and for authentication between machines.
```
#generate SSH key (if you don't have one)
ssh-keygen -t ed25519 -f ~/.ssh/comp-02359_key
```

### 1. Clone repository
```
git clone https://gitlab2.ds4eng.condenser.arc.ucl.ac.uk/ucabcph/comp0239_cw.git
cd comp0239_cw
```

### 2. Provision infrastructure 
(if running from scratch)
```
cd terraform
#change the relevant variables in variables.tf 
terraform init
terraform apply
```
variables.tf
```
# your rancher username (e.g. ucbcdwb)
variable username {
  type = string
  default = "username"
}

variable namespace_ending {
  type = string
  default = "-comp0235-ns"
}

# The name of your ssh key uploaded to rancher 
variable keyname {
  type = string
  default = "sshkey"
}
```
### 3. Configure software
```
#copy public key over to all machines 
#change any variables/directories before running any ansible notebooks
cd ../ansible
#configure host machine
ansible-playbook -i inventory.json host.yml --private-key ~/.ssh/your_private_key
#configure worker machines
ansible-playbook -i inventory.json worker.yml --private-key ~/.ssh/your_private_key
```
inventory.json
```
{
    "all": {
        "children": {
            "host": {},
            "workers": {}
        },
        "vars": {
            "nfs_server_ip": "10.134.12.124",
            "master_ip": "10.134.12.124",
            "project_dir": "/home/almalinux/comp0239_cw",
            "worker_ips": [
                "10.134.12.123",
                "10.134.12.93",
                "10.134.12.85",
                "10.134.12.101"
            ]
        }
    },
    "host": {
        "hosts": {
            "localhost": {
                "ansible_connection": "local"
            }
        }
    },
    "workers": {
        "hosts": {
            "10.134.12.123": {
                "ansible_host": "10.134.12.123"
            },
            "10.134.12.93": {
                "ansible_host": "10.134.12.93"
            },
            "10.134.12.85": {
                "ansible_host": "10.134.12.85"
            },
            "10.134.12.101": {
                "ansible_host": "10.134.12.101"
            }
        }
    }
}
```
### 4. Submit a query
Submit a query for fare prediction
```
cd ../query
#can be run with a .csv file 
python3 submit_query.py --input input_file.csv
#can be run with a .parquet file
python3 submit_query.py --input input_file.parquet
```

#### Input Format
A CSV or Parquet file with these columns:
```
trip_distance, PULocationID, DOLocationID, passenger_count, tpep_pickup_datetime
```
Example CSV:
 
```csv
trip_distance,PULocationID,DOLocationID,passenger_count,tpep_pickup_datetime
2.5,132,138,1,2022-06-15 08:30:00
5.1,161,234,2,2022-06-15 17:45:00
```
Zone IDs can be found in the [NYC TLC Taxi Zone Lookup Table](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

The script automatically:
1. Copies the input file to NFS
2. Submits a Spark job to the cluster
3. Cleans up the input file after completion
4. Saves the prediction results
5. Logs the query to `/data/nyc-taxi/outputs/query_log.csv`

### 5. Retrieve results

```bash
# list all completed queries
python3 ~/comp0239_cw/query/get_results.py --list
 
# view results for a specific query
python3 ~/comp0239_cw/query/get_results.py --query query_name
 # example: python3 ~/comp0239_cw/query/get_results.py --query query_20260417_120905_ccbb8980
```

#### Output format
```
  distance  pickup zone  dropoff zone  passengers  predicted fare
------------------------------------------------------------
       2.5          132           138         1.0 $         12.52
       5.1          161           234         2.0 $         21.92
```
 
Full results are saved to `/data/nyc-taxi/outputs/<query_name>/part-*.csv`.
 
---

## Monitoring and Logging

### Grafana dashboard (Shows CPU, Ram, disk, network per node): 
Access the monitoring dashboard:
- Via SSH tunnel:
```
    ssh -i ~/.ssh/comp0235 -L 3000:10.134.12.124:3000 -J condenser almalinux@10.134.12.124 -N &
```
- Link: http://localhost:3000
- Default credentials: admin/admin

### Spark UI (Shows running and completed applications):
- Via SSH tunnel:
```
ssh -i ~/.ssh/comp0235 -L 8080:10.134.12.124:8080 -J condenser almalinux@10.134.12.124 -N &
```

### View logs:
```
# capacity test shell output
tail -f /tmp/capacity_test.log

# per-query log (timing, config, success)
cat /data/nyc-taxi/outputs/query_log.csv

# spark application logs
/opt/spark/logs/
```
---
## Capacity Test
Simulates multiple users sending in queries, processes sequentially through each month of data. Runs up to 24 hours, adjusting spark configurations at each iteration. 

**Note:** Output for stress test is not saved as that would exceed disk space. Logs with results for each test can be viewed by accessing logs file.

### Run the 24-hour stress test
```
nohup bash ~/comp0239_cw/query/capacity_test.sh > /tmp/capacity_test.log 2>&1 &
echo "PID: $!"
```

 ### Monitor progress
```
tail -f /tmp/capacity_test.log
tail -f /data/nyc-taxi/outputs/query_log.csv
ps aux | grep capacity_test | grep -v grep
```

### Spark configurations tested
 
| Config | Executor Memory | Executor Cores | Parallelism |
|--------|----------------|----------------|-------------|
| A — baseline | 2g | 2 | 8 |
| B — more memory | 4g | 2 | 8 |
| C — more cores | 2g | 4 | 16 |
| D — balanced | 4g | 4 | 16 |
 
The test cycles through all 108 input files per configuration before switching, ensuring fair comparison.

### Analysis Results
Once the stress test is complete, run the analysis script to generate a summary of performance across all configurations:

```
python3 ~/comp0239_cw/scripts/analyse_results.py
```
The script reads 'query_log.csv' and outputs:
- Total runs and success rate
- Mean, min, max, and standard deviation of query duration per config
- Throughput in runs per hour per config
- Best and worst performing config

**Test duration:** 2026-04-17 16:39 UTC to 2026-04-18 16:36 UTC (24 hours)

**Summary:**
| Metric | Value |
|--------|-------|
| Total runs | 842 |
| Successful runs | 842 |
| Failed runs | 0 |
| Success rate | 100% |

**Performance by Spark configuration:**
| Config | Executor Memory | Cores | Mean Duration | Throughput (runs/hr) |
|--------|----------------|-------|---------------|----------------------|
| A — baseline | 2g | 2 | 103.6s | 34.75 |
| B — more memory | 4g | 2 | 102.2s | 35.23 |
| C — more cores | 2g | 4 | **100.5s** | **35.84** |
| D — balanced | 4g | 4 | 102.8s | 35.02 |

**Conclusion:** Config C (2g memory, 4 cores) achieved the best throughput at 35.84 runs/hour, 3% faster than the baseline. The workload is CPU-bound rather than memory-bound, doubling cores improved performance more than doubling memory. All configurations maintained 100% success rate throughout the 24-hour test.

Full results available in `results/stress_test_analysis.json`.

---
## Training the model
If necessary to train the model from scratch, upload all PARQUET files into the nfs raw folder then adjust file names and file paths.

### Preprocess (one-time, run as spark user)
```
sudo -u spark /opt/spark/bin/spark-submit \
  --master spark://10.134.12.124:7077 \
  --py-files /home/almalinux/comp0239_cw/spark/pipeline/features.py \
  /home/almalinux/comp0239_cw/spark/pipeline/preprocess.py
```
Reads all raw Parquet files, normalises schemas, filters invalid records, extracts time features, and writes cleaned Parquet to `/data/nyc-taxi/cleaned/`.

### Train (one-time, run as spark user)
 
```
sudo -u spark /opt/spark/bin/spark-submit \
  --master spark://10.134.12.124:7077 \
  --py-files /home/almalinux/comp0239_cw/spark/pipeline/features.py \
  /home/almalinux/comp0239_cw/spark/pipeline/train.py
```
 
Trains GBTRegressor on cleaned data and saves the model to NFS.

### Evaluate (run as spark user)
```
sudo -u spark /opt/spark/bin/spark-submit \
  --master spark://10.134.12.124:7077 \
  --executor-memory 8g \
  --py-files /home/almalinux/comp0239_cw/spark/pipeline/features.py \
  /home/almalinux/comp0239_cw/spark/pipeline/evaluate.py
```
Results saved to `results/evaluation.json`.

---
## Repository Structure
 
```
comp0239_cw/
├── terraform/              # VM provisioning
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   ├── providers.tf
│   ├── versions.tf
│   └── cloud-init.tmpl.yml
├── ansible/                # Configuration management
│   ├── inventory.json
│   ├── host.yml
│   ├── worker.yml
│   └── roles/
│       ├── common/         # Java, Python, NFS utils
│       ├── nfs_server/     # NFS export, directory setup
│       ├── nfs_client/     # NFS mount on workers
│       ├── dataset/        # NYC TLC data download
│       ├── spark_master/   # Spark master service
│       ├── spark_worker/   # Spark worker service
│       ├── node_exporter/  # Prometheus node exporter
│       ├── prometheus/     # Prometheus server
│       ├── pyspark/        # Pyspark utils
│       └── grafana/        # Grafana server
├── spark/
│   └── pipeline/
│       ├── features.py     # Feature engineering
│       ├── preprocess.py   # Bulk data preprocessing (for training/batch pred)
│       ├── train.py        # Model training
│       ├── evaluate.py     # Model evaluation
│       ├── predict.py      # Batch prediction
│       └── predict_user.py # User query prediction
│
│
├── results/
│       ├── evaluation.json # Model evaluation metrics
│       └── stress_test_analysis.json # Capacity test analysis
│
└── query/
    ├── submit_query.py     # User job submission
    ├── get_results.py      # Result retrieval
    ├── analyse_results.py  # Stress test analysis
    └── capacity_test.sh    # 24-hour stress test
```
---

## Scaling up

### Expandability of Data Sources
Currently, only yellow taxi data from 2015-2023 is used but there is data available up to 2009. It may be possible to utilise this data if gps-coordinates to location zone mapping is done. Other data sources such as green taxi data can also be used

## Known limitations
1. **UTC timestamps:** All VM clocks run in UTC. UK users should account for BST offset (+1h) when cross-referencing query logs with Grafana dashboards.

2. **Sequential job execution:** Spark standalone mode processes one job at a time. Multiple simultaneous user submissions queue rather than run concurrently. This is appropriate for a 4-worker cluster where full resource utilisation per job is preferable to resource splitting.












