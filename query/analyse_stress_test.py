import pandas as pd
import json
from datetime import datetime
import os

data = pd.read_csv("/data/nyc-taxi/outputs/query_log.csv")

# filter stress test runs only
stress = data[data['config'].str.startswith('config_')]

print(f"Total stress test runs: {len(stress)}")
print(f"Successful runs: {stress['success'].sum()}")
print(f"Failed runs: {(~stress['success']).sum()}")
print(f"Duration: {stress['timestamp'].min()} to {stress['timestamp'].max()}")

print("\n--- Average duration per config (seconds) ---")
print(stress.groupby('config')['duration_seconds'].agg(['mean', 'std', 'min', 'max', 'count']).round(2))

print("\n--- Success rate per config ---")
print(stress.groupby('config')['success'].mean().round(4))

print("\n--- Throughput (runs per hour) per config ---")
print((3600 / stress.groupby('config')['duration_seconds'].mean()).round(4))

results = {
    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "total_runs": len(stress),
    "successful_runs": int(stress['success'].sum()),
    "failed_runs": int((~stress['success']).sum()),
    "duration": {
        "start": stress['timestamp'].min(),
        "end": stress['timestamp'].max()
    },
    "config_performance": stress.groupby('config')['duration_seconds'].agg(
        ['mean', 'std', 'min', 'max', 'count']
    ).round(2).to_dict(),
    "success_rate": stress.groupby('config')['success'].mean().round(4).to_dict(),
    "throughput_runs_per_hour": (3600 / stress.groupby('config')['duration_seconds'].mean()).round(2).to_dict(),
    "best_config": stress.groupby('config')['duration_seconds'].mean().idxmin(),
    "worst_config": stress.groupby('config')['duration_seconds'].mean().idxmax()
}

os.makedirs("/home/almalinux/comp0239_cw/results", exist_ok=True)
with open("/home/almalinux/comp0239_cw/results/stress_test_analysis.json", "w") as file:
    json.dump(results, file, indent=2)

print("\nResults saved to results/stress_test_analysis.json")
