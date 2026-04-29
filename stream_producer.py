"""
stream_producer.py
------------------
Reads power_streaming_data.csv into a pandas DataFrame and repeatedly
samples 5 random rows, writing each sample as a timestamped CSV file
into the watched folder (power_stream_folder/).  A 10-second pause
separates each iteration so the Spark stream can pick up batches one
at a time.

Run this script from a separate Python console (NOT inside the notebook)
while the Spark streaming query is active in FinalProject.ipynb.

Usage:
    python stream_producer.py
"""

import os
import time
import pandas as pd

# Configuration
STREAMING_DATA_PATH = "power_streaming_data.csv"   # path to the source CSV
WATCH_FOLDER = "power_stream_folder"    # folder monitored by Spark
N_ITERATIONS = 20 # number of batches to send
SAMPLE_SIZE = 5  # rows per batch
SLEEP_SECONDS = 20 # time of pause between batches
RANDOM_SEED = 7

# Read the full streaming dataset into memory once
print(f"Reading source data from '{STREAMING_DATA_PATH}' ...")
df = pd.read_csv(STREAMING_DATA_PATH)
print(f"  Loaded {len(df):,} rows with columns: {list(df.columns)}\n")

# Stream loop: sample → write CSV → sleep
for i in range(1, N_ITERATIONS + 1):
    # Randomly sample SAMPLE_SIZE rows (with replacement=False for cleanliness)
    sample = df.sample(n=SAMPLE_SIZE, random_state=RANDOM_SEED + i)

    # Build a unique filename using the iteration counter
    filename = os.path.join(WATCH_FOLDER, f"batch_{i:03d}.csv")

    # Write without the pandas index; keep column headers so Spark can parse
    sample.to_csv(filename, index=False)

    print(f"[Batch {i:>2d}/{N_ITERATIONS}]  Wrote {SAMPLE_SIZE} rows → {filename}")

    # Wait before sending the next batch (skip sleep after the last batch)
    if i < N_ITERATIONS:
        time.sleep(SLEEP_SECONDS)

print("\nAll batches sent.  You may stop the Spark streaming query now.")
