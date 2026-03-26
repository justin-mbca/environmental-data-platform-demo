"""
Transform and integrate validated data into unified schema.
"""
import os
import pandas as pd
import json
from datetime import datetime

def run(silver_paths):
    os.makedirs('data/gold', exist_ok=True)
    gold_paths = []
    # Merge CSV and JSON into unified DataFrame
    dfs = []
    for path in silver_paths:
        if path.endswith('.csv'):
            dfs.append(pd.read_csv(path))
        elif path.endswith('.json'):
            with open(path) as f:
                data = json.load(f)
            dfs.append(pd.DataFrame(data))
    if dfs:
        df_merged = pd.concat(dfs, ignore_index=True, sort=False)
        # Simulate CAS number resolution
        if 'cas_number' not in df_merged.columns:
            df_merged['cas_number'] = 'unknown'
        # Add ingestion timestamp
        df_merged['ingestion_ts'] = datetime.now().isoformat()
        # Preserve record-level source/lineage
        if 'source' not in df_merged.columns:
            df_merged['source'] = 'unknown'
        out_path = 'data/gold/curated_dataset_v1.csv'
        df_merged.to_csv(out_path, index=False)
        gold_paths.append(out_path)
    return gold_paths
