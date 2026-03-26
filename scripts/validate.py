"""
Validate and clean raw data from the bronze layer.
"""

import os
import pandas as pd
import json
import logging
from datetime import datetime
from scripts import data_quality

def run(bronze_paths):
    os.makedirs('data/silver', exist_ok=True)
    # Set up data quality log
    dq_log_path = 'logs/data_quality.log'
    logging.basicConfig(
        filename=dq_log_path,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s'
    )
    silver_paths = []
    for path in bronze_paths:
        if path.endswith('.csv'):
            df = pd.read_csv(path)
            expected_cols = {'timestamp', 'location', 'parameter', 'value', 'unit', 'source'}
            # Schema validation
            schema_ok = data_quality.validate_schema(df, expected_cols)
            logging.info(f"provenance: {os.path.basename(path)} schema_valid={schema_ok}")
            if not schema_ok:
                continue
            # Range check
            range_ok = data_quality.check_ranges(df, 'value', 0, 1000)
            logging.info(f"provenance: {os.path.basename(path)} value_range_valid={range_ok}")
            # Duplicate detection
            dups_ok = data_quality.detect_duplicates(df, subset=['timestamp', 'location', 'parameter'])
            logging.info(f"provenance: {os.path.basename(path)} duplicates_found={not dups_ok}")
            # Handle missing
            missing_before = df.isnull().sum().sum()
            df = df.dropna()
            missing_after = df.isnull().sum().sum()
            logging.info(f"provenance: {os.path.basename(path)} missing_before={missing_before} missing_after={missing_after}")
            # Standardize units
            df['unit'] = df['unit'].str.lower()
            out_path = path.replace('bronze', 'silver')
            df.to_csv(out_path, index=False)
            silver_paths.append(out_path)
        elif path.endswith('.json'):
            with open(path) as f:
                data = json.load(f)
            # Simple validation
            if isinstance(data, list) and data:
                for item in data:
                    item['unit'] = item.get('unit', '').lower()
                    if 'source' not in item:
                        item['source'] = 'json'
            # Log provenance for JSON
            logging.info(f"provenance: {os.path.basename(path)} json_records={len(data) if isinstance(data, list) else 1}")
            out_path = path.replace('bronze', 'silver')
            with open(out_path, 'w') as f:
                json.dump(data, f)
            silver_paths.append(out_path)
    return silver_paths
