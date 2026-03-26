"""
Data quality and validation framework for the pipeline.
Logs schema validation, range checks, and duplicates.
"""
import logging
import pandas as pd

def validate_schema(df, expected_cols):
    missing = set(expected_cols) - set(df.columns)
    if missing:
        logging.warning('Missing columns: %s', missing)
    return not missing

def check_ranges(df, col, min_val, max_val):
    out_of_range = df[(df[col] < min_val) | (df[col] > max_val)]
    if not out_of_range.empty:
        logging.warning('Values out of range in %s: %s', col, out_of_range.to_dict('records'))
    return out_of_range.empty

def detect_duplicates(df, subset=None):
    dups = df.duplicated(subset=subset)
    if dups.any():
        logging.warning('Duplicate rows detected: %s', df[dups].to_dict('records'))
    return not dups.any()
