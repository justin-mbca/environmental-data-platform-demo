"""
Curate final analytics-ready dataset and add metadata.
"""
import os
import pandas as pd
import json
from datetime import datetime

def run(gold_paths):
    for path in gold_paths:
        if path.endswith('.csv'):
            df = pd.read_csv(path)
            # Add versioning
            version = 'v1'
            df['version'] = version
            # Save final curated dataset
            curated_path = path.replace('.csv', f'_{version}.csv')
            df.to_csv(curated_path, index=False)
            # Update metadata
            metadata = {
                'source_file': path,
                'curated_file': curated_path,
                'version': version,
                'timestamp': datetime.now().isoformat()
            }
            with open('metadata.json', 'w') as f:
                json.dump(metadata, f, indent=2)
