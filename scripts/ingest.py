"""
Ingest data from CSV, JSON, and mock REST API into the bronze layer.
"""
import os
import pandas as pd
import requests
import json
from datetime import datetime

def run():
    os.makedirs('data/bronze', exist_ok=True)
    bronze_paths = []
    # Ingest CSV with multiple locations and parameters, add source column
    csv_path = 'data/bronze/environmental_measurements.csv'
    df_csv = pd.DataFrame([
        {'timestamp': datetime.now().isoformat(), 'location': 'SiteA', 'parameter': 'NO2', 'value': 23.5, 'unit': 'ppm', 'source': 'csv'},
        {'timestamp': datetime.now().isoformat(), 'location': 'SiteA', 'parameter': 'SO2', 'value': 12.1, 'unit': 'ppm', 'source': 'csv'},
        {'timestamp': datetime.now().isoformat(), 'location': 'SiteB', 'parameter': 'O3', 'value': 30.2, 'unit': 'ppb', 'source': 'csv'},
        {'timestamp': datetime.now().isoformat(), 'location': 'SiteB', 'parameter': 'CO', 'value': 0.8, 'unit': 'ppm', 'source': 'csv'},
        {'timestamp': datetime.now().isoformat(), 'location': 'SiteC', 'parameter': 'PM2.5', 'value': 15.0, 'unit': 'ug/m3', 'source': 'csv'},
        {'timestamp': datetime.now().isoformat(), 'location': 'SiteC', 'parameter': 'NO2', 'value': 19.7, 'unit': 'ppm', 'source': 'csv'}
    ])
    df_csv.to_csv(csv_path, index=False)
    bronze_paths.append(csv_path)
    # Ingest JSON with multiple chemicals, add source field
    json_path = 'data/bronze/chemical_data.json'
    chemical_data = [
        {'cas_number': '50-00-0', 'chemical': 'Formaldehyde', 'toxicity': 0.5, 'unit': 'mg/L', 'source': 'json'},
        {'cas_number': '75-07-0', 'chemical': 'Acetaldehyde', 'toxicity': 0.3, 'unit': 'mg/L', 'source': 'json'},
        {'cas_number': '67-56-1', 'chemical': 'Methanol', 'toxicity': 1.0, 'unit': 'mg/L', 'source': 'json'}
    ]
    with open(json_path, 'w') as f:
        json.dump(chemical_data, f)
    bronze_paths.append(json_path)
    # Ingest from EPA AirData API (real data if available, fallback to mock), add source field
    api_path = 'data/bronze/api_data.json'
    try:
        url = (
            'https://aqs.epa.gov/data/api/sampleData/bySite?email=example@example.com&key=DEMO_KEY'
            '&param=44201&bdate=20230101&edate=20230107&state=06&county=065&site=8001'
        )
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            api_data = []
            for item in data.get('Data', []):
                api_data.append({
                    'timestamp': item.get('date_local'),
                    'location': f"{item.get('state_code')}-{item.get('county_code')}-{item.get('site_number')}",
                    'parameter': item.get('parameter'),
                    'value': item.get('sample_measurement'),
                    'unit': item.get('unit'),
                    'source': 'api'
                })
            if not api_data:
                raise ValueError('No data returned from EPA API')
        else:
            raise ValueError(f'API error: {response.status_code}')
    except Exception as e:
        api_data = [
            {'timestamp': datetime.now().isoformat(), 'location': 'EPA-SiteA', 'parameter': 'Ozone', 'value': 0.034, 'unit': 'ppm', 'source': 'api'},
            {'timestamp': datetime.now().isoformat(), 'location': 'EPA-SiteB', 'parameter': 'PM2.5', 'value': 12.5, 'unit': 'ug/m3', 'source': 'api'}
        ]
    with open(api_path, 'w') as f:
        json.dump(api_data, f)
    bronze_paths.append(api_path)
    return bronze_paths
