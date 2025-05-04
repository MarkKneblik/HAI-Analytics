# etl_modules/extract.py
import requests
import pandas as pd

def extract(api_url, params):
    all_records = []
    
    # Keep fetching data until there is no more data to fetch
    while True:
        response = requests.get(api_url, params = params)
        
        if response.status_code != 200:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            break
        
        data = response.json()
        records = data.get('results', [])
        
        # If there are no more records, exit the loop
        if not records:
            break
        
        all_records.extend(records)  # Add the new records to the list of all records
        
        # Increase the offset for the next request
        params["offset"] += 1000
    
    # Check if records were found
    if not all_records:
        raise Exception("No data found in 'results'.")
    
    df = pd.DataFrame(all_records)
    return df