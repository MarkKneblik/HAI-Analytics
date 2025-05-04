# etl_modules/transform.py
import pandas as pd
import re

def clean_zip(zip_code):
    match = re.match(r'^\d{5}', str(zip_code))  # Regex to ensure zip code is 5 digits
    return match.group(0) if match else None

def transform(df):
    # Drop unnecessary location info
    df.drop(columns=['address', 'citytown', 'state', 'countyparish', 'telephone_number', 'footnote', 'start_date', 'end_date'], inplace=True)
    
    # Convert column types
    df['facility_id'] = df['facility_id'].astype(str)
    df['measure_id'] = df['measure_id'].astype(str)
    df['facility_name'] = df['facility_name'].astype(str)
    df['measure_name'] = df['measure_name'].astype(str)
    df['compared_to_national'] = df['compared_to_national'].astype(str)
    df['zip_code'] = df['zip_code'].astype(str)  # Convert 'zip_code' to string
    df['score'] = pd.to_numeric(df['score'], errors='coerce')  # Convert 'score' to float. If there are any non-numeric values, they will be coerced to NaN

    # Clean ZIP code to ensure it's exactly 5 digits with no letters
    df['zip_code'] = df['zip_code'].apply(clean_zip)
    
    # Drop rows with critical missing values
    df.dropna(subset=['facility_id', 'zip_code', 'score'], inplace=True)
    
    # Filter rows based on measure_id (focus on CLABSI-related data)
    df_clabsi = df[df['measure_id'].str.startswith('HAI_1')]
    
    # Filter out non-relevant measure IDs
    df_clabsi = df_clabsi[~df_clabsi['measure_id'].isin(['HAI_1_CILOWER', 'HAI_1_CIUPPER', 'HAI_1_DOPC'])]
    
    # Filter valid benchmarks
    valid_benchmarks = [
        "Better than the National Benchmark",
        "No Different than National Benchmark",
        "Worse than the National Benchmark"
    ]
    df_clabsi = df_clabsi[df_clabsi['compared_to_national'].isin(valid_benchmarks)]
    
    # Clean the facility names
    df_clabsi['facility_name'] = df_clabsi['facility_name'].str.strip().str.upper()
    
    return df_clabsi
