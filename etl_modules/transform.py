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
    df['zip_code'] = df['zip_code'].astype(str) 
    df['score'] = pd.to_numeric(df['score'], errors='coerce')  # Convert 'score' to float. If there are any non-numeric values, they will be coerced to NaN

    # Clean ZIP code to ensure it's exactly 5 digits with no letters
    df['zip_code'] = df['zip_code'].apply(clean_zip)
    
    # Drop rows with critical missing values
    df.dropna(subset=['facility_id', 'zip_code', 'score'], inplace=True)
    
    # Convert all column headers to uppercase to match Snowflake 
    df.columns = df.columns.str.upper()
    
    return df
