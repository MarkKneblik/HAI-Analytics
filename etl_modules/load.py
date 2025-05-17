# etl_modules/load.py
import pandas as pd
from snowflake.snowpark import Session

def load(df):
    # Define connection parameters
    connection_parameters = {
        "account": "CJSOHEE-QX70579",       
        "user": "AIRFLOW_USER",              
        "password": "airflow",              
        "database": "HAI_DATABASE",
        "schema": "RAW",
        "warehouse": "COMPUTE_WH",
        "role": "AIRFLOW_ROLE"
    }

    # Create a Snowflake session
    session = Session.builder.configs(connection_parameters).create()

    # Define the table name in Snowflake
    table_name = "HAI_DATA"

    # Write the DataFrame to Snowflake, auto-creating the table if needed
    session.write_pandas(df, table_name, auto_create_table=False)

    print(f"Inserted {len(df)} rows successfully.")

    # Close the session
    session.close()