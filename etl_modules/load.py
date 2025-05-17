import pandas as pd
from snowflake.snowpark import Session

def load(df):
    connection_parameters = {
        "account": "CJSOHEE-QX70579",
        "user": "AIRFLOW_USER",
        "password": "airflow",
        "role": "AIRFLOW_ROLE",
        "warehouse": "COMPUTE_WH",
        "database": "HAI_DATABASE",
        "schema": "RAW",
    }

    # Create Snowflake session
    session = Session.builder.configs(connection_parameters).create()

    # Ensure context is active in case config didn't persist it
    session.sql("USE DATABASE HAI_DATABASE").collect()
    session.sql("USE SCHEMA RAW").collect()

    # Write DataFrame to Snowflake
    session.write_pandas(
        df, 
        table_name="HAI_DATA", 
        database="HAI_DATABASE", 
        schema="RAW", 
        auto_create_table=False
    )

    print(f"Inserted {len(df)} rows successfully.")

    session.close()