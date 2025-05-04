from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl_modules.extract import extract
from etl_modules.transform import transform
from etl_modules.load import load
import pandas as pd

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the API endpoint for the HAI dataset
api_url = "https://data.cms.gov/provider-data/api/1/datastore/query/77hc-ibv8/0"

# Set parameters for the API request
params = {
    "limit": 1000,
    "offset": 0
}

# Create ETL DAG
with DAG(
    dag_id = 'DAG-1',
    default_args = default_args,
    schedule_interval = '@monthly',
    catchup = False,
    max_active_runs = 1
) as etl_dag:

    # Create extract task
    def extract_task(**kwargs):
        # Fetch data using extract function and return a pandas DataFrame
        df = extract(api_url, params)  # Extract returns a DataFrame
        print(df.head())
        # Push the DataFrame to XCom for the transform task
        kwargs['ti'].xcom_push(key = 'dataframe', value = df)
        return df

    # Create transform task
    def transform_task(**kwargs):
        # Pull the DataFrame from XCom (from extract task)
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids = 'extract', key = 'dataframe')
        if df is not None:
            transformed_df = transform(df)  # Transform data
            # Push the transformed data to XCom
            ti.xcom_push(key = 'transformed_dataframe', value = transformed_df)
            return transformed_df

    # # Create load task
    # def load_task(**kwargs):
    #     # Pull the transformed data from XCom (from transform task)
    #     ti = kwargs['ti']
    #     transformed_data = ti.xcom_pull(task_ids = 'transform', key = 'transformed_dataframe')
    #     if transformed_data is not None:
    #         load(transformed_data)  # Load transformed data into PostgreSQL database
    #     else:
    #         raise ValueError("No transformed data available for loading.")

    extract = PythonOperator(
        task_id = 'extract',
        python_callable = extract_task,
    )

    transform = PythonOperator(
        task_id = 'transform',
        python_callable = transform_task,
    )

    # load = PythonOperator(
    #     task_id = 'load',
    #     python_callable = load_task,
    # )

    # Set up dependencies
    extract >> transform #>> load
