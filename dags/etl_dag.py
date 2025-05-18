from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import logging
import subprocess
from datetime import datetime, timedelta
from etl_modules.extract import extract
from etl_modules.transform import transform
from etl_modules.load import load

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 4),
    'retries': 1,
    'retry_delay': timedelta(seconds=1),
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
    dag_id = 'ETL-DAG',
    default_args = default_args,
    schedule = '@monthly',
    catchup = False,
    max_active_runs = 1
) as etl_dag:

    # Create extract task
    def extract_task(**kwargs):
        # Fetch data using extract function and return a pandas DataFrame
        df = extract(api_url, params)  # Extract returns a DataFrame
        # Push the DataFrame to XCom for the transform task
        kwargs['ti'].xcom_push(key = 'dataframe', value = df)
        return df

    def transform_task(**kwargs):
        # Get the current date and time for dynamic log filename
        current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_filename = f"transform_log_{current_time}.log"
        
        # Set up logging to this file
        logging.basicConfig(filename=log_filename, level=logging.INFO)
        
        # Pull the DataFrame from XCom (from extract task)
        task_instance = kwargs['ti']
        df = task_instance.xcom_pull(task_ids='extract', key='dataframe')
        
        if df is not None:
            transformed_df = transform(df)  # Transform data
            # Push the transformed data to XCom
            task_instance.xcom_push(key = 'transformed_dataframe', value = transformed_df)
            
            # Log the transformed data using Python's logging
            logging.info(f"Transformed Data:\n{transformed_df}")
            
            return transformed_df
        else:
            logging.error("No data received from the extract task.")
            return None

    # Create load task
    def load_task(**kwargs):
        # Pull the transformed data from XCom (from transform task)
        ti = kwargs['ti']
        transformed_data = ti.xcom_pull(task_ids = 'transform', key = 'transformed_dataframe')
        if transformed_data is not None:
            load(transformed_data)  # Load transformed data into Snowflake warehouse
        else:
            raise ValueError("No transformed data available for loading.")
    
    # Create dbt task
    def run_dbt_task():
        # Variable to hold the directory that contains dbt project
        dbt_project_dir = '/opt/airflow/dbt/snowflake_transformations'
        
        try:
            # Call dbt run and capture output for logging purposes
            result = subprocess.run(
                ['dbt', 'run'],
                cwd = dbt_project_dir,
                capture_output = True,
                text = True,
                check = True
            )
            print("DBT Output:\n", result.stdout)
            if result.stderr:
                print("DBT Errors:\n", result.stderr)
        except subprocess.CalledProcessError as e:
            print("DBT Failed!")
            print("STDOUT:\n", e.stdout)
            print("STDERR:\n", e.stderr)
            raise

    extract_task_operator = PythonOperator(
        task_id = 'extract',
        python_callable = extract_task,
    )

    transform_task_operator = PythonOperator(
        task_id = 'transform',
        python_callable = transform_task,
    )

    load_task_operator = PythonOperator(
        task_id = 'load',
        python_callable = load_task,
    )
    
    dbt_task_operator = PythonOperator(
    task_id = 'run_dbt',
    python_callable = run_dbt_task,
    )

    # Set up dependencies
    extract_task_operator >> transform_task_operator >> load_task_operator >> dbt_task_operator
