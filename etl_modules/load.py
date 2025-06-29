from airflow.hooks.base import BaseHook
from snowflake.snowpark import Session
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend


def load(df):
    # Load a pandas DataFrame into Snowflake using Snowpark and Airflow connection.
    #
    # Args:
    #     df (pandas.DataFrame): The DataFrame to be loaded into Snowflake.

    conn_id = "snowflake_airflow"

    # Retrieve the Airflow connection details by connection ID
    conn = BaseHook.get_connection(conn_id)
    extras = conn.extra_dejson  # Extra parameters stored as JSON in Airflow connection

    # Extract the private key PEM string from the connection extras
    pem_private_key = extras.get("private_key")
    if not pem_private_key:
        raise ValueError("Private key not found in Airflow connection extras.")

    # Replace escaped newline characters with actual newlines for correct key parsing
    pem_private_key = pem_private_key.replace("\\n", "\n")

    try:
        # Load the PEM private key string into a private key object
        private_key_obj = serialization.load_pem_private_key(
            pem_private_key.encode("utf-8"),
            password=None,
            backend=default_backend()
        )
    except Exception as e:
        raise ValueError(f"Failed to load private key: {e}")

    # Convert the private key object to DER format bytes required by Snowflake connector
    private_key_der = private_key_obj.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    # Prepare Snowflake connection parameters from Airflow extras, including the private key
    connection_parameters = {
        "account": extras.get("account"),
        "user": "AIRFLOW_USER",
        "role": extras.get("role"),
        "warehouse": extras.get("warehouse"),
        "database": extras.get("database"),
        "schema": extras.get("schema"),
        "authenticator": extras.get("authenticator") or "snowflake",  # Default authenticator if not provided
        "private_key": private_key_der,
    }

    # Initialize a Snowpark session with the provided connection parameters
    session = Session.builder.configs(connection_parameters).create()

    # Set the database and schema context for the session
    session.sql(f"USE DATABASE {connection_parameters['database']}").collect()
    session.sql(f"USE SCHEMA {connection_parameters['schema']}").collect()

    # Use Snowpark to write the pandas DataFrame to the HAI_DATA table
    # Set auto_create_table=False to avoid creating the table if it doesn't exist
    session.write_pandas(
        df,
        table_name="HAI_DATA",
        database=connection_parameters["database"],
        schema=connection_parameters["schema"],
        auto_create_table=False
    )

    print(f"Inserted {len(df)} rows successfully.")

    # Close the Snowpark session to free resources
    session.close()