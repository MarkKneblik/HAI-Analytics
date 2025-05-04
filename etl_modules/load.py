# etl_modules/load.py
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

def load(df):
    # Connect to local PostgreSQL database
    engine = create_engine("postgresql+psycopg2://postgres:Bomber21@host.docker.internal:5432/CLABSI")

    # Test connection to local PostgreSQL database using the SQLAlchemy engine
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            print("Connection successful:", result.scalar() == 1)
            df.to_sql('cms_hai_data', engine, if_exists='replace', index=False)
    except SQLAlchemyError as error:
        print("Connection failed:", str(error))