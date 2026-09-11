# Step 1:- Library initialization
from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


# Step 2:- Declare variables
SOURCE_FILE = "/usr/local/airflow/data/source_data.csv"
STAGING_FILE = "/usr/local/airflow/data/staging_raw_data.csv"
TRANSFORMED_FILE = "/usr/local/airflow/data/staging_clean_data.csv"
TARGET_FILE = "/usr/local/airflow/data/cleaned_data.csv"


# Step 3:- Define tasks

# 1. EXTRACT TASK
def extract_data():
    print(f"Extracting data from {SOURCE_FILE}...")

    df = pd.read_csv(SOURCE_FILE)

    df.to_csv(STAGING_FILE, index=False)

    print("Extraction complete.")


# 2. TRANSFORM TASK
def transform_data():
    print(f"Reading from staging file {STAGING_FILE} for transformation...")

    df = pd.read_csv(STAGING_FILE)

    # Cleaning / transformation
    df['id'] = df['id'].astype(str).str.strip()
    df['name'] = df['name'].astype(str).str.strip().str.upper()
    df['city'] = df['city'].astype(str).str.strip().str.upper()

    df.to_csv(TRANSFORMED_FILE, index=False)

    print("Transformation complete.")


# 3. VALIDATE TASK
def validate_data():
    print(f"Validating data from {STAGING_FILE}...")

    df = pd.read_csv(STAGING_FILE)

    # Check whether data exists
    if df.empty:
        raise ValueError("Data validation failed: file is empty.")

    # Check required columns
    required_columns = ['id', 'name', 'city']

    for column in required_columns:
        if column not in df.columns:
            raise ValueError(
                f"Data validation failed: missing column {column}"
            )

    print("Data validation successful.")


# 4. LOAD TASK
def load_data():
    print(f"Loading final data from {TRANSFORMED_FILE}...")

    df = pd.read_csv(TRANSFORMED_FILE)

    df.to_csv(TARGET_FILE, index=False)

    print("Data loaded successfully!")


# Step 4:- Default DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Step 5:- Define DAG
with DAG(
    dag_id='pandas_parallel_etl_v1',
    default_args=default_args,
    description='Pandas ETL DAG with parallel dependencies',
    schedule_interval=None,
    catchup=False,
) as dag:

    # Step 6:- Create tasks

    # Extract
    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
    )

    # Transform
    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
    )

    # Validate
    task_validate = PythonOperator(
        task_id='validate',
        python_callable=validate_data,
    )

    # Load
    task_load = PythonOperator(
        task_id='load',
        python_callable=load_data,
    )


    # Step 7:- Parallel dependency

    task_extract >> [task_transform, task_validate]

    [task_transform, task_validate] >> task_load