# step 1:- Library initillasition.
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# step 2:- Declere variable
SOURCE_FILE = "/usr/local/airflow/data/source_data.csv"
STAGING_FILE = "/usr/local/airflow/data/staging_raw_data.csv"
TRANSFORMED_FILE = "/usr/local/airflow/data/staging_clean_data.csv"
TARGET_FILE = "/usr/local/airflow/data/cleaned_data.csv"

# step 3:- Define a tasks
# 1. EXTRACT TASK
def extract_data():
    print(f"Extracting data from {SOURCE_FILE}...")
    # Read the raw source CSV
    df = pd.read_csv(SOURCE_FILE)
    # Save it to a staging location so the next task can pick it up
    df.to_csv(STAGING_FILE, index=False)
    print("Extraction complete.")

# 2. TRANSFORM TASK
def transform_data():
    print(f"Reading from staging file {STAGING_FILE} for transformation...")
    df = pd.read_csv(STAGING_FILE)
    
    # Pandas transformations: 
    # .str.strip() removes spacing, .str.upper() converts to uppercase
    df['id'] = df['id'].astype(str).str.strip()
    df['name'] = df['name'].astype(str).str.strip().str.upper()
    df['city'] = df['city'].astype(str).str.strip().str.upper()
    
    # Save the polished data to a clean staging file
    df.to_csv(TRANSFORMED_FILE, index=False)
    print("Transformation complete.")

# 3. LOAD TASK
def load_data():
    print(f"Loading final data from {TRANSFORMED_FILE} to production location...")
    df = pd.read_csv(TRANSFORMED_FILE)
    
    # Save to final destination
    df.to_csv(TARGET_FILE, index=False)
    print("Data loaded successfully!")
    
    
# step 4:- Define DAGs configration
# Default DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# step 5:- Define the DAGs 
# Define the DAG
with DAG(
    dag_id='pandas_multi_task_etl_v1',
    default_args=default_args,
    description='A modular ETL DAG using Pandas and multi-task dependencies',
    schedule_interval=None,
    catchup=False,
) as dag:
    
# step 6:- Execte the task  
# Task 1: Extract
    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
    )

    # Task 2: Transform
    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
    )

    # Task 3: Load
    task_load = PythonOperator(
        task_id='load',
        python_callable=load_data,
    )  
    
# step 7:- setting the pipeline dependecy graph.
# Setting the pipeline dependency graph
    task_extract >> task_transform >> task_load