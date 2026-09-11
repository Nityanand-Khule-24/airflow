# ============================================================
# Pandas ETL DAG using chain() method
# ============================================================

# Step 1:- Library initialization

from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.helpers import chain


# ============================================================
# Step 2:- Declare file paths
# ============================================================

SOURCE_FILE = "/usr/local/airflow/data/source_data.csv"
STAGING_FILE = "/usr/local/airflow/data/staging_raw_data.csv"
TRANSFORMED_FILE = "/usr/local/airflow/data/staging_clean_data.csv"
TARGET_FILE = "/usr/local/airflow/data/cleaned_data.csv"


# ============================================================
# Step 3:- Define task functions
# ============================================================

# ------------------------------------------------------------
# 1. EXTRACT TASK
# ------------------------------------------------------------

def extract_data():

    print(f"Extracting data from {SOURCE_FILE}...")

    # Read source CSV
    df = pd.read_csv(SOURCE_FILE)

    # Save raw data into staging
    df.to_csv(STAGING_FILE, index=False)

    print("Extraction complete.")


# ------------------------------------------------------------
# 2. TRANSFORM TASK
# ------------------------------------------------------------

def transform_data():

    print(f"Reading data from {STAGING_FILE}...")

    # Read staging data
    df = pd.read_csv(STAGING_FILE)

    # Cleaning / transformation
    df['id'] = df['id'].astype(str).str.strip()

    df['name'] = (
        df['name']
        .astype(str)
        .str.strip()
        .str.upper()
    )

    df['city'] = (
        df['city']
        .astype(str)
        .str.strip()
        .str.upper()
    )

    # Save transformed data
    df.to_csv(TRANSFORMED_FILE, index=False)

    print("Transformation complete.")


# ------------------------------------------------------------
# 3. LOAD TASK
# ------------------------------------------------------------

def load_data():

    print(f"Loading data from {TRANSFORMED_FILE}...")

    # Read transformed data
    df = pd.read_csv(TRANSFORMED_FILE)

    # Save final data
    df.to_csv(TARGET_FILE, index=False)

    print("Data loaded successfully!")


# ============================================================
# Step 4:- Default DAG configuration
# ============================================================

default_args = {

    'owner': 'airflow',

    'depends_on_past': False,

    'start_date': datetime(2026, 8, 23),

    'retries': 1,

    'retry_delay': timedelta(minutes=5),
}


# ============================================================
# Step 5:- Define DAG
# ============================================================

with DAG(

    dag_id='pandas_etl_chain_v1',

    default_args=default_args,

    description='Pandas ETL DAG using chain method',

    schedule_interval=None,

    catchup=False,

) as dag:


    # ========================================================
    # Step 6:- Create tasks
    # ========================================================

    # EXTRACT
    task_extract = PythonOperator(

        task_id='extract',

        python_callable=extract_data,
    )


    # TRANSFORM
    task_transform = PythonOperator(

        task_id='transform',

        python_callable=transform_data,
    )


    # LOAD
    task_load = PythonOperator(

        task_id='load',

        python_callable=load_data,
    )


    # ========================================================
    # Step 7:- Define dependencies using chain()
    # ========================================================

    chain(

        task_extract,

        task_transform,

        task_load

    )