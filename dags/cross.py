# ============================================================
# Pandas ETL DAG - Cross Dependency
# ============================================================

# Step 1:- Library initialization

from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


# ============================================================
# Step 2:- Declare file paths
# ============================================================

SOURCE_FILE_A = "/usr/local/airflow/data/source_data_a.csv"
SOURCE_FILE_B = "/usr/local/airflow/data/source_data_b.csv"

STAGING_FILE_A = "/usr/local/airflow/data/staging_a.csv"
STAGING_FILE_B = "/usr/local/airflow/data/staging_b.csv"

TRANSFORMED_FILE_A = "/usr/local/airflow/data/clean_a.csv"
TRANSFORMED_FILE_B = "/usr/local/airflow/data/clean_b.csv"

TARGET_FILE = "/usr/local/airflow/data/final_data.csv"


# ============================================================
# Step 3:- Define task functions
# ============================================================

# ------------------------------------------------------------
# 1. EXTRACT A
# ------------------------------------------------------------

def extract_data_a():

    print(f"Extracting data from {SOURCE_FILE_A}...")

    df = pd.read_csv(SOURCE_FILE_A)

    df.to_csv(STAGING_FILE_A, index=False)

    print("Extract A complete.")


# ------------------------------------------------------------
# 2. EXTRACT B
# ------------------------------------------------------------

def extract_data_b():

    print(f"Extracting data from {SOURCE_FILE_B}...")

    df = pd.read_csv(SOURCE_FILE_B)

    df.to_csv(STAGING_FILE_B, index=False)

    print("Extract B complete.")


# ------------------------------------------------------------
# 3. TRANSFORM A
# ------------------------------------------------------------

def transform_data_a():

    print(f"Transforming data from {STAGING_FILE_A}...")

    df = pd.read_csv(STAGING_FILE_A)

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

    df.to_csv(TRANSFORMED_FILE_A, index=False)

    print("Transform A complete.")


# ------------------------------------------------------------
# 4. TRANSFORM B
# ------------------------------------------------------------

def transform_data_b():

    print(f"Transforming data from {STAGING_FILE_B}...")

    df = pd.read_csv(STAGING_FILE_B)

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

    df.to_csv(TRANSFORMED_FILE_B, index=False)

    print("Transform B complete.")


# ------------------------------------------------------------
# 5. LOAD
# ------------------------------------------------------------

def load_data():

    print("Loading final data...")

    df_a = pd.read_csv(TRANSFORMED_FILE_A)
    df_b = pd.read_csv(TRANSFORMED_FILE_B)

    # Combine both transformed datasets
    final_df = pd.concat(
        [df_a, df_b],
        ignore_index=True
    )

    final_df.to_csv(TARGET_FILE, index=False)

    print("Final data loaded successfully!")


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

    dag_id='pandas_cross_dependency_v1',

    default_args=default_args,

    description='Pandas ETL DAG using cross dependency',

    schedule_interval=None,

    catchup=False,

) as dag:


    # ========================================================
    # Step 6:- Create tasks
    # ========================================================

    # Extract tasks

    task_extract_a = PythonOperator(
        task_id='extract_a',
        python_callable=extract_data_a,
    )

    task_extract_b = PythonOperator(
        task_id='extract_b',
        python_callable=extract_data_b,
    )


    # Transform tasks

    task_transform_a = PythonOperator(
        task_id='transform_a',
        python_callable=transform_data_a,
    )

    task_transform_b = PythonOperator(
        task_id='transform_b',
        python_callable=transform_data_b,
    )


    # Load task

    task_load = PythonOperator(
        task_id='load',
        python_callable=load_data,
    )


    # ========================================================
    # Step 7:- CROSS DEPENDENCY
    # ========================================================

    task_extract_a >> task_transform_a
    task_extract_a >> task_transform_b

    task_extract_b >> task_transform_a
    task_extract_b >> task_transform_b

    task_transform_a >> task_load
    task_transform_b >> task_load