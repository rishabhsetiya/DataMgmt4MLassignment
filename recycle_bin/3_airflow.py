from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import requests

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# File paths
RAW_DATA_PATH = "/opt/airflow/data/raw"
DATA_LAKE_PATH = "/opt/airflow/data_lake/raw"
KAGGLE_CSV_PATH = os.path.join(RAW_DATA_PATH, "kaggle_telco_churn.csv")
SYNTHETIC_CSV_PATH = os.path.join(RAW_DATA_PATH, "synthetic_telco_churn.csv")

# Create directories if they don't exist
os.makedirs(RAW_DATA_PATH, exist_ok=True)
os.makedirs(DATA_LAKE_PATH, exist_ok=True)

# Kaggle dataset URL
kaggle_dataset_url = "https://raw.githubusercontent.com/dsrscientist/DSData/master/Telecom_customer_churn.csv"

# Function to fetch Kaggle dataset and store it
def fetch_kaggle_data():
    """Fetch Kaggle dataset and save it locally"""
    try:
        df = pd.read_csv(kaggle_dataset_url)
        df.to_csv(KAGGLE_CSV_PATH, index=False)
        print(f"Kaggle dataset downloaded and saved at: {KAGGLE_CSV_PATH}")
    except Exception as e:
        print(f"Error fetching Kaggle dataset: {str(e)}")

# Function to call the synthetic data API and store the data
def fetch_synthetic_data():
    """Fetch synthetic dataset from API and save it locally"""
    api_url = "http://localhost:5000/generate_synthetic_data"  # API endpoint
    response = requests.get(api_url)
    
    if response.status_code == 200:
        synthetic_data = response.json()
        df = pd.DataFrame(synthetic_data)
        df.to_csv(SYNTHETIC_CSV_PATH, index=False)
        print(f"Synthetic dataset saved at: {SYNTHETIC_CSV_PATH}")
    else:
        print("Failed to fetch synthetic data from API.")

# Function to store data in a local data lake with timestamped partitioning
def store_data(source, file_path):
    """Move dataset to a partitioned local storage structure"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    source_path = os.path.join(DATA_LAKE_PATH, source, timestamp)
    os.makedirs(source_path, exist_ok=True)
    
    dest_path = os.path.join(source_path, "telco_churn.csv")
    
    if os.path.exists(file_path):
        os.rename(file_path, dest_path)
        print(f"Stored {source} data at: {dest_path}")
    else:
        print(f"File {file_path} not found!")

# Function to store Kaggle dataset
def store_kaggle_data():
    store_data("kaggle", KAGGLE_CSV_PATH)

# Function to store Synthetic dataset
def store_synthetic_data():
    store_data("synthetic", SYNTHETIC_CSV_PATH)

# Define DAG
dag = DAG(
    'data_ingestion',
    default_args=default_args,
    description='Fetch and Store Kaggle and Synthetic Telco Customer Churn Data',
    schedule_interval='@daily',
)

# Define tasks
fetch_kaggle_task = PythonOperator(
    task_id='fetch_kaggle_data',
    python_callable=fetch_kaggle_data,
    dag=dag,
)

fetch_synthetic_task = PythonOperator(
    task_id='fetch_synthetic_data',
    python_callable=fetch_synthetic_data,
    dag=dag,
)

store_kaggle_task = PythonOperator(
    task_id='store_kaggle_data',
    python_callable=store_kaggle_data,
    dag=dag,
)

store_synthetic_task = PythonOperator(
    task_id='store_synthetic_data',
    python_callable=store_synthetic_data,
    dag=dag,
)

# Define dependencies
fetch_kaggle_task >> store_kaggle_task
fetch_synthetic_task >> store_synthetic_task
