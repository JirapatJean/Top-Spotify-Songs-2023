import os
from datetime import timedelta
from google.cloud.storage import Client
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)

# Param initializations
DAG_ID = 'top_spotify_songs_2023'
PROJECT_ID = os.environ['GCP_PROJECT']
CLUSTER_NAME = "top-spotify-songs-2023"
REGION = "asia-southeast1"
ZONE = "asia-southeast1-a"

# PySPark scripts paths
GCS_BUCKET = os.environ['GCS_BUCKET']
SCRIPT_BUCKET_PATH = f'{GCS_BUCKET}/dags/scripts'
SCRIPT_NAME = 'top_spotify_songs_cleaning.py'

# DataProc cluster configurations
CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id = PROJECT_ID,
    zone = ZONE,
    master_machine_type = 'n2-standard-2',
    worker_machine_type = 'n2-standard-2',
    num_workers = 2,
).make()

# Airflow DAG default arguments
DEFAULT_ARGS = {
    'owner': 'Spotify',
    'start_date': days_ago(1),
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# PySpark job configs
PYSPARK_JOB = {
    "reference": {'project_id': PROJECT_ID},
    "placement": {'cluster_name': CLUSTER_NAME},
    "pyspark_job": {'main_python_file_uri': f'gs://{SCRIPT_BUCKET_PATH}/{SCRIPT_NAME}'}
}

# DAG definition
with DAG(
    DAG_ID,
    default_args = DEFAULT_ARGS,
    tags = ['Spotify-2023']
) as dag:
    
    dag.doc_md = """
        # Airflow: Top spotify Songs 2023
        End-to-end data pipeline for the analysis process of the top Spotify songs in 2023
    """

    # Create cluster with generates cluster config operator
    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id = 'create_dataproc_cluster',
        cluster_name = CLUSTER_NAME,
        project_id = PROJECT_ID,
        region = REGION,
        cluster_config = CLUSTER_GENERATOR_CONFIG,
    )

    # PySpark task
    pyspark_task_data_cleaning = DataprocSubmitJobOperator(
        task_id = 'pyspark_task_data_cleaning', 
        job = PYSPARK_JOB, 
        region = REGION, 
        project_id = PROJECT_ID
    )

    # Delete cluster once done with jobs
    delete_cluster = DataprocDeleteClusterOperator(
        task_id = 'delete_cluster',
        project_id = PROJECT_ID,
        cluster_name = CLUSTER_NAME,
        region = REGION
    )

# Set task dependencies
create_dataproc_cluster >> pyspark_task_data_cleaning >> delete_cluster