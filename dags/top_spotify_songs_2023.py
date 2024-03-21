from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BaseOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from scripts import upload_dataset_to_storage

default_args = {
    'owner': 'Spotify',
    'start_date': days_ago(1),
    'schedule_interval': None,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id = 'top_spotify_songs_2023',
    start_date = default_args['start_date'],
    schedule_interval = default_args['schedule_interval'],
    tags = ['exercise']
) as dag:
    
    dag.doc_md = """
        # Airflow: Top spotify Songs 2023
        End-to-end data pipeline for the analysis process of the top Spotify songs in 2023
    """

    download_spotify_dataset = BaseOperator(
        task_id = 'download_spotify_dataset',
        bash_command = 'download_dataset.sh'
    )

    upload_to_storage = PythonOperator(
        task_id = 'upload_to_storage',
        python_callable = upload_dataset_to_storage,
        op_kwargs={'bucket_name': bucket_name, 'source_file_name': source_file_name, 'destination_blob_name': destination_blob_name}
    )

    download_spotify_dataset.set_downstream(upload_to_storage)
