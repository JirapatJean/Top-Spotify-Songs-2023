from airflow.models import DAG, Variable
from airflow.operators.empty import EmptyOperator
# from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

DAGS_FOLDER = os.environ['DAGS_FOLDER']
dataset = 'nelgiriyewithana/top-spotify-songs-2023'
dataset_save_path = '/home/airflow/gcs/data/spotify_tracks_2023.csv'
kaggle_api = Variable.get('kaggle', deserialize_json = True)

# from scripts.upload_and_download_dataset import upload_blob, download_blob

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
    # t1 = BashOperator(
    #     task_id = 'get_kaggle_api',
    #     bash_command = f'echo {kaggle_api}'
    # )

    download_dataset = BashOperator(
        task_id = 'download_dataset',
        bash_command = f'kaggle datasets download -d {dataset} --unzip -o -p {dataset_save_path}'
    )
    
    end = EmptyOperator(
        task_id = 'end'
    )

    # t1 >> end
    download_dataset >> end
