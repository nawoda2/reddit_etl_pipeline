from airflow import DAG
from datetime import datetime
import os 
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

default_args = {
    'owner': 'Nawoda Wijesooriya',
    'start_date': datetime(2023, 10, 22)
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id='etl_reddit_pipeline',
    dafault_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit', 'etl', 'pipeline']
)


extract = PythonOperator(
    task_id = 'reddit_extraction',
    python_callable=reddit_pipeline,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}',
        'subreddit': 'lakers',
        'limit': 100
    }
)