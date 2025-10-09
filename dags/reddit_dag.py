from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import sys
import os 

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from workflows.reddit_workflow import reddit_pipeline
from workflows.aws_workflow import upload_s3_pipeline
from data_processors.streamlined_sentiment_pipeline import StreamlinedSentimentPipeline

default_args = {
    'owner': 'Nawoda Wijesooriya',
    'start_date': datetime(2025, 7, 19)
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['reddit', 'etl', 'pipeline']
)


def run_streamlined_sentiment_pipeline():
    """Run the streamlined sentiment analysis pipeline using S3 as source"""
    pipeline = StreamlinedSentimentPipeline()
    results = pipeline.run_full_pipeline(days=30)
    return results

extract = PythonOperator(
    task_id='reddit_extraction',
    python_callable=reddit_pipeline,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}',
        'subreddit': 'lakers',
        'time_filter': 'month',
        'limit': 100
    },
    dag=dag
)

upload_s3 = PythonOperator(
    task_id = 's3_upload',
    python_callable = upload_s3_pipeline,
    dag = dag
)

sentiment_analysis = PythonOperator(
    task_id='sentiment_analysis',
    python_callable=run_streamlined_sentiment_pipeline,
    dag=dag
)

# Task dependencies
extract >> upload_s3 >> sentiment_analysis
