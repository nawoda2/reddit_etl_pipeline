from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import sys
import os 

# Don't import heavy modules at the top level - this causes DAG parsing errors
# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# from workflows.reddit_workflow import reddit_pipeline
# from workflows.aws_workflow import upload_s3_pipeline
# from data_processors.streamlined_sentiment_pipeline import StreamlinedSentimentPipeline

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


def reddit_extraction_task(**context):
    """Reddit extraction task with imports inside function"""
    # Add path and import inside function to avoid DAG parsing errors
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from workflows.reddit_workflow import reddit_pipeline
    
    file_name = f'reddit_{file_postfix}'
    subreddit = 'lakers'
    time_filter = 'month'
    limit = 100
    
    return reddit_pipeline(file_name, subreddit, time_filter, limit)


def s3_upload_task(**context):
    """S3 upload task with imports inside function"""
    # Add path and import inside function to avoid DAG parsing errors
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from workflows.aws_workflow import upload_s3_pipeline
    
    # Get file path from previous task
    ti = context['ti']
    return upload_s3_pipeline(ti)


def sentiment_analysis_task(**context):
    """Sentiment analysis task with imports inside function"""
    # Add path and import inside function to avoid DAG parsing errors
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from data_processors.streamlined_sentiment_pipeline import StreamlinedSentimentPipeline
    
    pipeline = StreamlinedSentimentPipeline()
    results = pipeline.run_full_pipeline(days=30)
    return results


extract = PythonOperator(
    task_id='reddit_extraction',
    python_callable=reddit_extraction_task,
    dag=dag
)

upload_s3 = PythonOperator(
    task_id='s3_upload',
    python_callable=s3_upload_task,
    dag=dag
)

sentiment_analysis = PythonOperator(
    task_id='sentiment_analysis',
    python_callable=sentiment_analysis_task,
    dag=dag
)

# Task dependencies
extract >> upload_s3 >> sentiment_analysis
