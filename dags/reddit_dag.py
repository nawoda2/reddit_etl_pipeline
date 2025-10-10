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
    sys.path.insert(0, '/opt/airflow')
    from workflows.reddit_workflow import reddit_pipeline
    
    file_name = f'reddit_{file_postfix}'
    subreddit = 'lakers'
    time_filter = 'day'  # Changed to daily for better partitioning
    limit = 100
    
    return reddit_pipeline(file_name, subreddit, time_filter, limit)


def s3_upload_task(**context):
    """S3 upload task with imports inside function"""
    # Add path and import inside function to avoid DAG parsing errors
    sys.path.insert(0, '/opt/airflow')
    from workflows.aws_workflow import upload_s3_pipeline
    
    # Get file path from previous task
    ti = context['ti']
    return upload_s3_pipeline(ti)


def sentiment_analysis_task(**context):
    """Sentiment analysis task with imports inside function"""
    # Add path and import inside function to avoid DAG parsing errors
    sys.path.insert(0, '/opt/airflow')
    from data_processors.streamlined_sentiment_pipeline import StreamlinedSentimentPipeline
    
    pipeline = StreamlinedSentimentPipeline()
    results = pipeline.run_full_pipeline(days=7)  # Changed to 7 days for daily pipeline
    return results


def weekly_analytics_task(**context):
    """Weekly analytics task for comprehensive analysis"""
    # Add path and import inside function to avoid DAG parsing errors
    sys.path.insert(0, '/opt/airflow')
    from workflows.weekly_analytics_workflow import weekly_analytics_pipeline
    
    results = weekly_analytics_pipeline(weeks=1)
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

weekly_analytics = PythonOperator(
    task_id='weekly_analytics',
    python_callable=weekly_analytics_task,
    dag=dag
)

# Task dependencies
extract >> upload_s3 >> sentiment_analysis
sentiment_analysis >> weekly_analytics
