from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import sys
import os 

sys.path.insert(0, '/opt/airflow')

default_args = {
    'owner': 'Nawoda Wijesooriya',
    'start_date': datetime(2025, 7, 19)
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id='simple_reddit_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['reddit', 'etl', 'pipeline']
)

def simple_reddit_extraction():
    """Simple Reddit data extraction"""
    try:
        from workflows.reddit_workflow import reddit_pipeline
        result = reddit_pipeline(
            file_name=f'reddit_{file_postfix}',
            subreddit='lakers',
            time_filter='month',
            limit=100
        )
        print(f"Reddit extraction completed: {result}")
        return result
    except Exception as e:
        print(f"Error in Reddit extraction: {e}")
        return None

def simple_s3_upload():
    """Simple S3 upload"""
    try:
        from workflows.aws_workflow import upload_s3_pipeline
        result = upload_s3_pipeline()
        print(f"S3 upload completed: {result}")
        return result
    except Exception as e:
        print(f"Error in S3 upload: {e}")
        return None

def simple_sentiment_analysis():
    """Simple sentiment analysis using basic libraries"""
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        from textblob import TextBlob
        import pandas as pd
        
        # Initialize VADER sentiment analyzer
        analyzer = SentimentIntensityAnalyzer()
        
        # This is a placeholder - in a real implementation, you'd read from your data source
        sample_texts = [
            "The Lakers played great today!",
            "I'm disappointed with the team's performance.",
            "LeBron James is amazing!",
            "The game was okay, nothing special."
        ]
        
        results = []
        for text in sample_texts:
            # VADER sentiment
            vader_scores = analyzer.polarity_scores(text)
            
            # TextBlob sentiment
            blob = TextBlob(text)
            textblob_polarity = blob.sentiment.polarity
            textblob_subjectivity = blob.sentiment.subjectivity
            
            results.append({
                'text': text,
                'vader_compound': vader_scores['compound'],
                'vader_positive': vader_scores['pos'],
                'vader_negative': vader_scores['neg'],
                'vader_neutral': vader_scores['neu'],
                'textblob_polarity': textblob_polarity,
                'textblob_subjectivity': textblob_subjectivity
            })
        
        df = pd.DataFrame(results)
        print("Sentiment analysis completed:")
        print(df)
        return df.to_dict('records')
        
    except Exception as e:
        print(f"Error in sentiment analysis: {e}")
        return None

# Define tasks
extract = PythonOperator(
    task_id='reddit_extraction',
    python_callable=simple_reddit_extraction,
    dag=dag
)

upload_s3 = PythonOperator(
    task_id='s3_upload',
    python_callable=simple_s3_upload,
    dag=dag
)

sentiment_analysis = PythonOperator(
    task_id='sentiment_analysis',
    python_callable=simple_sentiment_analysis,
    dag=dag
)

# Task dependencies
extract >> upload_s3 >> sentiment_analysis
