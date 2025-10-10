from utils.constants import CLIENT_ID, SECRET, OUTPUT_PATH
from data_collectors.reddit_collector import connect_reddit, extract_posts, transform_data, load_data_to_csv
import pandas as pd
from datetime import datetime, timedelta
import os

def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit=None):
    """
    Reddit data pipeline with daily partitioning and deduplication
    
    Args:
        file_name: Base filename (will be partitioned by date)
        subreddit: Subreddit to extract from
        time_filter: Time filter for Reddit API ('day', 'week', 'month')
        limit: Maximum number of posts to extract
    """
    instance = connect_reddit(CLIENT_ID, SECRET, 'example_user_agent')
    
    # Extract posts
    posts = extract_posts(instance, subreddit, time_filter, limit)
    post_df = pd.DataFrame(posts)
    
    if post_df.empty:
        print("No posts found for the specified time period")
        return None
    
    # Transform data
    post_df = transform_data(post_df)
    
    # Add partitioning columns
    post_df['extraction_date'] = datetime.now().strftime('%Y-%m-%d')
    post_df['year'] = post_df['created_utc'].dt.year
    post_df['month'] = post_df['created_utc'].dt.month
    post_df['day'] = post_df['created_utc'].dt.day
    
    # Create partitioned file path
    current_date = datetime.now()
    partition_path = f"{current_date.year:04d}/{current_date.month:02d}/{current_date.day:02d}"
    
    # Create directory structure
    full_output_path = f'{OUTPUT_PATH}/partitioned/{partition_path}'
    os.makedirs(full_output_path, exist_ok=True)
    
    # Save with timestamp to avoid conflicts
    timestamp = datetime.now().strftime('%H%M%S')
    file_path = f'{full_output_path}/{file_name}_{timestamp}.csv'
    
    load_data_to_csv(post_df, file_path)
    
    print(f"Data saved to partitioned location: {file_path}")
    print(f"Total posts extracted: {len(post_df)}")
    
    return file_path