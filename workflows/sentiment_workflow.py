"""
Sentiment Analysis Pipeline for Reddit Data
Combines data extraction, sentiment analysis, and database storage
"""
import pandas as pd
from datetime import datetime
from data_processors.sentiment_analyzer import analyze_reddit_sentiment
from data_processors.database_manager import store_reddit_data_with_sentiment
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def sentiment_analysis_pipeline(file_path: str, subreddit: str) -> dict:
    """
    Complete sentiment analysis pipeline for Reddit data
    
    Args:
        file_path: Path to the CSV file with Reddit posts
        subreddit: Name of the subreddit
    
    Returns:
        dict: Summary of the analysis results
    """
    try:
        logger.info(f"Starting sentiment analysis for {subreddit} data from {file_path}")
        
        # Load the data
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} posts for analysis")
        
        # Perform sentiment analysis
        processed_df, summary = analyze_reddit_sentiment(df)
        
        # Get current date for daily summary
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        # Store in database
        success = store_reddit_data_with_sentiment(
            processed_df, 
            summary, 
            subreddit, 
            current_date
        )
        
        if success:
            logger.info("Sentiment analysis pipeline completed successfully")
            return {
                'status': 'success',
                'posts_processed': len(processed_df),
                'sentiment_summary': summary,
                'database_stored': True
            }
        else:
            logger.error("Failed to store data in database")
            return {
                'status': 'error',
                'posts_processed': len(processed_df),
                'sentiment_summary': summary,
                'database_stored': False
            }
            
    except Exception as e:
        logger.error(f"Sentiment analysis pipeline failed: {e}")
        return {
            'status': 'error',
            'error_message': str(e),
            'posts_processed': 0,
            'database_stored': False
        }

