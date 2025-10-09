#!/usr/bin/env python3
"""
Export sentiment analysis database to CSV
"""

import sys
import os
import pandas as pd
from datetime import datetime
from sqlalchemy import text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_processors.database_manager import DatabaseManager

def export_sentiment_csv():
    """Export sentiment analysis data to CSV"""
    
    db_manager = DatabaseManager()
    
    try:
        with db_manager.engine.connect() as conn:
            # Get all posts with sentiment data
            query = text("""
                SELECT 
                    rp.id,
                    rp.title,
                    rp.selftext,
                    rp.author,
                    rp.score,
                    rp.num_comments,
                    rp.upvote_ratio,
                    rp.created_utc,
                    rp.subreddit,
                    ss.vader_compound,
                    ss.vader_positive,
                    ss.vader_negative,
                    ss.vader_neutral,
                    -- TextBlob fields removed
                    ss.transformer_positive,
                    ss.transformer_negative,
                    ss.transformer_neutral,
                    ss.transformer_sentiment,
                    ss.analysis_timestamp
                FROM reddit_posts rp
                JOIN sentiment_scores ss ON rp.id = ss.post_id
                ORDER BY rp.score DESC
            """)
            
            result = conn.execute(query)
            posts = result.fetchall()
            
            # Convert to DataFrame
            df = pd.DataFrame(posts, columns=[
                'post_id', 'title', 'selftext', 'author', 'score', 'num_comments',
                'upvote_ratio', 'created_utc', 'subreddit', 'vader_compound',
                'vader_positive', 'vader_negative', 'vader_neutral',
                # TextBlob fields removed
                'transformer_positive', 'transformer_negative', 'transformer_neutral',
                'transformer_sentiment', 'analysis_timestamp'
            ])
            
            # Add derived columns
            df['vader_classification'] = df['vader_compound'].apply(
                lambda x: 'positive' if x > 0.05 else 'negative' if x < -0.05 else 'neutral'
            )
            
            df['agreement'] = df.apply(lambda row: 
                'all_agree' if (
                    row['vader_classification'] == row['transformer_sentiment']
                ) else 'disagree', axis=1
            )
            
            # Create timestamp for filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Export to CSV
            csv_filename = f"sentiment_analysis_{timestamp}.csv"
            df.to_csv(csv_filename, index=False)
            
            print(f"Sentiment analysis data exported to: {csv_filename}")
            print(f"Total records: {len(df)}")
            print(f"Columns: {list(df.columns)}")
            
            # Show sample data
            print("\nSample data:")
            print(df[['post_id', 'title', 'vader_classification', 'transformer_sentiment', 'agreement']].head())
            
            return csv_filename
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    export_sentiment_csv()
