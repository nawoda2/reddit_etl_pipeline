#!/usr/bin/env python3
"""
Re-run sentiment analysis with only Vader and Transformer
"""

import sys
import os
import pandas as pd
from sqlalchemy import text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_processors.database_manager import DatabaseManager
from data_processors.sentiment_analyzer import analyze_reddit_sentiment

def rerun_analysis():
    """Re-run sentiment analysis with Vader and Transformer only"""
    
    db_manager = DatabaseManager()
    
    try:
        # Clear existing sentiment data
        with db_manager.engine.connect() as conn:
            print("Clearing existing sentiment data...")
            conn.execute(text("DELETE FROM sentiment_scores"))
            conn.commit()
            print("SUCCESS: Existing sentiment data cleared")
        
        # Get posts from database
        with db_manager.engine.connect() as conn:
            query = text("""
                SELECT id, title, selftext, author, score, num_comments, upvote_ratio, created_utc
                FROM reddit_posts
                ORDER BY score DESC
            """)
            
            result = conn.execute(query)
            posts_data = []
            
            for row in result:
                posts_data.append({
                    'id': row[0],
                    'title': row[1],
                    'selftext': row[2] or '',
                    'author': row[3],
                    'score': row[4],
                    'num_comments': row[5],
                    'upvote_ratio': row[6],
                    'created_utc': row[7]
                })
            
            print(f"Loaded {len(posts_data)} posts for re-analysis")
            
            # Convert to DataFrame
            df = pd.DataFrame(posts_data)
            
            # Re-run sentiment analysis with Vader and Transformer only
            print("Running sentiment analysis with Vader and Transformer...")
            processed_df, summary = analyze_reddit_sentiment(df, text_column='title')
            
            print(f"Analysis complete!")
            print(f"Total posts: {summary['total_posts']}")
            print(f"Average sentiment: {summary['average_sentiment']:.3f}")
            print(f"Positive posts: {summary['positive_posts']}")
            print(f"Negative posts: {summary['negative_posts']}")
            print(f"Neutral posts: {summary['neutral_posts']}")
            
            # Store results in database
            print("Storing results in database...")
            from data_processors.database_manager import store_reddit_data_with_sentiment
            
            success = store_reddit_data_with_sentiment(
                processed_df, 
                summary, 
                'lakers', 
                '2025-10-08'
            )
            
            if success:
                print("SUCCESS: Results stored in database")
            else:
                print("ERROR: Failed to store results")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    rerun_analysis()

