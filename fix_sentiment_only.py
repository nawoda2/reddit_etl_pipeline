#!/usr/bin/env python3
"""
Fix sentiment analysis by only updating sentiment scores, not posts
"""

import sys
import os
import pandas as pd
from sqlalchemy import text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_processors.database_manager import DatabaseManager
from data_processors.sentiment_analyzer import analyze_reddit_sentiment

def fix_sentiment_analysis():
    """Fix sentiment analysis by only updating sentiment scores"""
    
    db_manager = DatabaseManager()
    
    try:
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
            
            print(f"Loaded {len(posts_data)} posts for sentiment analysis")
            
            # Convert to DataFrame
            df = pd.DataFrame(posts_data)
            
            # Run sentiment analysis with Vader and Transformer only
            print("Running sentiment analysis with Vader and Transformer...")
            processed_df, summary = analyze_reddit_sentiment(df, text_column='title')
            
            print(f"Analysis complete!")
            print(f"Total posts: {summary['total_posts']}")
            print(f"Average sentiment: {summary['average_sentiment']:.3f}")
            print(f"Positive posts: {summary['positive_posts']}")
            print(f"Negative posts: {summary['negative_posts']}")
            print(f"Neutral posts: {summary['neutral_posts']}")
            
            # Insert only sentiment scores
            print("Inserting sentiment scores...")
            
            for _, row in processed_df.iterrows():
                # Check if sentiment record already exists
                check_query = text("SELECT COUNT(*) FROM sentiment_scores WHERE post_id = :post_id")
                result = conn.execute(check_query, {"post_id": row['id']})
                exists = result.fetchone()[0] > 0
                
                if exists:
                    # Update existing record
                    update_query = text("""
                        UPDATE sentiment_scores 
                        SET 
                            vader_positive = :vader_positive,
                            vader_negative = :vader_negative,
                            vader_neutral = :vader_neutral,
                            vader_compound = :vader_compound,
                            transformer_positive = :transformer_positive,
                            transformer_negative = :transformer_negative,
                            transformer_neutral = :transformer_neutral,
                            transformer_sentiment = :transformer_sentiment,
                            analysis_timestamp = :analysis_timestamp
                        WHERE post_id = :post_id
                    """)
                    
                    conn.execute(update_query, {
                        'post_id': row['id'],
                        'vader_positive': float(row.get('vader_positive', 0.0)),
                        'vader_negative': float(row.get('vader_negative', 0.0)),
                        'vader_neutral': float(row.get('vader_neutral', 1.0)),
                        'vader_compound': float(row.get('vader_compound', 0.0)),
                        'transformer_positive': float(row.get('transformer_positive', 0.0)),
                        'transformer_negative': float(row.get('transformer_negative', 0.0)),
                        'transformer_neutral': float(row.get('transformer_neutral', 1.0)),
                        'transformer_sentiment': str(row.get('transformer_sentiment', 'neutral')),
                        'analysis_timestamp': row.get('analysis_timestamp', '2025-10-08T18:00:00')
                    })
                else:
                    # Insert new record
                    insert_query = text("""
                        INSERT INTO sentiment_scores 
                        (post_id, vader_positive, vader_negative, vader_neutral, vader_compound,
                         transformer_positive, transformer_negative, transformer_neutral, transformer_sentiment, analysis_timestamp)
                        VALUES 
                        (:post_id, :vader_positive, :vader_negative, :vader_neutral, :vader_compound,
                         :transformer_positive, :transformer_negative, :transformer_neutral, :transformer_sentiment, :analysis_timestamp)
                    """)
                    
                    conn.execute(insert_query, {
                        'post_id': row['id'],
                        'vader_positive': float(row.get('vader_positive', 0.0)),
                        'vader_negative': float(row.get('vader_negative', 0.0)),
                        'vader_neutral': float(row.get('vader_neutral', 1.0)),
                        'vader_compound': float(row.get('vader_compound', 0.0)),
                        'transformer_positive': float(row.get('transformer_positive', 0.0)),
                        'transformer_negative': float(row.get('transformer_negative', 0.0)),
                        'transformer_neutral': float(row.get('transformer_neutral', 1.0)),
                        'transformer_sentiment': str(row.get('transformer_sentiment', 'neutral')),
                        'analysis_timestamp': row.get('analysis_timestamp', '2025-10-08T18:00:00')
                    })
            
            conn.commit()
            print("SUCCESS: Sentiment scores updated!")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fix_sentiment_analysis()

