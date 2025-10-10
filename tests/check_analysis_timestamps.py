#!/usr/bin/env python3
"""
Check analysis timestamps to understand when duplicates were created
"""

import sys
import os
from sqlalchemy import text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_processors.database_manager import DatabaseManager

def check_analysis_timestamps():
    """Check when sentiment analysis was run"""
    
    db_manager = DatabaseManager()
    
    try:
        with db_manager.engine.connect() as conn:
            print("ANALYSIS TIMESTAMP INVESTIGATION")
            print("=" * 50)
            
            # Check unique analysis timestamps
            timestamps_query = text("""
                SELECT 
                    analysis_timestamp,
                    COUNT(*) as record_count
                FROM sentiment_scores
                GROUP BY analysis_timestamp
                ORDER BY analysis_timestamp
            """)
            
            result = conn.execute(timestamps_query)
            timestamps = result.fetchall()
            
            print("ANALYSIS RUNS:")
            print("-" * 20)
            for timestamp, count in timestamps:
                print(f"{timestamp}: {count} records")
            
            print()
            
            # Check a specific post with many duplicates
            sample_query = text("""
                SELECT 
                    post_id,
                    analysis_timestamp,
                    vader_compound,
                    textblob_sentiment,
                    transformer_sentiment
                FROM sentiment_scores
                WHERE post_id = '1o0xmzn'
                ORDER BY analysis_timestamp
            """)
            
            result = conn.execute(sample_query)
            sample_records = result.fetchall()
            
            print("SAMPLE POST WITH MANY DUPLICATES (1o0xmzn):")
            print("-" * 50)
            for i, record in enumerate(sample_records):
                print(f"Run {i+1}: {record[1]}")
                print(f"  Vader: {record[2]:.4f}")
                print(f"  TextBlob: {record[3]}")
                print(f"  Transformer: {record[4]}")
                print()
            
            # Check if the issue is in the sentiment analysis workflow
            print("POSSIBLE CAUSES:")
            print("-" * 20)
            print("1. Sentiment analysis was run multiple times")
            print("2. The workflow didn't check for existing records")
            print("3. Database constraints weren't properly set")
            print("4. Multiple API calls or script runs")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_analysis_timestamps()

