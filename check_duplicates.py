#!/usr/bin/env python3
"""
Check for duplicate posts in the database
"""

import sys
import os
from sqlalchemy import text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_processors.database_manager import DatabaseManager

def check_duplicates():
    """Check for duplicate posts and their causes"""
    
    db_manager = DatabaseManager()
    
    try:
        with db_manager.engine.connect() as conn:
            print("CHECKING FOR DUPLICATE POSTS")
            print("=" * 50)
            
            # Check total posts vs unique posts
            total_query = text("""
                SELECT COUNT(*) as total_posts
                FROM reddit_posts
            """)
            
            unique_query = text("""
                SELECT COUNT(DISTINCT id) as unique_posts
                FROM reddit_posts
            """)
            
            total_result = conn.execute(total_query)
            unique_result = conn.execute(unique_query)
            
            total_posts = total_result.fetchone()[0]
            unique_posts = unique_result.fetchone()[0]
            
            print(f"Total posts in database: {total_posts}")
            print(f"Unique post IDs: {unique_posts}")
            print(f"Duplicates: {total_posts - unique_posts}")
            print()
            
            # Find duplicate post IDs
            duplicates_query = text("""
                SELECT id, COUNT(*) as count
                FROM reddit_posts
                GROUP BY id
                HAVING COUNT(*) > 1
                ORDER BY count DESC
            """)
            
            duplicates_result = conn.execute(duplicates_query)
            duplicates = duplicates_result.fetchall()
            
            print("DUPLICATE POSTS:")
            print("-" * 30)
            for post_id, count in duplicates:
                print(f"Post ID: {post_id} - appears {count} times")
            
            print()
            
            # Check sentiment_scores table
            sentiment_query = text("""
                SELECT COUNT(*) as total_sentiment_records
                FROM sentiment_scores
            """)
            
            sentiment_result = conn.execute(sentiment_query)
            sentiment_count = sentiment_result.fetchone()[0]
            
            print(f"Total sentiment records: {sentiment_count}")
            print(f"Expected sentiment records: {unique_posts}")
            print(f"Extra sentiment records: {sentiment_count - unique_posts}")
            print()
            
            # Check if sentiment analysis was run multiple times
            sentiment_duplicates_query = text("""
                SELECT post_id, COUNT(*) as count
                FROM sentiment_scores
                GROUP BY post_id
                HAVING COUNT(*) > 1
                ORDER BY count DESC
            """)
            
            sentiment_duplicates_result = conn.execute(sentiment_duplicates_query)
            sentiment_duplicates = sentiment_duplicates_result.fetchall()
            
            print("DUPLICATE SENTIMENT RECORDS:")
            print("-" * 35)
            for post_id, count in sentiment_duplicates:
                print(f"Post ID: {post_id} - has {count} sentiment records")
            
            print()
            
            # Show sample duplicate post details
            if duplicates:
                sample_post_id = duplicates[0][0]
                sample_query = text("""
                    SELECT 
                        rp.id,
                        rp.title,
                        rp.author,
                        rp.score,
                        rp.created_utc,
                        ss.analysis_timestamp
                    FROM reddit_posts rp
                    JOIN sentiment_scores ss ON rp.id = ss.post_id
                    WHERE rp.id = :post_id
                    ORDER BY ss.analysis_timestamp
                """)
                
                sample_result = conn.execute(sample_query, {"post_id": sample_post_id})
                sample_records = sample_result.fetchall()
                
                print(f"SAMPLE DUPLICATE POST ({sample_post_id}):")
                print("-" * 40)
                for i, record in enumerate(sample_records):
                    print(f"Record {i+1}:")
                    print(f"  Title: {record[1][:50]}...")
                    print(f"  Author: {record[2]}")
                    print(f"  Score: {record[3]}")
                    print(f"  Created: {record[4]}")
                    print(f"  Analysis Time: {record[5]}")
                    print()
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_duplicates()

