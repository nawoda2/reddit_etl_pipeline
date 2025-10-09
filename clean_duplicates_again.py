#!/usr/bin/env python3
"""
Clean up duplicate sentiment records again
"""

import sys
import os
from sqlalchemy import text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_processors.database_manager import DatabaseManager

def clean_duplicates_again():
    """Clean up duplicate sentiment records"""
    
    db_manager = DatabaseManager()
    
    try:
        with db_manager.engine.connect() as conn:
            print("CLEANING UP DUPLICATES AGAIN")
            print("=" * 50)
            
            # Check current state
            count_query = text("SELECT COUNT(*) FROM sentiment_scores")
            result = conn.execute(count_query)
            total_before = result.fetchone()[0]
            print(f"Total sentiment records before cleanup: {total_before}")
            
            # Check for duplicates
            duplicates_query = text("""
                SELECT post_id, COUNT(*) as count
                FROM sentiment_scores
                GROUP BY post_id
                HAVING COUNT(*) > 1
                ORDER BY count DESC
            """)
            
            duplicates_result = conn.execute(duplicates_query)
            duplicates = duplicates_result.fetchall()
            
            if duplicates:
                print(f"Found {len(duplicates)} posts with duplicates:")
                for post_id, count in duplicates:
                    print(f"  Post {post_id}: {count} records")
                
                # Keep only the most recent analysis for each post
                cleanup_query = text("""
                    DELETE FROM sentiment_scores 
                    WHERE id NOT IN (
                        SELECT DISTINCT ON (post_id) id
                        FROM sentiment_scores
                        ORDER BY post_id, analysis_timestamp DESC
                    )
                """)
                
                conn.execute(cleanup_query)
                conn.commit()
                
                # Check results
                result = conn.execute(count_query)
                total_after = result.fetchone()[0]
                print(f"Total sentiment records after cleanup: {total_after}")
                print(f"Records removed: {total_before - total_after}")
            else:
                print("No duplicates found!")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    clean_duplicates_again()

