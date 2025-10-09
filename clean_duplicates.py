#!/usr/bin/env python3
"""
Clean up duplicate sentiment records and fix database constraints
"""

import sys
import os
from sqlalchemy import text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_processors.database_manager import DatabaseManager

def clean_duplicates():
    """Clean up duplicate sentiment records"""
    
    db_manager = DatabaseManager()
    
    try:
        with db_manager.engine.connect() as conn:
            print("CLEANING UP DUPLICATE RECORDS")
            print("=" * 50)
            
            # First, let's see what we have
            count_query = text("""
                SELECT COUNT(*) as total_records
                FROM sentiment_scores
            """)
            
            result = conn.execute(count_query)
            total_before = result.fetchone()[0]
            print(f"Total sentiment records before cleanup: {total_before}")
            
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
            
            # Verify no duplicates remain
            duplicates_query = text("""
                SELECT post_id, COUNT(*) as count
                FROM sentiment_scores
                GROUP BY post_id
                HAVING COUNT(*) > 1
            """)
            
            duplicates_result = conn.execute(duplicates_query)
            remaining_duplicates = duplicates_result.fetchall()
            
            if remaining_duplicates:
                print(f"WARNING: {len(remaining_duplicates)} posts still have duplicates!")
                for post_id, count in remaining_duplicates:
                    print(f"  Post {post_id}: {count} records")
            else:
                print("SUCCESS: No duplicates remaining!")
            
            # Add unique constraint to prevent future duplicates
            print("\nAdding database constraints...")
            
            try:
                # Add unique constraint on post_id
                constraint_query = text("""
                    ALTER TABLE sentiment_scores 
                    ADD CONSTRAINT unique_post_sentiment 
                    UNIQUE (post_id)
                """)
                conn.execute(constraint_query)
                conn.commit()
                print("SUCCESS: Added unique constraint on post_id")
            except Exception as e:
                if "already exists" in str(e):
                    print("SUCCESS: Unique constraint already exists")
                else:
                    print(f"WARNING: Could not add constraint: {e}")
            
            print("\nDatabase cleanup complete!")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    clean_duplicates()
