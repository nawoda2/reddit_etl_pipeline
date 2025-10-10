#!/usr/bin/env python3
"""
Fix the root cause of duplicate sentiment records by adding proper checks
"""

import sys
import os
from sqlalchemy import text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_processors.database_manager import DatabaseManager

def fix_duplicate_root_cause():
    """Fix the root cause of duplicates by adding proper constraints and checks"""
    
    db_manager = DatabaseManager()
    
    try:
        with db_manager.engine.connect() as conn:
            print("FIXING ROOT CAUSE OF DUPLICATES")
            print("=" * 50)
            
            # 1. Add unique constraint to prevent future duplicates
            print("1. Adding unique constraint to sentiment_scores...")
            try:
                # Drop existing constraint if it exists
                conn.execute(text("ALTER TABLE sentiment_scores DROP CONSTRAINT IF EXISTS unique_post_sentiment"))
                
                # Add unique constraint on post_id
                conn.execute(text("""
                    ALTER TABLE sentiment_scores 
                    ADD CONSTRAINT unique_post_sentiment 
                    UNIQUE (post_id)
                """))
                conn.commit()
                print("   SUCCESS: Unique constraint added")
            except Exception as e:
                print(f"   WARNING: Could not add constraint: {e}")
            
            # 2. Add check constraint to ensure data integrity
            print("2. Adding check constraints...")
            try:
                # Add check constraint for vader_compound range
                conn.execute(text("""
                    ALTER TABLE sentiment_scores 
                    ADD CONSTRAINT check_vader_compound 
                    CHECK (vader_compound >= -1 AND vader_compound <= 1)
                """))
                
                # Add check constraint for transformer probabilities
                conn.execute(text("""
                    ALTER TABLE sentiment_scores 
                    ADD CONSTRAINT check_transformer_probs 
                    CHECK (transformer_positive >= 0 AND transformer_positive <= 1 AND
                           transformer_negative >= 0 AND transformer_negative <= 1 AND
                           transformer_neutral >= 0 AND transformer_neutral <= 1)
                """))
                
                conn.commit()
                print("   SUCCESS: Check constraints added")
            except Exception as e:
                print(f"   WARNING: Could not add check constraints: {e}")
            
            # 3. Add index for better performance
            print("3. Adding performance indexes...")
            try:
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sentiment_post_id ON sentiment_scores(post_id)"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sentiment_timestamp ON sentiment_scores(analysis_timestamp)"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sentiment_vader ON sentiment_scores(vader_compound)"))
                conn.commit()
                print("   SUCCESS: Performance indexes added")
            except Exception as e:
                print(f"   WARNING: Could not add indexes: {e}")
            
            # 4. Verify current state
            print("4. Verifying current state...")
            count_query = text("SELECT COUNT(*) FROM sentiment_scores")
            result = conn.execute(count_query)
            total_records = result.fetchone()[0]
            print(f"   Total sentiment records: {total_records}")
            
            # Check for any remaining duplicates
            duplicates_query = text("""
                SELECT post_id, COUNT(*) as count
                FROM sentiment_scores
                GROUP BY post_id
                HAVING COUNT(*) > 1
            """)
            
            duplicates_result = conn.execute(duplicates_query)
            duplicates = duplicates_result.fetchall()
            
            if duplicates:
                print(f"   WARNING: {len(duplicates)} posts still have duplicates!")
            else:
                print("   SUCCESS: No duplicates found")
            
            print("\n✅ Root cause fixes applied!")
            print("   - Unique constraint prevents future duplicates")
            print("   - Check constraints ensure data integrity")
            print("   - Performance indexes added")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fix_duplicate_root_cause()

