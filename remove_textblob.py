#!/usr/bin/env python3
"""
Remove TextBlob from sentiment analysis and keep only Vader and Transformer
"""

import sys
import os
from sqlalchemy import text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_processors.database_manager import DatabaseManager

def remove_textblob():
    """Remove TextBlob from sentiment analysis"""
    
    db_manager = DatabaseManager()
    
    try:
        with db_manager.engine.connect() as conn:
            print("REMOVING TEXTBLOB FROM SENTIMENT ANALYSIS")
            print("=" * 50)
            
            # Drop TextBlob columns from sentiment_scores table
            print("Dropping TextBlob columns...")
            
            try:
                # Drop textblob columns
                drop_columns = [
                    "ALTER TABLE sentiment_scores DROP COLUMN IF EXISTS textblob_polarity",
                    "ALTER TABLE sentiment_scores DROP COLUMN IF EXISTS textblob_subjectivity", 
                    "ALTER TABLE sentiment_scores DROP COLUMN IF EXISTS textblob_sentiment"
                ]
                
                for query in drop_columns:
                    conn.execute(text(query))
                
                conn.commit()
                print("SUCCESS: TextBlob columns removed from database")
                
            except Exception as e:
                print(f"WARNING: Could not drop columns: {e}")
            
            # Check current data
            count_query = text("SELECT COUNT(*) FROM sentiment_scores")
            result = conn.execute(count_query)
            count = result.fetchone()[0]
            print(f"Current sentiment records: {count}")
            
            print("\nTextBlob removal complete!")
            print("Next steps:")
            print("1. Update sentiment analyzer to remove TextBlob")
            print("2. Re-run sentiment analysis with Vader + Transformer only")
            print("3. Update API to reflect changes")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    remove_textblob()

