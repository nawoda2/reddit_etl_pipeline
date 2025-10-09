#!/usr/bin/env python3
"""
Create the reddit_comments table in the database
"""

import sys
import os
from sqlalchemy import text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_processors.database_manager import DatabaseManager, RedditComment

def create_comments_table():
    """Create the reddit_comments table"""
    
    db_manager = DatabaseManager()
    
    try:
        with db_manager.engine.connect() as conn:
            print("Creating reddit_comments table...")
            
            # Create the table using the model definition
            RedditComment.__table__.create(db_manager.engine, checkfirst=True)
            
            print("SUCCESS: reddit_comments table created!")
            
            # Verify the table was created
            verify_query = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name = 'reddit_comments'
            """)
            
            result = conn.execute(verify_query)
            if result.fetchone():
                print("SUCCESS: Table verified in database")
            else:
                print("ERROR: Table not found in database")
                
    except Exception as e:
        print(f"ERROR: Failed to create table: {e}")

if __name__ == "__main__":
    create_comments_table()

