#!/usr/bin/env python3
"""
Create individual tables for each Lakers player and populate with NER data
"""

import sys
import os
import pandas as pd
from sqlalchemy import text
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_processors.database_manager import DatabaseManager
from data_processors.ner_processor import LakersNERProcessor
from utils.lakers_roster import LAKERS_ROSTER

def create_player_tables():
    """Create individual tables for each Lakers player"""
    
    db_manager = DatabaseManager()
    ner_processor = LakersNERProcessor()
    
    try:
        with db_manager.engine.connect() as conn:
            print("CREATING LAKERS PLAYER TABLES")
            print("=" * 50)
            
            # Create tables for each player
            for player_name, player_info in LAKERS_ROSTER.items():
                print(f"Creating table for {player_name}...")
                
                # Create player-specific mention table
                table_name = f"player_mentions_{player_name.lower().replace(' ', '_').replace('č', 'c')}"
                
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    mention_id SERIAL PRIMARY KEY,
                    post_id VARCHAR(50),
                    comment_id VARCHAR(50),
                    player_name VARCHAR(255) NOT NULL,
                    alias_found VARCHAR(255),
                    context TEXT,
                    confidence DECIMAL(3,2),
                    mention_type VARCHAR(50),
                    post_title TEXT,
                    post_author VARCHAR(255),
                    post_score INTEGER,
                    comment_author VARCHAR(255),
                    comment_score INTEGER,
                    created_utc TIMESTAMP,
                    is_submitter BOOLEAN DEFAULT FALSE,
                    subreddit VARCHAR(100),
                    analysis_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                
                conn.execute(text(create_table_sql))
                
                # Create indexes for performance
                indexes = [
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_post_id ON {table_name}(post_id)",
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_comment_id ON {table_name}(comment_id)",
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_created_utc ON {table_name}(created_utc)",
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}_confidence ON {table_name}(confidence)"
                ]
                
                for idx_sql in indexes:
                    conn.execute(text(idx_sql))
                
                print(f"   ✅ Table {table_name} created with indexes")
            
            conn.commit()
            print("\n✅ ALL PLAYER TABLES CREATED SUCCESSFULLY!")
            
    except Exception as e:
        print(f"Error creating player tables: {e}")
        import traceback
        traceback.print_exc()

def populate_player_tables():
    """Populate player tables with existing data"""
    
    db_manager = DatabaseManager()
    ner_processor = LakersNERProcessor()
    
    try:
        print("\nPOPULATING PLAYER TABLES WITH EXISTING DATA")
        print("=" * 50)
        
        # Get all Reddit posts
        print("Fetching Reddit posts...")
        posts_query = text("""
            SELECT id, title, selftext, author, subreddit, score, created_utc
            FROM reddit_posts
            ORDER BY created_utc DESC
        """)
        
        with db_manager.engine.connect() as conn:
            posts_df = pd.read_sql(posts_query, conn)
            print(f"Found {len(posts_df)} Reddit posts")
            
            if not posts_df.empty:
                # Process posts for player mentions
                print("Processing posts for player mentions...")
                processed_posts, post_mentions = ner_processor.process_reddit_posts(posts_df)
                
                # Insert mentions into player tables
                if not post_mentions.empty:
                    print(f"Found {len(post_mentions)} player mentions in posts")
                    _insert_mentions_to_player_tables(conn, post_mentions, 'post')
                else:
                    print("No player mentions found in posts")
            
            # Get all Reddit comments
            print("\nFetching Reddit comments...")
            comments_query = text("""
                SELECT id, post_id, author, body, score, created_utc, is_submitter
                FROM reddit_comments
                ORDER BY created_utc DESC
            """)
            
            comments_df = pd.read_sql(comments_query, conn)
            print(f"Found {len(comments_df)} Reddit comments")
            
            if not comments_df.empty:
                # Process comments for player mentions
                print("Processing comments for player mentions...")
                processed_comments, comment_mentions = ner_processor.process_reddit_comments(comments_df)
                
                # Insert mentions into player tables
                if not comment_mentions.empty:
                    print(f"Found {len(comment_mentions)} player mentions in comments")
                    _insert_mentions_to_player_tables(conn, comment_mentions, 'comment')
                else:
                    print("No player mentions found in comments")
            
            conn.commit()
            print("\n✅ PLAYER TABLES POPULATED SUCCESSFULLY!")
            
    except Exception as e:
        print(f"Error populating player tables: {e}")
        import traceback
        traceback.print_exc()

def _insert_mentions_to_player_tables(conn, mentions_df, mention_type):
    """Insert mentions into individual player tables"""
    
    for player_name in LAKERS_ROSTER.keys():
        player_mentions = mentions_df[mentions_df['player_name'] == player_name].copy()
        
        if player_mentions.empty:
            continue
        
        table_name = f"player_mentions_{player_name.lower().replace(' ', '_').replace('č', 'c')}"
        
        print(f"   Inserting {len(player_mentions)} mentions for {player_name}...")
        
        for _, mention in player_mentions.iterrows():
            insert_sql = f"""
            INSERT INTO {table_name} (
                post_id, comment_id, player_name, alias_found, context, confidence,
                mention_type, post_title, post_author, post_score, comment_author,
                comment_score, created_utc, is_submitter, subreddit
            ) VALUES (
                :post_id, :comment_id, :player_name, :alias_found, :context, :confidence,
                :mention_type, :post_title, :post_author, :post_score, :comment_author,
                :comment_score, :created_utc, :is_submitter, :subreddit
            )
            """
            
            conn.execute(text(insert_sql), {
                'post_id': mention.get('post_id'),
                'comment_id': mention.get('comment_id'),
                'player_name': mention['player_name'],
                'alias_found': mention['alias_found'],
                'context': mention['context'],
                'confidence': mention['confidence'],
                'mention_type': mention['mention_type'],
                'post_title': mention.get('post_title'),
                'post_author': mention.get('post_author'),
                'post_score': mention.get('post_score'),
                'comment_author': mention.get('comment_author'),
                'comment_score': mention.get('comment_score'),
                'created_utc': mention.get('created_utc'),
                'is_submitter': mention.get('is_submitter', False),
                'subreddit': mention.get('subreddit')
            })

def get_player_mention_summary():
    """Get summary of player mentions across all tables"""
    
    db_manager = DatabaseManager()
    
    try:
        print("\nPLAYER MENTION SUMMARY")
        print("=" * 50)
        
        with db_manager.engine.connect() as conn:
            for player_name in LAKERS_ROSTER.keys():
                table_name = f"player_mentions_{player_name.lower().replace(' ', '_').replace('č', 'c')}"
                
                # Check if table exists and get count
                count_query = text(f"SELECT COUNT(*) as mention_count FROM {table_name}")
                
                try:
                    result = conn.execute(count_query)
                    count = result.fetchone()[0]
                    print(f"{player_name}: {count} mentions")
                except Exception as e:
                    print(f"{player_name}: Table not found or error - {e}")
        
    except Exception as e:
        print(f"Error getting summary: {e}")

def main():
    """Main function to create and populate player tables"""
    print("LAKERS PLAYER TABLE CREATION AND POPULATION")
    print("=" * 60)
    
    # Create tables
    create_player_tables()
    
    # Populate tables
    populate_player_tables()
    
    # Show summary
    get_player_mention_summary()
    
    print("\n🎉 PROCESS COMPLETED SUCCESSFULLY!")
    print("Individual tables created for each Lakers player with NER data")

if __name__ == "__main__":
    main()
