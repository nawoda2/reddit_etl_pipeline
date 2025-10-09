#!/usr/bin/env python3
"""
Create a proper fact and dimension schema for the Reddit sentiment analysis
"""

import sys
import os
from sqlalchemy import text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_processors.database_manager import DatabaseManager

def create_fact_dimension_schema():
    """Create fact and dimension tables for proper data warehouse structure"""
    
    db_manager = DatabaseManager()
    
    try:
        with db_manager.engine.connect() as conn:
            print("CREATING FACT AND DIMENSION SCHEMA")
            print("=" * 50)
            
            # 1. Create dimension tables
            print("1. Creating dimension tables...")
            
            # Dim_Date table
            print("   Creating dim_date...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_date (
                    date_key INTEGER PRIMARY KEY,
                    full_date DATE NOT NULL,
                    year INTEGER NOT NULL,
                    quarter INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    day INTEGER NOT NULL,
                    day_of_week INTEGER NOT NULL,
                    day_name VARCHAR(10) NOT NULL,
                    month_name VARCHAR(10) NOT NULL,
                    is_weekend BOOLEAN NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Dim_Author table
            print("   Creating dim_author...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_author (
                    author_key SERIAL PRIMARY KEY,
                    author_name VARCHAR(255) NOT NULL UNIQUE,
                    is_submitter BOOLEAN DEFAULT FALSE,
                    first_seen_date DATE,
                    last_seen_date DATE,
                    total_posts INTEGER DEFAULT 0,
                    total_comments INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Dim_Player table
            print("   Creating dim_player...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_player (
                    player_key SERIAL PRIMARY KEY,
                    player_name VARCHAR(255) NOT NULL UNIQUE,
                    position VARCHAR(50),
                    jersey_number INTEGER,
                    team_status VARCHAR(50),
                    first_mentioned_date DATE,
                    last_mentioned_date DATE,
                    total_mentions INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Dim_Post table
            print("   Creating dim_post...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_post (
                    post_key SERIAL PRIMARY KEY,
                    reddit_post_id VARCHAR(50) NOT NULL UNIQUE,
                    title TEXT,
                    selftext TEXT,
                    subreddit VARCHAR(100),
                    url TEXT,
                    is_self BOOLEAN DEFAULT FALSE,
                    is_over_18 BOOLEAN DEFAULT FALSE,
                    is_edited BOOLEAN DEFAULT FALSE,
                    is_spoiler BOOLEAN DEFAULT FALSE,
                    is_stickied BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # 2. Create fact tables
            print("2. Creating fact tables...")
            
            # Fact_Post_Sentiment table
            print("   Creating fact_post_sentiment...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fact_post_sentiment (
                    sentiment_key SERIAL PRIMARY KEY,
                    post_key INTEGER REFERENCES dim_post(post_key),
                    author_key INTEGER REFERENCES dim_author(author_key),
                    date_key INTEGER REFERENCES dim_date(date_key),
                    vader_positive DECIMAL(5,4) NOT NULL,
                    vader_negative DECIMAL(5,4) NOT NULL,
                    vader_neutral DECIMAL(5,4) NOT NULL,
                    vader_compound DECIMAL(5,4) NOT NULL,
                    transformer_positive DECIMAL(5,4) NOT NULL,
                    transformer_negative DECIMAL(5,4) NOT NULL,
                    transformer_neutral DECIMAL(5,4) NOT NULL,
                    transformer_sentiment VARCHAR(20) NOT NULL,
                    vader_classification VARCHAR(20) NOT NULL,
                    agreement VARCHAR(20) NOT NULL,
                    analysis_timestamp TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT unique_post_sentiment_fact UNIQUE (post_key)
                )
            """))
            
            # Fact_Comment_Sentiment table
            print("   Creating fact_comment_sentiment...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fact_comment_sentiment (
                    comment_sentiment_key SERIAL PRIMARY KEY,
                    comment_id VARCHAR(50) NOT NULL UNIQUE,
                    post_key INTEGER REFERENCES dim_post(post_key),
                    author_key INTEGER REFERENCES dim_author(author_key),
                    date_key INTEGER REFERENCES dim_date(date_key),
                    parent_comment_id VARCHAR(50),
                    is_submitter BOOLEAN DEFAULT FALSE,
                    is_edited BOOLEAN DEFAULT FALSE,
                    comment_score INTEGER NOT NULL,
                    vader_positive DECIMAL(5,4) NOT NULL,
                    vader_negative DECIMAL(5,4) NOT NULL,
                    vader_neutral DECIMAL(5,4) NOT NULL,
                    vader_compound DECIMAL(5,4) NOT NULL,
                    transformer_positive DECIMAL(5,4) NOT NULL,
                    transformer_negative DECIMAL(5,4) NOT NULL,
                    transformer_neutral DECIMAL(5,4) NOT NULL,
                    transformer_sentiment VARCHAR(20) NOT NULL,
                    vader_classification VARCHAR(20) NOT NULL,
                    agreement VARCHAR(20) NOT NULL,
                    analysis_timestamp TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Fact_Player_Mentions table
            print("   Creating fact_player_mentions...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fact_player_mentions (
                    mention_key SERIAL PRIMARY KEY,
                    post_key INTEGER REFERENCES dim_post(post_key),
                    player_key INTEGER REFERENCES dim_player(player_key),
                    date_key INTEGER REFERENCES dim_date(date_key),
                    mention_context TEXT,
                    sentiment_score DECIMAL(5,4),
                    mention_type VARCHAR(20) DEFAULT 'post',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # 3. Create indexes for performance
            print("3. Creating performance indexes...")
            
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_fact_post_sentiment_date ON fact_post_sentiment(date_key)",
                "CREATE INDEX IF NOT EXISTS idx_fact_post_sentiment_author ON fact_post_sentiment(author_key)",
                "CREATE INDEX IF NOT EXISTS idx_fact_comment_sentiment_date ON fact_comment_sentiment(date_key)",
                "CREATE INDEX IF NOT EXISTS idx_fact_comment_sentiment_author ON fact_comment_sentiment(author_key)",
                "CREATE INDEX IF NOT EXISTS idx_fact_comment_sentiment_post ON fact_comment_sentiment(post_key)",
                "CREATE INDEX IF NOT EXISTS idx_fact_player_mentions_date ON fact_player_mentions(date_key)",
                "CREATE INDEX IF NOT EXISTS idx_fact_player_mentions_player ON fact_player_mentions(player_key)",
                "CREATE INDEX IF NOT EXISTS idx_fact_player_mentions_post ON fact_player_mentions(post_key)",
                "CREATE INDEX IF NOT EXISTS idx_dim_date_full_date ON dim_date(full_date)",
                "CREATE INDEX IF NOT EXISTS idx_dim_author_name ON dim_author(author_name)",
                "CREATE INDEX IF NOT EXISTS idx_dim_player_name ON dim_player(player_name)",
                "CREATE INDEX IF NOT EXISTS idx_dim_post_reddit_id ON dim_post(reddit_post_id)"
            ]
            
            for idx_sql in indexes:
                conn.execute(text(idx_sql))
            
            conn.commit()
            print("   SUCCESS: Performance indexes created")
            
            # 4. Create views for easy querying
            print("4. Creating analytical views...")
            
            # Post sentiment summary view
            conn.execute(text("""
                CREATE OR REPLACE VIEW vw_post_sentiment_summary AS
                SELECT 
                    d.full_date,
                    da.author_name,
                    dp.title,
                    dp.reddit_post_id,
                    fps.vader_compound,
                    fps.transformer_sentiment,
                    fps.agreement,
                    fps.analysis_timestamp
                FROM fact_post_sentiment fps
                JOIN dim_date d ON fps.date_key = d.date_key
                JOIN dim_author da ON fps.author_key = da.author_key
                JOIN dim_post dp ON fps.post_key = dp.post_key
                ORDER BY d.full_date DESC, fps.vader_compound DESC
            """))
            
            # Comment sentiment summary view
            conn.execute(text("""
                CREATE OR REPLACE VIEW vw_comment_sentiment_summary AS
                SELECT 
                    d.full_date,
                    da.author_name,
                    dp.title as post_title,
                    dp.reddit_post_id,
                    fcs.comment_id,
                    fcs.comment_score,
                    fcs.vader_compound,
                    fcs.transformer_sentiment,
                    fcs.agreement,
                    fcs.analysis_timestamp
                FROM fact_comment_sentiment fcs
                JOIN dim_date d ON fcs.date_key = d.date_key
                JOIN dim_author da ON fcs.author_key = da.author_key
                JOIN dim_post dp ON fcs.post_key = dp.post_key
                ORDER BY d.full_date DESC, fcs.comment_score DESC
            """))
            
            # Player mentions summary view
            conn.execute(text("""
                CREATE OR REPLACE VIEW vw_player_mentions_summary AS
                SELECT 
                    d.full_date,
                    dp.player_name,
                    dp.position,
                    dp.jersey_number,
                    COUNT(fpm.mention_key) as total_mentions,
                    AVG(fpm.sentiment_score) as avg_sentiment,
                    COUNT(CASE WHEN fpm.sentiment_score > 0.05 THEN 1 END) as positive_mentions,
                    COUNT(CASE WHEN fpm.sentiment_score < -0.05 THEN 1 END) as negative_mentions
                FROM fact_player_mentions fpm
                JOIN dim_date d ON fpm.date_key = d.date_key
                JOIN dim_player dp ON fpm.player_key = dp.player_key
                GROUP BY d.full_date, dp.player_name, dp.position, dp.jersey_number
                ORDER BY d.full_date DESC, total_mentions DESC
            """))
            
            conn.commit()
            print("   SUCCESS: Analytical views created")
            
            print("\n✅ FACT AND DIMENSION SCHEMA CREATED!")
            print("   - Dimension tables: dim_date, dim_author, dim_player, dim_post")
            print("   - Fact tables: fact_post_sentiment, fact_comment_sentiment, fact_player_mentions")
            print("   - Analytical views: vw_post_sentiment_summary, vw_comment_sentiment_summary, vw_player_mentions_summary")
            print("   - Performance indexes created")
            print("   - Ready for data migration and analysis")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    create_fact_dimension_schema()

