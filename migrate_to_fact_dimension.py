#!/usr/bin/env python3
"""
Migrate existing data to the new fact and dimension schema
"""

import sys
import os
import pandas as pd
from datetime import datetime
from sqlalchemy import text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_processors.database_manager import DatabaseManager

def migrate_to_fact_dimension():
    """Migrate existing data to fact and dimension tables"""
    
    db_manager = DatabaseManager()
    
    try:
        with db_manager.engine.connect() as conn:
            print("MIGRATING DATA TO FACT AND DIMENSION SCHEMA")
            print("=" * 60)
            
            # 1. Populate dim_date
            print("1. Populating dim_date...")
            
            # Get all unique dates from posts and comments
            conn.execute(text("""
                INSERT INTO dim_date (date_key, full_date, year, quarter, month, day, day_of_week, day_name, month_name, is_weekend)
                SELECT DISTINCT
                    EXTRACT(EPOCH FROM DATE(created_utc))::INTEGER as date_key,
                    DATE(created_utc) as full_date,
                    EXTRACT(YEAR FROM created_utc)::INTEGER as year,
                    EXTRACT(QUARTER FROM created_utc)::INTEGER as quarter,
                    EXTRACT(MONTH FROM created_utc)::INTEGER as month,
                    EXTRACT(DAY FROM created_utc)::INTEGER as day,
                    EXTRACT(DOW FROM created_utc)::INTEGER as day_of_week,
                    TO_CHAR(created_utc, 'Day') as day_name,
                    TO_CHAR(created_utc, 'Month') as month_name,
                    EXTRACT(DOW FROM created_utc) IN (0, 6) as is_weekend
                FROM (
                    SELECT created_utc FROM reddit_posts
                    UNION
                    SELECT created_utc FROM reddit_comments
                ) all_dates
                WHERE DATE(created_utc) IS NOT NULL
                ON CONFLICT (date_key) DO NOTHING
            """))
            
            # 2. Populate dim_author
            print("2. Populating dim_author...")
            
            conn.execute(text("""
                INSERT INTO dim_author (author_name, is_submitter, first_seen_date, last_seen_date, total_posts, total_comments)
                SELECT 
                    author,
                    FALSE as is_submitter,
                    MIN(DATE(created_utc)) as first_seen_date,
                    MAX(DATE(created_utc)) as last_seen_date,
                    COUNT(*) as total_posts,
                    0 as total_comments
                FROM reddit_posts
                WHERE author IS NOT NULL AND author != '[deleted]'
                GROUP BY author
                ON CONFLICT (author_name) DO NOTHING
            """))
            
            # Add comment authors (simplified to avoid conflicts)
            conn.execute(text("""
                INSERT INTO dim_author (author_name, is_submitter, first_seen_date, last_seen_date, total_posts, total_comments)
                SELECT 
                    author,
                    FALSE as is_submitter,
                    MIN(DATE(created_utc)) as first_seen_date,
                    MAX(DATE(created_utc)) as last_seen_date,
                    0 as total_posts,
                    COUNT(*) as total_comments
                FROM reddit_comments
                WHERE author IS NOT NULL AND author != '[deleted]'
                GROUP BY author
                ON CONFLICT (author_name) DO NOTHING
            """))
            
            # 3. Populate dim_post
            print("3. Populating dim_post...")
            
            conn.execute(text("""
                INSERT INTO dim_post (reddit_post_id, title, selftext, subreddit, url, is_self, is_over_18, is_edited, is_spoiler, is_stickied)
                SELECT 
                    id,
                    title,
                    selftext,
                    subreddit,
                    url,
                    CASE WHEN selftext IS NOT NULL AND selftext != '' THEN TRUE ELSE FALSE END as is_self,
                    over_18,
                    edited,
                    spoiler,
                    stickied
                FROM reddit_posts
                ON CONFLICT (reddit_post_id) DO NOTHING
            """))
            
            # 4. Populate dim_player (Lakers roster)
            print("4. Populating dim_player...")
            
            lakers_players = [
                ('LeBron James', 'SF', 6, 'Starter'),
                ('Anthony Davis', 'PF', 3, 'Starter'),
                ('Luka Doncic', 'PG', 77, 'Starter'),
                ('Dejounte Murray', 'SG', 5, 'Starter'),
                ('Deandre Ayton', 'C', 2, 'Starter'),
                ('Jarred Vanderbilt', 'PF', 12, 'Bench'),
                ('Rui Hachimura', 'SF', 28, 'Bench'),
                ('Austin Reaves', 'SG', 15, 'Bench'),
                ('Marcus Smart', 'PG', 36, 'Bench'),
                ('Christian Wood', 'PF', 35, 'Bench'),
                ('Jaxson Hayes', 'C', 11, 'Bench'),
                ('Gabe Vincent', 'PG', 7, 'Bench'),
                ('Cam Reddish', 'SF', 5, 'Bench'),
                ('Max Christie', 'SG', 10, 'Bench'),
                ('Jalen Hood-Schifino', 'PG', 1, 'Bench'),
                ('Maxwell Lewis', 'SF', 21, 'Two-Way'),
                ('Alex Fudge', 'SF', 8, 'Two-Way')
            ]
            
            for player_name, position, jersey, status in lakers_players:
                conn.execute(text("""
                    INSERT INTO dim_player (player_name, position, jersey_number, team_status)
                    VALUES (:name, :pos, :jersey, :status)
                    ON CONFLICT (player_name) DO NOTHING
                """), {
                    'name': player_name,
                    'pos': position,
                    'jersey': jersey,
                    'status': status
                })
            
            # 5. Populate fact_post_sentiment
            print("5. Populating fact_post_sentiment...")
            
            conn.execute(text("""
                INSERT INTO fact_post_sentiment (
                    post_key, author_key, date_key, vader_positive, vader_negative, vader_neutral, 
                    vader_compound, transformer_positive, transformer_negative, transformer_neutral, 
                    transformer_sentiment, vader_classification, agreement, analysis_timestamp
                )
                SELECT 
                    dp.post_key,
                    da.author_key,
                    dd.date_key,
                    ss.vader_positive,
                    ss.vader_negative,
                    ss.vader_neutral,
                    ss.vader_compound,
                    ss.transformer_positive,
                    ss.transformer_negative,
                    ss.transformer_neutral,
                    ss.transformer_sentiment,
                    CASE 
                        WHEN ss.vader_compound > 0.05 THEN 'positive'
                        WHEN ss.vader_compound < -0.05 THEN 'negative'
                        ELSE 'neutral'
                    END as vader_classification,
                    CASE 
                        WHEN (CASE 
                            WHEN ss.vader_compound > 0.05 THEN 'positive'
                            WHEN ss.vader_compound < -0.05 THEN 'negative'
                            ELSE 'neutral'
                        END) = ss.transformer_sentiment THEN 'agree'
                        ELSE 'disagree'
                    END as agreement,
                    ss.analysis_timestamp
                FROM sentiment_scores ss
                JOIN dim_post dp ON ss.post_id = dp.reddit_post_id
                JOIN dim_author da ON ss.post_id IN (
                    SELECT id FROM reddit_posts WHERE author = da.author_name
                )
                JOIN dim_date dd ON DATE(ss.analysis_timestamp) = dd.full_date
                ON CONFLICT (post_key) DO NOTHING
            """))
            
            # 6. Populate fact_comment_sentiment
            print("6. Populating fact_comment_sentiment...")
            
            # First, we need to analyze comment sentiment
            print("   Analyzing comment sentiment for migration...")
            
            # Get comments that need sentiment analysis
            comments_query = text("""
                SELECT 
                    rc.id, rc.post_id, rc.author, rc.body, rc.score, rc.created_utc, 
                    rc.parent_id, rc.is_submitter, rc.edited
                FROM reddit_comments rc
                WHERE LENGTH(rc.body) > 10
                ORDER BY rc.score DESC
            """)
            
            result = conn.execute(comments_query)
            comments_data = []
            
            for row in result:
                comments_data.append({
                    'comment_id': row[0],
                    'post_id': row[1],
                    'author': row[2],
                    'body': row[3],
                    'score': row[4],
                    'created_utc': row[5],
                    'parent_id': row[6],
                    'is_submitter': row[7],
                    'edited': row[8]
                })
            
            print(f"   Found {len(comments_data)} comments to analyze...")
            
            # Analyze sentiment for comments (simplified version)
            from data_processors.sentiment_analyzer import LakersSentimentAnalyzer
            analyzer = LakersSentimentAnalyzer()
            
            analyzed_comments = []
            for i, comment in enumerate(comments_data[:50]):  # Limit to first 50 for migration
                if i % 10 == 0:
                    print(f"     Processing comment {i+1}/50...")
                
                try:
                    sentiment_result = analyzer.analyze_sentiment_comprehensive(comment['body'])
                    
                    analyzed_comments.append({
                        'comment_id': comment['comment_id'],
                        'post_id': comment['post_id'],
                        'author': comment['author'],
                        'score': comment['score'],
                        'created_utc': comment['created_utc'],
                        'parent_id': comment['parent_id'],
                        'is_submitter': comment['is_submitter'],
                        'edited': comment['edited'],
                        'vader_positive': sentiment_result.get('vader_positive', 0.0),
                        'vader_negative': sentiment_result.get('vader_negative', 0.0),
                        'vader_neutral': sentiment_result.get('vader_neutral', 1.0),
                        'vader_compound': sentiment_result.get('vader_compound', 0.0),
                        'transformer_positive': sentiment_result.get('transformer_positive', 0.0),
                        'transformer_negative': sentiment_result.get('transformer_negative', 0.0),
                        'transformer_neutral': sentiment_result.get('transformer_neutral', 1.0),
                        'transformer_sentiment': sentiment_result.get('transformer_sentiment', 'neutral'),
                        'analysis_timestamp': sentiment_result.get('analysis_timestamp', datetime.now().isoformat())
                    })
                except Exception as e:
                    print(f"     Error analyzing comment {comment['comment_id']}: {e}")
                    continue
            
            # Insert analyzed comments into fact table
            for comment in analyzed_comments:
                conn.execute(text("""
                    INSERT INTO fact_comment_sentiment (
                        comment_id, post_key, author_key, date_key, parent_comment_id, 
                        is_submitter, is_edited, comment_score, vader_positive, vader_negative, 
                        vader_neutral, vader_compound, transformer_positive, transformer_negative, 
                        transformer_neutral, transformer_sentiment, vader_classification, agreement, analysis_timestamp
                    )
                    SELECT 
                        :comment_id,
                        dp.post_key,
                        da.author_key,
                        dd.date_key,
                        :parent_id,
                        :is_submitter,
                        :is_edited,
                        :score,
                        :vader_positive,
                        :vader_negative,
                        :vader_neutral,
                        :vader_compound,
                        :transformer_positive,
                        :transformer_negative,
                        :transformer_neutral,
                        :transformer_sentiment,
                        CASE 
                            WHEN :vader_compound > 0.05 THEN 'positive'
                            WHEN :vader_compound < -0.05 THEN 'negative'
                            ELSE 'neutral'
                        END as vader_classification,
                        CASE 
                            WHEN (CASE 
                                WHEN :vader_compound > 0.05 THEN 'positive'
                                WHEN :vader_compound < -0.05 THEN 'negative'
                                ELSE 'neutral'
                            END) = :transformer_sentiment THEN 'agree'
                            ELSE 'disagree'
                        END as agreement,
                        :analysis_timestamp
                    FROM dim_post dp
                    JOIN dim_author da ON da.author_name = :author
                    JOIN dim_date dd ON dd.full_date = DATE(:created_utc)
                    WHERE dp.reddit_post_id = :post_id
                    ON CONFLICT (comment_id) DO NOTHING
                """), comment)
            
            conn.commit()
            
            # 7. Verify migration
            print("7. Verifying migration...")
            
            # Count records in each table
            tables = ['dim_date', 'dim_author', 'dim_player', 'dim_post', 'fact_post_sentiment', 'fact_comment_sentiment']
            
            for table in tables:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.fetchone()[0]
                print(f"   {table}: {count} records")
            
            print("\nSUCCESS: Data migration completed!")
            print("   - Dimension tables populated")
            print("   - Fact tables populated")
            print("   - Ready for analytical queries")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    migrate_to_fact_dimension()
