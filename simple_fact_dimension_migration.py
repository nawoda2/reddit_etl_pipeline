#!/usr/bin/env python3
"""
Simple migration to fact and dimension schema - focus on core functionality
"""

import sys
import os
from sqlalchemy import text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_processors.database_manager import DatabaseManager

def simple_migration():
    """Simple migration focusing on core fact and dimension tables"""
    
    db_manager = DatabaseManager()
    
    try:
        with db_manager.engine.connect() as conn:
            print("SIMPLE FACT AND DIMENSION MIGRATION")
            print("=" * 50)
            
            # 1. Create a simple fact table for post sentiment
            print("1. Creating simplified fact_post_sentiment...")
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fact_post_sentiment_simple (
                    id SERIAL PRIMARY KEY,
                    reddit_post_id VARCHAR(50) NOT NULL UNIQUE,
                    title TEXT,
                    author VARCHAR(255),
                    subreddit VARCHAR(100),
                    score INTEGER,
                    num_comments INTEGER,
                    upvote_ratio DECIMAL(3,2),
                    created_utc TIMESTAMP,
                    vader_compound DECIMAL(5,4),
                    vader_positive DECIMAL(5,4),
                    vader_negative DECIMAL(5,4),
                    vader_neutral DECIMAL(5,4),
                    transformer_positive DECIMAL(5,4),
                    transformer_negative DECIMAL(5,4),
                    transformer_neutral DECIMAL(5,4),
                    transformer_sentiment VARCHAR(20),
                    vader_classification VARCHAR(20),
                    agreement VARCHAR(20),
                    analysis_timestamp TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # 2. Populate the simple fact table
            print("2. Populating fact_post_sentiment_simple...")
            
            conn.execute(text("""
                INSERT INTO fact_post_sentiment_simple (
                    reddit_post_id, title, author, subreddit, score, num_comments, upvote_ratio,
                    created_utc, vader_compound, vader_positive, vader_negative, vader_neutral,
                    transformer_positive, transformer_negative, transformer_neutral, transformer_sentiment,
                    vader_classification, agreement, analysis_timestamp
                )
                SELECT 
                    rp.id,
                    rp.title,
                    rp.author,
                    rp.subreddit,
                    rp.score,
                    rp.num_comments,
                    rp.upvote_ratio,
                    rp.created_utc,
                    ss.vader_compound,
                    ss.vader_positive,
                    ss.vader_negative,
                    ss.vader_neutral,
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
                FROM reddit_posts rp
                JOIN sentiment_scores ss ON rp.id = ss.post_id
                ON CONFLICT (reddit_post_id) DO NOTHING
            """))
            
            # 3. Create a simple fact table for comment sentiment
            print("3. Creating simplified fact_comment_sentiment...")
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fact_comment_sentiment_simple (
                    id SERIAL PRIMARY KEY,
                    comment_id VARCHAR(50) NOT NULL UNIQUE,
                    post_id VARCHAR(50),
                    author VARCHAR(255),
                    body TEXT,
                    score INTEGER,
                    created_utc TIMESTAMP,
                    parent_id VARCHAR(50),
                    is_submitter BOOLEAN,
                    is_edited BOOLEAN,
                    vader_compound DECIMAL(5,4),
                    vader_positive DECIMAL(5,4),
                    vader_negative DECIMAL(5,4),
                    vader_neutral DECIMAL(5,4),
                    transformer_positive DECIMAL(5,4),
                    transformer_negative DECIMAL(5,4),
                    transformer_neutral DECIMAL(5,4),
                    transformer_sentiment VARCHAR(20),
                    vader_classification VARCHAR(20),
                    agreement VARCHAR(20),
                    analysis_timestamp TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # 4. Populate comment sentiment (using existing data)
            print("4. Populating fact_comment_sentiment_simple...")
            
            # Get comments from the CSV we created earlier
            import pandas as pd
            csv_path = "outputs/sentiment_data/comment_sentiment_analysis_20251008_183038.csv"
            
            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path)
                print(f"   Found {len(df)} comments in CSV")
                
                for _, row in df.iterrows():
                    conn.execute(text("""
                        INSERT INTO fact_comment_sentiment_simple (
                            comment_id, post_id, author, body, score, created_utc, parent_id,
                            is_submitter, is_edited, vader_compound, vader_positive, vader_negative,
                            vader_neutral, transformer_positive, transformer_negative, transformer_neutral,
                            transformer_sentiment, vader_classification, agreement, analysis_timestamp
                        )
                        VALUES (
                            :comment_id, :post_id, :author, :body, :score, :created_utc, :parent_id,
                            :is_submitter, :is_edited, :vader_compound, :vader_positive, :vader_negative,
                            :vader_neutral, :transformer_positive, :transformer_negative, :transformer_neutral,
                            :transformer_sentiment, :vader_classification, :agreement, :analysis_timestamp
                        )
                        ON CONFLICT (comment_id) DO NOTHING
                    """), {
                        'comment_id': row['comment_id'],
                        'post_id': row['post_id'],
                        'author': row['author'],
                        'body': row['body'],
                        'score': int(row['score']),
                        'created_utc': row['created_utc'],
                        'parent_id': row['parent_id'],
                        'is_submitter': bool(row['is_submitter']),
                        'is_edited': bool(row['edited']),
                        'vader_compound': float(row['vader_compound']),
                        'vader_positive': float(row['vader_positive']),
                        'vader_negative': float(row['vader_negative']),
                        'vader_neutral': float(row['vader_neutral']),
                        'transformer_positive': float(row['transformer_positive']),
                        'transformer_negative': float(row['transformer_negative']),
                        'transformer_neutral': float(row['transformer_neutral']),
                        'transformer_sentiment': row['transformer_sentiment'],
                        'vader_classification': row['vader_classification'],
                        'agreement': row['agreement'],
                        'analysis_timestamp': row['analysis_timestamp']
                    })
            else:
                print("   No comment CSV found, skipping comment migration")
            
            # 5. Create analytical views
            print("5. Creating analytical views...")
            
            # Post sentiment summary view
            conn.execute(text("""
                CREATE OR REPLACE VIEW vw_post_sentiment_analysis AS
                SELECT 
                    reddit_post_id,
                    title,
                    author,
                    score,
                    num_comments,
                    vader_compound,
                    transformer_sentiment,
                    vader_classification,
                    agreement,
                    created_utc,
                    analysis_timestamp
                FROM fact_post_sentiment_simple
                ORDER BY score DESC, vader_compound DESC
            """))
            
            # Comment sentiment summary view
            conn.execute(text("""
                CREATE OR REPLACE VIEW vw_comment_sentiment_analysis AS
                SELECT 
                    comment_id,
                    post_id,
                    author,
                    body,
                    score,
                    vader_compound,
                    transformer_sentiment,
                    vader_classification,
                    agreement,
                    created_utc,
                    analysis_timestamp
                FROM fact_comment_sentiment_simple
                ORDER BY score DESC, vader_compound DESC
            """))
            
            # Overall sentiment summary view
            conn.execute(text("""
                CREATE OR REPLACE VIEW vw_overall_sentiment_summary AS
                SELECT 
                    'Posts' as content_type,
                    COUNT(*) as total_count,
                    AVG(vader_compound) as avg_vader_compound,
                    COUNT(CASE WHEN vader_classification = 'positive' THEN 1 END) as positive_count,
                    COUNT(CASE WHEN vader_classification = 'negative' THEN 1 END) as negative_count,
                    COUNT(CASE WHEN vader_classification = 'neutral' THEN 1 END) as neutral_count,
                    COUNT(CASE WHEN agreement = 'agree' THEN 1 END) as agreement_count
                FROM fact_post_sentiment_simple
                UNION ALL
                SELECT 
                    'Comments' as content_type,
                    COUNT(*) as total_count,
                    AVG(vader_compound) as avg_vader_compound,
                    COUNT(CASE WHEN vader_classification = 'positive' THEN 1 END) as positive_count,
                    COUNT(CASE WHEN vader_classification = 'negative' THEN 1 END) as negative_count,
                    COUNT(CASE WHEN vader_classification = 'neutral' THEN 1 END) as neutral_count,
                    COUNT(CASE WHEN agreement = 'agree' THEN 1 END) as agreement_count
                FROM fact_comment_sentiment_simple
            """))
            
            conn.commit()
            
            # 6. Verify migration
            print("6. Verifying migration...")
            
            # Count records
            result = conn.execute(text("SELECT COUNT(*) FROM fact_post_sentiment_simple"))
            post_count = result.fetchone()[0]
            print(f"   Posts migrated: {post_count}")
            
            result = conn.execute(text("SELECT COUNT(*) FROM fact_comment_sentiment_simple"))
            comment_count = result.fetchone()[0]
            print(f"   Comments migrated: {comment_count}")
            
            # Test views
            result = conn.execute(text("SELECT * FROM vw_overall_sentiment_summary"))
            summary = result.fetchall()
            print(f"   Summary view created with {len(summary)} rows")
            
            print("\nSUCCESS: Simple fact and dimension migration completed!")
            print("   - fact_post_sentiment_simple: Post sentiment data")
            print("   - fact_comment_sentiment_simple: Comment sentiment data")
            print("   - Analytical views created for easy querying")
            print("   - Ready for business intelligence and reporting")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    simple_migration()

